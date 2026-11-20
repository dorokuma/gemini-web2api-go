package app

// cdp_ws.go —— 回环专用极简 RFC 6455 WebSocket 客户端，只服务本机 Chromium DevTools。
//
// 为什么自己写而不引 gorilla/websocket：本仓是 6 平台静态二进制，go.mod 已经
// 有一条不许动的硬约束；而 DevTools 的 WS 面只需要 Text 帧 + Ping/Pong + Close，
// 完整客户端库（压缩扩展、并发写、buffer pool）的 90% 都用不上。
//
// 只允许的拨号目标是 127.0.0.1 / ::1 / localhost：DevTools WS 没有任何鉴权，
// 连上就等于拿到整个浏览器。这个客户端宁可拒连，也不接受能被外部 URL
// 带偏的拨号目标（比如把 ws:// 指到内网别的机器上）。

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"
)

// maxCDPFrameBytes 是单条消息（含分片重组后）的防御上限。
//
// DevTools 回包基本是 JSON，最大的场景是 Network.getCookies / Page.captureScreenshot，
// 几 MB 量级。取 32 MiB：再大几乎必然是异常流（恶意目标、协议错位），收下它只会
// 白占常驻进程的内存。
const maxCDPFrameBytes = 32 << 20

// cdpConn 是一条已握手的回环 WebSocket 连接。
//
// 读侧单 goroutine（readFrame 不加锁，只有读循环在调）；写侧任何 goroutine 可来，
// 统一过 writeMu 抢锁写整帧，帧之间不会交错。写是有界动作：sendText/sendClose
// 都设写 deadline，对端不收（渲染器卡死、TCP 窗口关死）时一次 Write 不能把
// 调用方或关闭路径挂住。Close 先给短 deadline 发 Close 帧再立刻 conn.Close，
// 强制唤醒所有阻塞读写与 pending 调用方。
type cdpConn struct {
	conn    net.Conn
	br      *bufio.Reader
	writeMu sync.Mutex

	closeOnce sync.Once
	closed    chan struct{}
}

// cdpWriteTimeout 是单帧写超时：回环 DevTools 写帧通常 <1ms，5s 是对
// 「对端彻底不收」的兜底——卡死宁可让这次调用报错，也不阻塞协程。
const cdpWriteTimeout = 5 * time.Second

// cdpCloseTimeout 是发 Close 帧的预算：礼貌帧发不出去就直接断，
// 绝不让 Close() 自身被拖住。
const cdpCloseTimeout = 300 * time.Millisecond

// dialCDPWebsocket 向 wsURL（ws://127.0.0.1:port/devtools/... 形态）完成 HTTP Upgrade 握手。
//
// 刻意不发 Origin 头：Chromium 的 DevTools 服务端只对「带了 Origin 且不在
// --remote-allow-origins 白名单里」的握手回 403。不带 Origin 的客户端被当成
// 非浏览器源（curl / 自研工具）直接放行，于是我们不需要给 Chromium 传
// --remote-allow-origins=* —— 那个开关会同时放行任意网页对本机 DevTools 的连接，
// 属于用危险默认值换一个本来免费的东西。
func dialCDPWebsocket(ctx context.Context, wsURL string) (*cdpConn, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return nil, fmt.Errorf("cdp ws url parse: %w", err)
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, fmt.Errorf("cdp ws url must be ws:// (got %q)", u.Scheme)
	}
	if u.Scheme == "wss" {
		return nil, errors.New("cdp ws url must be ws:// (loopback DevTools never speaks wss)")
	}
	host := u.Hostname()
	if !isLoopbackHost(host) {
		return nil, fmt.Errorf("cdp ws target %q is not loopback; refusing to dial", host)
	}
	port := u.Port()
	if port == "" {
		port = "80"
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
	if err != nil {
		return nil, err
	}

	// Sec-WebSocket-Key：16 字节随机数 base64。服务端回执必须等于
	// base64(sha1(key + GUID))，GUID 是 RFC 6455 钉死的常量，算出来的 accept 是
	// 定值——但 Chromium 不在乎，标准客户端也不该复用固定 key（缓存代理语义）。
	keyBytes := make([]byte, 16)
	if _, err := rand.Read(keyBytes); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(keyBytes)

	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	// 握手头刻意精简：不带 Origin（见上）、不带任何 User-Agent/Cookie，
	// DevTools 只需要 Upgrade/Connection/Key/Version 四项。
	handshake := "GET " + path + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"\r\n"
	if err := conn.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		conn.Close()
		return nil, err
	}
	if _, err := io.WriteString(conn, handshake); err != nil {
		conn.Close()
		return nil, err
	}
	_ = conn.SetWriteDeadline(time.Time{})

	// 读响应头。DevTools 回 "HTTP/1.1 101 ..."，之后跟若干行头、空行结束。
	// 用 bufio 读并把 reader 留给后续帧解析——握手后可能紧跟的第一帧就在缓冲里。
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	br := bufio.NewReader(conn)
	statusLine, err := br.ReadString('\n')
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("cdp ws handshake read: %w", err)
	}
	if !strings.Contains(statusLine, "101") {
		conn.Close()
		return nil, fmt.Errorf("cdp ws handshake refused: %s", strings.TrimSpace(statusLine))
	}
	wantAccept := wsAccept(key)
	gotAccept := ""
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("cdp ws handshake headers: %w", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			break
		}
		if i := strings.IndexByte(line, ':'); i > 0 &&
			strings.EqualFold(strings.TrimSpace(line[:i]), "Sec-WebSocket-Accept") {
			gotAccept = strings.TrimSpace(line[i+1:])
		}
	}
	conn.SetReadDeadline(time.Time{})
	if gotAccept != wantAccept {
		conn.Close()
		return nil, fmt.Errorf("cdp ws handshake: bad Sec-WebSocket-Accept %q", gotAccept)
	}
	return &cdpConn{conn: conn, br: br, closed: make(chan struct{})}, nil
}

// isLoopbackHost 收紧拨号目标：只放 IPv4/IPv6 回环与 localhost。
// 名字解析后的其它地址（内网、公网）一律拒——DevTools 无鉴权，错连一台主机
// 等于把这台机器上跑着的任意浏览器的控制面暴露出去。
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// wsAccept 是 RFC 6455 的服务端回执：base64(SHA1(key + 固定 GUID))。
func wsAccept(key string) string {
	h := sha1.New()
	io.WriteString(h, key)
	io.WriteString(h, "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// WS 帧操作码，只用到这四个。
const (
	wsOpContinuation = 0x0
	wsOpText         = 0x1
	wsOpClose        = 0x8
	wsOpPing         = 0x9
	wsOpPong         = 0xA
)

// sendText 发一条客户端→服务端的 Text 帧。客户端帧必须 Masked（RFC 6455 §5.3），
// 服务端对 unmasked 客户端帧有权直接关连接，Chromium 就是这么做的。
// 有界写：cdpWriteTimeout 内写不完就报错，不阻塞调用方。
func (c *cdpConn) sendText(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return errors.New("cdp ws: connection closed")
	default:
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(cdpWriteTimeout))
	defer c.conn.SetWriteDeadline(time.Time{})
	return c.writeFrame(wsOpText, payload)
}

// sendPong 回一条 Pong。服务端 Ping 的载荷要原样带回。
func (c *cdpConn) sendPong(payload []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return errors.New("cdp ws: connection closed")
	default:
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(cdpWriteTimeout))
	defer c.conn.SetWriteDeadline(time.Time{})
	return c.writeFrame(wsOpPong, payload)
}

// sendClose 发 Close 帧做礼貌关闭；短 deadline，发不出就算了——
// 调用方随即 conn.Close，礼貌帧的价值只是让对端干净收尾。
func (c *cdpConn) sendClose() {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return
	default:
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(cdpCloseTimeout))
	defer c.conn.SetWriteDeadline(time.Time{})
	_ = c.writeFrame(wsOpClose, nil)
}

// writeFrame 写一帧带 Mask 的客户端帧。调用方必须已持有 writeMu。
//
// 头布局：|FIN+opcode(1B)|MASK+len(1B)|ext len(0/2/8B)|mask key(4B)|payload|。
// 大 payload 走 64-bit 长度位（len>=65536），CDP 的 getCookies 回包很容易超 64KiB。
func (c *cdpConn) writeFrame(op byte, payload []byte) error {
	select {
	case <-c.closed:
		return errors.New("cdp ws: connection closed")
	default:
	}
	var hdr [14]byte
	hdr[0] = 0x80 | op // FIN=1，控制帧与 text 帧我们都不分片
	n := len(payload)
	var pos int
	switch {
	case n < 126:
		hdr[1] = 0x80 | byte(n)
		pos = 2
	case n <= 0xFFFF:
		hdr[1] = 0x80 | 126
		binary.BigEndian.PutUint16(hdr[2:4], uint16(n))
		pos = 4
	default:
		hdr[1] = 0x80 | 127
		binary.BigEndian.PutUint64(hdr[2:10], uint64(n))
		pos = 10
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	copy(hdr[pos:pos+4], mask[:])
	pos += 4
	if _, err := c.conn.Write(hdr[:pos]); err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	// 逐字节 mask 一次写完：payload 可能几 MB，分块写徒增 syscall；
	// 单次 conn.Write 对内核来说就是一次或几次 send，比 4KB 切片循环稳。
	masked := make([]byte, n)
	for i := 0; i < n; i++ {
		masked[i] = payload[i] ^ mask[i%4]
	}
	_, err := c.conn.Write(masked)
	return err
}

// readMessage 读一条完整的服务端消息，返回 (opcode, payload)。
//
// 处理三类帧：
//   - Text/Continuation：服务端帧不 Mask，FIN=0 时把后续 Continuation 拼进来；
//   - Ping：立刻回 Pong 后继续等（Chromium 用它探活）；
//   - Close：回 Close 帧并返回 io.EOF，由上层走重连。
//
// 分片重组和 Ping 都在这个函数里消化，调用方拿到的永远是完整消息或错误。
func (c *cdpConn) readMessage() (byte, []byte, error) {
	var buf []byte
	var msgOp byte
	for {
		op, fin, payload, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch op {
		case wsOpPing:
			_ = c.sendPong(payload)
			continue
		case wsOpPong:
			continue // 我们不主动 Ping，Pong 直接丢
		case wsOpClose:
			c.sendClose()
			return 0, nil, io.EOF
		case wsOpText, wsOpContinuation:
			if msgOp == 0 {
				msgOp = op
			}
			if len(buf)+len(payload) > maxCDPFrameBytes {
				return 0, nil, fmt.Errorf("cdp ws: message exceeds %d bytes", maxCDPFrameBytes)
			}
			buf = append(buf, payload...)
			if fin {
				return msgOp, buf, nil
			}
		default:
			// Binary / 保留 opcode：DevTools 不发，忽略整帧比报错稳
			// （协议错位时上层会从 JSON 解析失败拿到更早的信号）。
			if fin {
				continue
			}
		}
	}
}

// readFrame 读单个帧，返回 (opcode, fin, payload)。
//
// 服务端帧按协议必须 unmasked；万一收到 masked 帧（错误目标、代理插手）直接当
// 协议错误断开，不做半猜半解。
func (c *cdpConn) readFrame() (op byte, fin bool, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(c.br, hdr[:]); err != nil {
		return 0, false, nil, err
	}
	fin = hdr[0]&0x80 != 0
	op = hdr[0] & 0x0F
	if hdr[1]&0x80 != 0 {
		return 0, false, nil, errors.New("cdp ws: masked server frame (protocol violation)")
	}
	n := uint64(hdr[1] & 0x7F)
	switch n {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		n = uint64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return 0, false, nil, err
		}
		n = binary.BigEndian.Uint64(ext[:])
	}
	if n > maxCDPFrameBytes {
		return 0, false, nil, fmt.Errorf("cdp ws: frame %d bytes exceeds limit", n)
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return 0, false, nil, err
	}
	return op, fin, payload, nil
}

// Close 立即打断阻塞中的读写循环并释放连接。幂等。
// 顺序严格：先 close(closed) 置终态标记（新写直接被拒，不再进 writeMu
// 排队），随即 conn.Close()——阻塞中的读与写（含持 writeMu 卡在
// conn.Write 上的那一方）全部立刻被底层断连唤醒；pending 调用方由
// cdpClient.failAll 唤醒。绝不先抢 writeMu 再关：写一个正在阻塞写的
// 持锁者后面排队，Close 自己就被拖住了。
func (c *cdpConn) Close() error {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.conn.Close()
	})
	return nil
}

// Done 返回连接关闭信号，给 select 等断线用。
func (c *cdpConn) Done() <-chan struct{} { return c.closed }
