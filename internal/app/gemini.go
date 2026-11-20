package app

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	"github.com/google/uuid"
)

// Gemini 服务端认的模型 id，来自 batchexecute?rpcids=otAQ7b 返回的权威清单。
const (
	hexFlash36   = "fbb127bbb056c959" // 3.6 Flash
	hexFlashLite = "cf41b0e0dd7d53e5" // 3.5 Flash-Lite
	hexPro31     = "9d8ca3786ebdfbea" // 3.1 Pro
	// 3.7 Flash：按账号灰度放出。hex 是 otAQ7b 里 3.7 条目的**主 hex**（第一个元素），
	// 两个独立已灰度账号的清单里都是它、且有 3.7 号的用户实测发它回报 "3.7 Flash"
	// （issue #4 / PR #11）。注意别用 compat 列表里那个 797f3d0293f288ad —— 那是
	// "当前 Flash" 泛指针，老批次号发它拿到的是 3.6，会冒充 3.7。老批次号发这个主 hex
	// 会干净降级成 3.5 Flash-Lite（跟 3.1 Pro 一样），所以 gate 成要 cookie。
	hexFlash38 = "56fdd199312815e2" // 3.8 Flash（原记 3.7；Google 把这个 hex 原地升成 3.8，HAR 响应帧显示名已是 "3.8 Flash"）
	// 4 Carbon（原 4 Argon，#35）：otAQ7b 清单里主 hex 95a4c0abcba24364、短名 Carbon、
	// 代号 Carbon4p0、显示名 "4 Carbon"（2026-11 由 "4 Argon" 原位改名，hex/Mode 不变）。
	// 付费/Ultra 号专属。实测**真正选中它靠的是 MODE=8（header 下标 14 / inner[79]），不是
	// hex**：借来的 Ultra 号上，hex=Argon 配 mode=1 回报 "3.8 Flash"（mode 1 那个模型），
	// hex=3.8Flash 配 mode=8 反而回报 "4 Argon"——mode 决定、hex 被忽略。所以 Carbon 条目的
	// Mode 必须填 8。仍按浏览器把 hex 配成该槽位主 hex（最忠实、将来若改回认 hex 也不塌）。
	hexArgon4 = "95a4c0abcba24364"
	// 4 Neon：otAQ7b 清单里主 hex e6fa609c3fa255c0、短名 Neon、代号 Neon4p0、显示名
	// "4 Neon"（desc "Deeper analysis"）。Mode=3 路由（mode 决定、hex 被忽略，同 Carbon 的
	// 实测结论）。付费号专属；think=2/4 分别自报 "4 Neon High"/"4 Neon Medium"，均服务端
	// 内部思考、不吐思考链。普通 inner 即可，不需要 argonMode8Slots（那套槽位仅
	// Mode=8 + TaskMode 的 gemini-4-carbon-task）。
	hexNeon4 = "e6fa609c3fa255c0"
)

// innerSlots 是 payload 里 inner 数组的长度。浏览器发 97-98 槽，我们原来只开 80，
// 于是 inner[80]（扩展思考）连位置都没有、想填也填不进去。
//
// 加长本身是安全的：曾经把它从 80 加到 102 来试 inner[79] 会不会复活，结论是不会
// —— 模型选择完全由 x-goog-ext-525001261-jspb header 决定，长度不影响。
const innerSlots = 99

// 4 Carbon 任务模式（mc.TaskMode && Mode==8，即 gemini-4-carbon-task）：按浏览器
// processSession 的 Task 模式抓包补齐槽位（protobuf 字段 N == inner[N-1]，2026-11-09 实测）。
// 这是网页端「任务」（Task）模式开关对应的协议槽位（inner[49]=40 等），**不是** mode-8
// 通用槽位：Mode=8 的普通对话实测走**纯普通 Chat inner**（不补这套槽位）即 HTTP 200，
// 三档思考各自准确自报 "4 Carbon"/"4 Carbon High"/"4 Carbon Medium"（2026-11-19 两轮
// 22 次探测）；补了这套槽位的 Mode=8 请求则是 Task 模式，think=2/4 自报被上游脱后缀回落
// 成裸名 "4 Carbon"。不补任务槽位的话付费号上 Task 模式请求会挂起没有回应。
// BotGuard 槽位 inner[3]/[4] 留空。inner[96] 按抓包恒为 1。
// 仅 mc.TaskMode && Mode==8 时触发——Mode=8 对话模型与其它任务模型（Mode!=8 走
// taskModeSlots）都不得对它补这些槽位。
// 单轮（streamGenerateOnce）与多轮（streamGenerateConv）共用，两条路径发的 inner 必须一致。
func argonMode8Slots(inner []interface{}) {
	inner[6] = []interface{}{1}
	inner[30] = []interface{}{}
	inner[49] = 40
	inner[68] = 2
	inner[83] = 1
	inner[91] = 1
	inner[96] = 1
	inner[98] = 1
}

// taskModeSlots 是 Gemini Web 网页端「任务」（Task）模式实测抓包的槽位补齐
// （protobuf 字段 N == inner[N-1]，槽位值取自浏览器 Task Mode 抓包）。
// inner[49]=40 是任务模式开关（媒体工具的 inner[49] 取值 2/11/14/21 互不冲突）。
// 仅 mc.TaskMode && Mode!=8 时调用——Mode==8 的任务模式（gemini-4-carbon-task）走
// argonMode8Slots，两块不得混用。
// 单轮（streamGenerateOnce）与多轮（streamGenerateConv）共用，两条路径发的 inner 必须一致。
func taskModeSlots(inner []interface{}) {
	inner[6] = []interface{}{0}
	inner[30] = []interface{}{4, 27, 28, 16, 19, 12}
	inner[49] = 40
	inner[68] = 1
	inner[83] = 1
	inner[91] = 1
	inner[96] = 1
	inner[98] = 1
}

// inner[80] / 模型 header 下标 15 的取值：1=普通，2=扩展思考。
//
// **只在登录态生效**：匿名请求带上它服务端静默忽略，回报的仍是普通模型、思考链 0
// 字符（对齐登录态抓包参数复测 3 组，8 次全灭）。所以带这个的模型跟 3.1 Pro 一样，
// 没 cookie 时不暴露。
const (
	thinkingNormal   = 1
	thinkingExtended = 2
	// thinkingMedium 是 4 Argon/Carbon 网页 UI「中等」思考档的线值（低=1 / 高=2 / 中=4）。
	// 判据：2026-10-19 的 processSession 抓包里选中档时 think 槽（新协议字段 #81 =
	// 老 inner[80]）发的就是 4；与 #37 自述一致。中档的思考在服务端内部完成、响应
	// 不吐思考链，所以不映射 reasoning_content。
	thinkingMedium = 4
)

// ModelConfig holds the server-side model id plus the legacy MODE_CATEGORY value.
type ModelConfig struct {
	// HexID 走 x-goog-ext-525001261-jspb header，是服务端唯一认的模型开关。
	// 实测：不发这个 header 时 inner[79] 取 1..6 全部落到 3.5 Flash-Lite；
	// header 写 3.6 而 inner[79] 写 6 时拿到的是 3.6 —— header 压过 inner[79]。
	HexID string
	Mode  int
	Desc  string
	// Thinking 为真时填 inner[80]=2，即网页 UI 上的「扩展思考」。跟 HexID 正交
	// —— 三个模型都能开，不是某个专属模型。
	Thinking bool
	// ThinkWire >0 时直接作为思考档线值（header 下标15 / inner[80]），覆盖 Thinking
	// 推出的 1/2。用于 4 Carbon「中等」档（=thinkingMedium=4）。两者都不设 = 普通(1)。
	ThinkWire int
	// TaskMode 对应 Gemini Web 网页端「任务」（Task）模式开关：为 true 时按实测
	// 抓包补任务槽位（Mode==8 走 argonMode8Slots，Mode!=8 走 taskModeSlots，均含
	// inner[49]=40）。与对话模式分离的独立模型（gemini-4-carbon-task /
	// gemini-4-neon-task / gemini-4-hydrogen-task）。
	TaskMode bool
	// Tool 非 0 时填 inner[49]，让服务端换后端模型生成媒体产物（14=生图 Nano Banana
	// / 21=音乐 Lyria）。产物不在 StreamGenerate 响应里，要再走 hNvQHb 拿下载链、
	// 用下载 host 认的 cookie 子集下回原始字节，见 media.go。跟登录态绑定：匿名请求
	// 这个会被静默降级成一句「Are you signed in?」文本。
	Tool int
	// Upstream 是真用上这个模型时服务端自报的显示名（不含思考档后缀："3.1 Pro High"
	// 记 "3.1 Pro"）。strict_model 拿它判静默降级，空 = 不判（媒体工具回报的是
	// "Nano Banana 2" 这类后端名）。只能用显示名：跟它并排的 hex 现在不管实际用了
	// 哪个模型都回 Flash-Lite 的那个。
	Upstream string
	// VideoAspect is the inner[55] aspect code for video: 16 = 16:9 (default), 17 = 9:16.
	VideoAspect int
	// PinAccountID forces the request onto one cookie-pool account (no fallback to
	// others). Required to continue a conversation, which lives in that account.
	PinAccountID int64
	// Cont, when set, sends this request as the next turn of an existing Gemini
	// conversation (used for video extension: "reply to the video").
	Cont *ConvRef
}

// ConvRef identifies the turn to continue: conversation id, the previous turn's
// response id and response-candidate id, and that turn's index (first turn = 0).
type ConvRef struct {
	CID, RID, RCID string
	Turn           int
}

// thinkWire 返回思考档线值（填 header 下标15 / inner[80]）：ThinkWire 显式指定优先，
// 否则 Thinking→扩展(2)、默认普通(1)。对现有模型取值跟旧的 normal/extended 逐字一致。
func (mc ModelConfig) thinkWire() int {
	if mc.ThinkWire > 0 {
		return mc.ThinkWire
	}
	if mc.Thinking {
		return thinkingExtended
	}
	return thinkingNormal
}

// 只暴露服务端清单（batchexecute?rpcids=otAQ7b）里真实存在的模型。
// 旧的 gemini-3.5-flash / -thinking / -thinking-lite / gemini-auto /
// gemini-flash-lite 别名已移除：它们在服务端没有对应条目，留着只会让人
// 以为有五种不同的模型可选。
var Models = map[string]ModelConfig{
	"gemini-3.6-flash":      {HexID: hexFlash36, Mode: 1, Upstream: "3.6 Flash", Desc: "Latest all-around model"},
	"gemini-3.5-flash-lite": {HexID: hexFlashLite, Mode: 6, Upstream: "3.5 Flash-Lite", Desc: "Fastest, lightweight"},
	"gemini-3.1-pro":        {HexID: hexPro31, Mode: 3, Upstream: "3.1 Pro", Desc: "Most capable; needs a signed-in cookie (downgraded to Flash-Lite without one)"},
	// 3.8 Flash：Google 把原 3.7 那个 hex 原地升级了（同 hex，服务端显示名从 3.7 变 3.8）。
	// 实测是**付费号专属**（不是灰度）：只有付费 Gemini 账号有，免费号降级成 3.5 Flash-Lite。
	// gemini-3.7-flash 保留为别名（同 hex）。
	"gemini-3.8-flash": {HexID: hexFlash38, Mode: 1, Upstream: "3.8 Flash", Desc: "3.8 Flash; needs a signed-in PAID Google account (free accounts get downgraded to 3.5 Flash-Lite)"},
	"gemini-3.7-flash": {HexID: hexFlash38, Mode: 1, Upstream: "3.8 Flash", Desc: "alias of gemini-3.8-flash (same hex; Google renamed 3.7→3.8)"},
	// 4 Hydrogen：Google 2026-11 把同一槽位（hexFlash38 / Mode=1）从 3.8 Flash 原位升级成
	// "4 Hydrogen"。与 3.8 三条完全同 hex 同 Mode，仅对外名和登记的 Upstream 名不同。
	// 仍 gate 成要 cookie（modelNeedsLogin 按 hexFlash38 覆盖）。
	"gemini-4-hydrogen": {HexID: hexFlash38, Mode: 1, Upstream: "4 Hydrogen", Desc: "4 Hydrogen (Everyday help, replaced 3.8 Flash on same slot); needs a signed-in Google account"},
	// 4 Carbon（#35）：Mode=8 是真正的选择开关（见 hexArgon4 注释）。付费/Ultra 号专属，
	// 免费号发它会被静默降级成 3.5 Flash-Lite，所以 gate 成要 cookie（modelNeedsLogin）。
	// 对话模式走**纯普通 Chat inner**（不再补 argonMode8Slots）：实测 think=1/2/4 均
	// HTTP 200、分别准确自报 "4 Carbon"/"4 Carbon High"/"4 Carbon Medium"；补了
	// argonMode8Slots（inner[49]=40）的 Mode=8 请求实为网页端 Task 模式，由
	// gemini-4-carbon-task 承接。
	"gemini-4-carbon": {HexID: hexArgon4, Mode: 8, Upstream: "4 Carbon", Desc: "4 Carbon (renamed 4 Argon, same slot); needs a signed-in PAID/Ultra Google account"},
	// 4 Neon：Mode=3 路由（hexNeon4 注释）。上游实测 think=1/2/4 均 HTTP 200、自报
	// "4 Neon"/"4 Neon High"/"4 Neon Medium"。付费号专属，gate 成要 cookie。
	"gemini-4-neon": {HexID: hexNeon4, Mode: 3, Upstream: "4 Neon", Desc: "4 Neon (deeper analysis); needs a signed-in PAID/Ultra Google account"},
	// 任务模式（Task Mode）：与对话模式分离的独立模型，走任务槽位（Mode==8 走
	// argonMode8Slots、其余走 taskModeSlots，均含 inner[49]=40）。不注册 -task-thinking
	// 变体，让下游插件天然以独立模型呈现。
	// gemini-4-carbon-task：hexArgon4 / Mode=8 + think=2 + argonMode8Slots。实测该组合是
	// 网页端 Task 模式（think=2/4 自报均被脱后缀回落为裸名 "4 Carbon"），与 neon-task
	// 同构；身份核验按 taskMode 放行裸名/High 名两套。ThinkWire=thinkingExtended 固定走高档线值。
	"gemini-4-carbon-task": {HexID: hexArgon4, Mode: 8, ThinkWire: thinkingExtended, TaskMode: true, Upstream: "4 Carbon", Desc: "4 Carbon task mode (web Task switch; separate from chat mode); needs a signed-in PAID/Ultra Google account"},
	// gemini-4-neon-task：hexNeon4 / Mode=3 + think=2 + inner[49]=40 时上游实测自报
	// 裸名 "4 Neon"（而非对话模式 think=2 的 "4 Neon High"），argon.go 身份核验按
	// taskMode 放行裸名/High 名两套。ThinkWire=thinkingExtended 固定走高档线值。
	"gemini-4-neon-task": {HexID: hexNeon4, Mode: 3, ThinkWire: thinkingExtended, TaskMode: true, Upstream: "4 Neon", Desc: "4 Neon task mode (web Task switch; separate from chat mode); needs a signed-in PAID/Ultra Google account"},
	// gemini-4-hydrogen-task：hexFlash38 / Mode=1 + think=2 + 任务模式槽位。
	// hexFlash38 槽位不在 argon 强身份核验集合（非 hexArgon4/hexNeon4），降级检测靠
	// strict_model（Upstream "4 Hydrogen"）。
	"gemini-4-hydrogen-task": {HexID: hexFlash38, Mode: 1, ThinkWire: thinkingExtended, TaskMode: true, Upstream: "4 Hydrogen", Desc: "4 Hydrogen task mode (web Task switch; separate from chat mode); needs a signed-in Google account"},

	// 扩展思考版。inner[80]=2 跟模型 hex 正交，都能开；但只在登录态生效，
	// 所以跟 3.1 Pro 一样在没 cookie 时不暴露。
	"gemini-3.6-flash-thinking":        {HexID: hexFlash36, Mode: 1, Thinking: true, Upstream: "3.6 Flash", Desc: "3.6 Flash with extended thinking; needs a signed-in cookie"},
	"gemini-3.5-flash-lite-thinking":   {HexID: hexFlashLite, Mode: 6, Thinking: true, Upstream: "3.5 Flash-Lite", Desc: "3.5 Flash-Lite with extended thinking; needs a signed-in cookie"},
	"gemini-3.1-pro-thinking":          {HexID: hexPro31, Mode: 3, Thinking: true, Upstream: "3.1 Pro", Desc: "3.1 Pro with extended thinking; needs a signed-in cookie"},
	"gemini-3.8-flash-thinking":        {HexID: hexFlash38, Mode: 1, Thinking: true, Upstream: "3.8 Flash", Desc: "3.8 Flash with extended thinking; needs a signed-in cookie"},
	"gemini-3.7-flash-thinking":        {HexID: hexFlash38, Mode: 1, Thinking: true, Upstream: "3.8 Flash", Desc: "alias of gemini-3.8-flash-thinking"},
	"gemini-3.8-flash-thinking-medium": {HexID: hexFlash38, Mode: 1, ThinkWire: thinkingMedium, Upstream: "3.8 Flash", Desc: "3.8 Flash at the Medium thinking tier; needs a signed-in Google account"},
	// 4 Carbon + 扩展思考：Mode=8 + inner[80]=2（上游现在把这档叫 "High"）。
	// 对话模式实测自报 "4 Carbon High"（滚动期也收裸名 "4 Carbon"，见 argon.go）。
	// Thinking=true 使 thinkWire()==2（高档线值）——ThinkWire 不设时靠它推档，必须保留。
	"gemini-4-carbon-thinking": {HexID: hexArgon4, Mode: 8, Thinking: true, Upstream: "4 Carbon", Desc: "4 Carbon with extended thinking (High tier); emits reasoning_content; needs a signed-in PAID/Ultra account"},
	// 中档：think 线值 4（高档是 2），对话模式实测自报 "4 Carbon Medium"。中档思考在
	// 服务端内部完成、不吐思考链，所以不设 Thinking（不期望 reasoning_content），
	// 登录门控靠 hexArgon4 + thinkWire。
	"gemini-4-carbon-thinking-medium": {HexID: hexArgon4, Mode: 8, ThinkWire: thinkingMedium, Upstream: "4 Carbon", Desc: "4 Carbon at the Medium thinking tier; needs a signed-in PAID/Ultra account"},
	// 4 Neon + 思考档：Mode=3（选 Neon），高档 think 线值 2、自报 "4 Neon High"，中档
	// thinkingMedium=4、自报 "4 Neon Medium"。实测三档都不吐思考链——思考在服务端内部
	// 完成，不设 Thinking、不期望 reasoning_content。
	"gemini-4-neon-thinking":        {HexID: hexNeon4, Mode: 3, Thinking: true, Upstream: "4 Neon High", Desc: "4 Neon High thinking tier (thinks server-side, no reasoning_content); needs a signed-in PAID/Ultra account"},
	"gemini-4-neon-thinking-medium": {HexID: hexNeon4, Mode: 3, ThinkWire: thinkingMedium, Upstream: "4 Neon Medium", Desc: "4 Neon Medium thinking tier (thinks server-side, no reasoning_content); needs a signed-in PAID/Ultra account"},
	// 4 Hydrogen 思考档：与 3.8 Flash 同槽位（hexFlash38 / Mode=1），高档自报 "4 Hydrogen High"
	// 并吐思考链，中档 think 线值 thinkingMedium、服务端内部思考不吐链。
	"gemini-4-hydrogen-thinking":        {HexID: hexFlash38, Mode: 1, Thinking: true, Upstream: "4 Hydrogen", Desc: "4 Hydrogen with extended thinking (High tier); emits reasoning_content; needs a signed-in Google account"},
	"gemini-4-hydrogen-thinking-medium": {HexID: hexFlash38, Mode: 1, ThinkWire: thinkingMedium, Upstream: "4 Hydrogen", Desc: "4 Hydrogen at the Medium thinking tier (thinks server-side, no reasoning_content); needs a signed-in Google account"},

	// 媒体生成。inner[49] 一填，服务端换后端模型出图/出乐；产物走 hNvQHb + 下载 host
	// 取回，以 base64 data URL 塞进 content 返回。都要登录态，没 cookie 时不暴露。
	"gemini-image": {HexID: hexFlash36, Mode: 1, Tool: toolImage, Desc: "Image generation (Nano Banana); returns a base64 data URL; needs a signed-in cookie"},
	"gemini-music": {HexID: hexFlash36, Mode: 1, Tool: toolMusic, Desc: "Music generation (Lyria, ~30s); returns a base64 data URL; needs a signed-in cookie"},
	"gemini-video": {HexID: hexFlash36, Mode: 1, Tool: toolVideo, Desc: "Video generation (Veo, async); returns a base64 data URL; needs a signed-in cookie (usually a paid account)"},
	// 画布：生成 immersive 交互 HTML 文档，内联返回（不是二进制、不用下载）。要登录态。
	"gemini-canvas": {HexID: hexFlash36, Mode: 1, Tool: toolCanvas, Desc: "Canvas: generates an interactive HTML document (returned inline as a ```html block); needs a signed-in cookie"},
}

// hasCookie 表示 cookie 池里有没有可用账号。决定 3.1 Pro 是否出现在模型列表里。
func hasCookie() bool {
	_, enabled := accountCount()
	return enabled > 0
}

// modelNeedsLogin 判断一个模型是否必须登录态才真正生效。匿名请求这批会被服务端
// 静默降级：3.1 Pro / 3.8 Flash → 3.5 Flash-Lite；思考链消失；媒体工具变成一句
// "Are you signed in?" 文本。是「没 cookie 时排除哪些模型」和「#20 匿名优先要不要
// 占一个 cookie 账号」共用的单一判据。
//
// 按 HexID + Thinking + ThinkWire + TaskMode + Tool 判，跟旧的按模型名
// （3.1-pro/3.8-flash/3.7-flash）判逐模型核对等价：那三个正名的 HexID 就是
// hexPro31 / hexFlash38，其余登录模型都被 Thinking / ThinkWire / Tool 覆盖。
// TaskMode 本身必须登录（任务模式是账号功能），冗余列入不影响既有模型。
func modelNeedsLogin(mc ModelConfig) bool {
	return mc.HexID == hexPro31 || mc.HexID == hexFlash38 || mc.HexID == hexArgon4 || mc.HexID == hexNeon4 || mc.Thinking || mc.ThinkWire > 0 || mc.Tool > 0 || mc.TaskMode
}

// anonFirstEligible 判断「匿名优先」开关下这次请求能不能走匿名（不占 cookie 账号）：
// 开关开着 + 没带附件 + 模型不需要登录态。附件（图/视频）在对话里引用必须登录
// （匿名会被上游回 1100），所以带附件时一律挑号。开关关着永远返回 false（保持旧行为：
// 池里有号就用号）。
func anonFirstEligible(mc ModelConfig, hasAttachment bool) bool {
	return rtCfg().AnonFirst && !hasAttachment && !modelNeedsLogin(mc)
}

// temporaryChatEligible 判断这次请求该不该走网页端原生「临时对话」（inner[45]=1，#34）。
//
// 与 multi_turn 冲突：临时对话不进会话列表，而 multi_turn 要按历史认出续接、靠服务端
// 留着这串对话——两个一起开，续接那半就没了。所以 multi_turn 开着时一律不发 inner[45]，
// 由 multi_turn 决定会话怎么留。调用点还会再要求有登录 cookie：匿名本来就不落账号，
// 发 inner[45] 是没验过的无谓偏离。
func temporaryChatEligible() bool {
	return rtCfg().TemporaryChat && !rtCfg().MultiTurn
}

// availableModels 返回当前配置下值得暴露的模型。
//
// 没配 cookie 时排除 3.1 Pro：实测匿名请求它会被静默降级成 3.5 Flash-Lite，
// 客户端还以为自己用上了 Pro。与其让它"成功"，不如直接不提供、让选型时就报错。
//
// 配了有效 cookie 时它是真能用的：连打 6 次服务端回报的都是 "3.1 Pro" 本身，
// 且每次都带思考链（118-152 字符，普通 3.6 Flash 为 0）。早前记录的「免费号
// 登录也只能拿到 3.6 Flash 扩展」是在缺 XSRF token 的条件下测的，那时候带
// cookie 的请求根本发不出去（见 xsrf.go）。
func availableModels() map[string]ModelConfig {
	if hasCookie() {
		return Models
	}
	out := make(map[string]ModelConfig, len(Models))
	for k, v := range Models {
		if modelNeedsLogin(v) {
			continue
		}
		out[k] = v
	}
	return out
}

// resolveModel maps a model name to its config.
//
// "name@think=N" 后缀会被剥掉并忽略。旧版本把它写进 inner[17] 当思考深度，
// 那是误读：抓包显示 inner[17] 是会话内的轮次索引（首轮 [[0]]，带会话 id 的
// 第二轮 [[1]]，逐轮递增），跟思考深度无关。我们每次都开新会话，该值恒为 0。
// 后缀不报错只忽略，避免打断已经配了这个写法的客户端。
func resolveModel(modelName string) (string, ModelConfig, error) {
	if idx := strings.Index(modelName, "@think="); idx >= 0 {
		modelName = modelName[:idx]
	}
	mc, ok := availableModels()[modelName]
	if !ok {
		if full, exists := Models[modelName]; exists && !hasCookie() {
			downgrade := "are silently downgraded to 3.5 Flash-Lite"
			if full.Tool > 0 {
				downgrade = "are silently downgraded to a plain \"Are you signed in?\" text reply"
			}
			return "", ModelConfig{}, fmt.Errorf(
				"%s is unavailable without a Google account cookie: anonymous requests for it "+
					"%s. Add a cookie in the admin panel (Cookie pool) or via --cookie-file to enable it",
				modelName, downgrade)
		}
		return "", ModelConfig{}, fmt.Errorf("unknown model: %s", modelName)
	}
	return modelName, mc, nil
}

// StreamResult holds raw body + per-request proxy + timing info.
type StreamResult struct {
	// Emitted 是流式模式下已经通过 onDelta 发出去的文本；非流式为空。
	Emitted string
	Raw     string
	// Reasoning 是模型的思考链（只有 3.1 Pro 会产出）。
	// 上游每次都发，我们以前只取正文、把它扔了。
	Reasoning string
	// EmittedReasoning 是流式下已经通过 onReasoning 发出去的思考链；非流式为空。
	EmittedReasoning string
	// UpstreamModel 是服务端在响应帧 [42] 里自报的模型显示名。
	// 跟请求的模型未必一致：gemini-3.1-pro 匿名时被静默降级成 3.5 Flash-Lite，
	// 只看请求名根本发现不了，所以这个字段要一直记着。
	UpstreamModel string
	ProxyID       int64
	ProxyName     string
	// 用了 cookie 池里的哪个账号，0 = 匿名。失败的请求也要带上——
	// 排查"加了 cookie 就大面积失败"时，最需要知道的正是失败那条用的哪个号。
	AccountID    int64
	AccountLabel string
	// Conversation ids of this turn (cid, rid, rcid), so a caller can continue it.
	ConvCID, ConvRID, ConvRCID string
	TTFBMs                     int64
	TotalMs                    int64
	// Artifacts 是媒体模型（生图/音乐）取回的产物原始字节，非媒体模型为空。
	Artifacts []MediaArtifact
	// MediaErr 记媒体产物取回失败的原因：生成本身 200 了、但走 hNvQHb / 下载那步挂了。
	// 调用方据此报错，而不是返回一个只有文字没有图的「半成功」。
	MediaErr error
	// Diag 是失败路径上游响应的结构化现场（帧数/有无内容帧/自报模型名），
	// 由上游响应到达时即时解算——完整 raw 只在内存里过一遍解析器就丢，
	// 不往 StreamResult 留原文（含内部 token，不能进 502 body 或 requests 表）。
	// 成功路径不填：Raw 本身已完整带回给正常消费者。
	Diag upstreamDiagMeta
}

// RateLimitError 表示所有 IP slot 都达到了限流上限。
// HTTP handler 看到这个错时返回 429 给客户端。
type RateLimitError struct {
	Reason  string // "concurrent" / "rpm" / "rph"
	ProxyID int64  // 0 = 直连 slot 满
}

func (e *RateLimitError) Error() string {
	if e.ProxyID == 0 {
		return "direct IP slot full: " + e.Reason + " limit reached (configure proxies to scale)"
	}
	return "all proxy slots full: " + e.Reason + " limit reached"
}

// acquireSlot 选一个有容量的 slot 给本次请求用。
// 优先级：代理池里有容量的代理 → 直连。
// 全满返回 *RateLimitError。
//
// 调用方拿到 (proxy, ok=true) 必须配 deferred releaseSlot()。
func acquireSlot(preferProxyID int64) (Proxy, bool, error) {
	// 1. 先试代理池（如果配了）
	proxyMu.RLock()
	hasProxies := len(proxyCache) > 0
	proxyMu.RUnlock()

	if hasProxies {
		if p, ok, reason := pickProxyPreferring(preferProxyID); ok {
			return p, true, nil
		} else if !rtCfg().FallbackDirect {
			if reason == "unavailable" {
				return Proxy{}, false, fmt.Errorf("proxy_unavailable: all configured proxies are disabled or in circuit-breaker cooldown")
			}
			return Proxy{}, false, &RateLimitError{Reason: reason, ProxyID: -1}
		}
		// 代理池里有代理但一个都用不上（限流满 / 全禁用 / 全熔断且没过冷却）。
		// 默认**不退回直连** —— 配了代理池就意味着不想让上游看到本机 IP，
		// 悄悄直连等于把这个前提废掉，而且日志上只是几条普通请求，很难发现。
		// 要可用性优先于隐藏 IP 的部署可以打开 fallback_direct。
		if ok, reason := trySlotAcquire(0); ok {
			logf("[proxy] 代理池无可用出口，本次退回直连（fallback_direct 已开）")
			return Proxy{}, true, nil
		} else {
			return Proxy{}, false, &RateLimitError{Reason: reason, ProxyID: 0}
		}
	}

	// 2. 没配代理池 → 用直连 slot（id=0）
	if ok, reason := trySlotAcquire(0); ok {
		return Proxy{}, true, nil // ProxyID=0 表示直连
	} else {
		return Proxy{}, false, &RateLimitError{Reason: reason, ProxyID: 0}
	}
}

// 一个请求最多试几个 cookie。池子大时挨个试到底会让失败请求拖很久，
// 而连试 3 个都不行基本说明是池子整体的问题，不是撞上个别坏号。
const maxCookieTries = 3

// releaseSlot 释放占用。proxyID=0 表示直连。
func releaseSlot(proxyID int64) {
	slotRelease(proxyID)
}

// deltaTracker 把上游的累积帧转成增量。
//
// 上游每帧带的是**到目前为止的全文**，不是新增部分，所以要跟已发出的做前缀
// 比对。帧之间偶尔不满足前缀关系（模型改写、或 clean 掉的 artifact 落在边界
// 上），这时宁可跳过也不能发——发了就等于把重复内容推给客户端，而已发出的
// 内容收不回来。漏掉的部分由调用方在结束时用 remainingText 补齐。
type deltaTracker struct{ emitted string }

// Push 吃进一帧的累积全文，返回相对上一次的增量；没有新增或无法安全 diff 时返回 ""。
func (d *deltaTracker) Push(fullText string) string {
	cleaned := cleanGeminiText(fullText)
	if len(cleaned) <= len(d.emitted) || !strings.HasPrefix(cleaned, d.emitted) {
		return ""
	}
	delta := cleaned[len(d.emitted):]
	d.emitted = cleaned
	return delta
}

// pinnedAccountError 表示 PinAccountID 指定的号用不了：Err 为 nil 是号被删 / 停用，
// 否则是登录态失效（Err 是取 XSRF 的错）。调用方靠它判断「换下一个号」，不比文案。
type pinnedAccountError struct {
	ID  int64
	Err error
}

func (e *pinnedAccountError) Error() string {
	if e.Err == nil {
		return fmt.Sprintf("account #%d is not available (deleted or disabled)", e.ID)
	}
	return fmt.Sprintf("pinned account #%d is not signed in: %v", e.ID, e.Err)
}

func (e *pinnedAccountError) Unwrap() error { return e.Err }

// streamGenerate POSTs to Gemini's StreamGenerate endpoint and returns raw body
// plus proxy/timing telemetry for the metrics layer.
// The 80-slot inner array is verbatim from the Python reference.
// onDelta 非 nil 时开启真流式：上游每写一帧就解析一次，跟已发出的内容做前缀
// diff，把新增部分立刻回调出去。上游每帧带的是累积全文而不是增量，diff 必须
// 自己做。一旦已经吐过内容就不再重试——重试会让客户端收到重复文本。
func streamGenerate(prompt, latest string, mc ModelConfig,
	onDelta, onReasoning func(string)) (*StreamResult, error) {
	return streamGenerateWithFiles(context.Background(), prompt, promptCtx{Latest: latest}, mc, nil, onDelta, onReasoning, nil)
}

// fileRef 是一个已上传附件的引用。
type fileRef struct {
	Ref  string // 上传返回的路径，形如 /contrib_service/ttl_1d/…
	Name string // 展示给模型看的文件名
	Kind int    // 附件类型：1=图片，3=文本/普通文件
	Mime string // 内容类型，服务端按它决定怎么解析附件
}

// streamGenerateWithFiles 同上，但可以带附件。
//
// 附件填 inner[0][3]，形状 [[[ref, 1], "文件名"], …]。附件只在登录态可用：
// 匿名能把文件传上去，但对话里一引用就被服务端回 1100。
//
// stopWhen 非 nil 时，每帧拿当前累积正文问一次；返回 true 就不再读上游、按已收到
// 的内容收尾。用于工具调用：围栏闭合时调用就已确定，上游最后一次改正文之后还要
// 再推几秒收尾帧（Argon 实测约 2.3 秒），没必要让客户端干等。
//
// ctx 是客户端请求的 context：客户端断开后中止上游读取、上传和重试等待，不再替
// 已经走掉的请求生成（否则账号照样被占用、重试还会再发一次，限流名额也一直占着）。
// 取消导致的失败不记进代理 / cookie 健康度，那不是它们的错。
func streamGenerateWithFiles(ctx context.Context, prompt string, pc promptCtx, mc ModelConfig, pending []pendingUpload,
	onDelta, onReasoning func(string), stopWhen func(string) bool) (*StreamResult, error) {
	// strict_model 换号：被降级的号记冷却、本次排除，换下一个号重来。上限跟 XSRF 换号
	// 一样是 maxCookieTries——连着几个号都降级，说明池子整体见底，不是撞上个别号。
	exclude := map[int64]bool{}
	var res *StreamResult
	var err error
	for try := 0; try < maxCookieTries; try++ {
		next, nextErr := streamGenerateOnce(ctx, prompt, pc, mc, pending, onDelta, onReasoning, stopWhen, exclude)
		var exhausted *ModelDowngradedError
		if try > 0 && errors.As(nextErr, &exhausted) && exhausted.Reported == "" {
			// 换号时已经没号可挑：报上一个号真实的降级（带自报模型名），补上解冻时间
			var last *ModelDowngradedError
			if errors.As(err, &last) {
				last.RetryAfter = exhausted.RetryAfter
			}
			return res, err
		}
		res, err = next, nextErr
		if !retryOnAnotherAccount(res, err, mc, exclude) {
			return res, err
		}
	}
	return res, err
}

func streamGenerateOnce(ctx context.Context, prompt string, pc promptCtx, mc ModelConfig, pending []pendingUpload,
	onDelta, onReasoning func(string), stopWhen func(string) bool, exclude map[int64]bool) (*StreamResult, error) {
	var files []fileRef

	// 先挑号，再按它上次绑的出口挑代理 —— 同一个账号要尽量固定从同一个 IP 出去，
	// 否则一个号在几十个出口之间跳，在 Google 眼里就是账号共享的特征。
	//
	// 挑号排在 acquireSlot 之前不违反「取 XSRF 必须走正式出口」：挑号只读库、
	// 不发请求，真正发请求的是下面的 getXSRF，它在拿到 slot 之后。
	// #20 匿名优先：不需要登录态能力时不占用 cookie 账号，走匿名省额度（见 anonFirstEligible）。
	// 带附件（图/视频）时不能走匿名——下面 uploadBytes 那段会因 cookieStr=="" 直接报错。
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var acct *CookieAccount
	skip := exclude
	if mc.PinAccountID > 0 {
		a := accountByID(mc.PinAccountID)
		if a == nil || a.Status != "enabled" {
			return &StreamResult{}, &pinnedAccountError{ID: mc.PinAccountID}
		}
		acct = a
	} else if !anonFirstEligible(mc, len(pending) > 0) {
		if shouldUseBrowserTransport() {
			// 策略 M1：常驻浏览器有粘性主号且它还能用时，优先继续驻留
			// （同 cookieIdentity 的 authuser=0..9 共用同一 /app Tab），
			// 不轻易切号——切一次就是 30s 冷却 + 清种 cookie。
			// 传真实 mc.Upstream：strict_model 的模型级冷却按模型登记，
			// 传空串等于把「这个模型正被该号降级冷却」的事实吞掉。
			acct = stickyResidentAccountFor(exclude, mc.Upstream)
		}
		if acct == nil {
			a, s, err := pickAccountForModel(mc, exclude)
			if err != nil {
				return &StreamResult{}, err
			}
			acct, skip = a, s
		}
	}
	preferProxy := int64(0)
	if acct != nil {
		preferProxy = acct.ProxyID
	}

	// 出错时也要把「用了哪个号 / 哪个出口」带回去，否则失败记录里全是空白。
	// picked 是拿到 slot 之后才填的，闭包捕获它，后面每次 attrib 都带上当时的出口。
	// lastDiag 记的是最后一次上游响应即时解算出的帧统计/内容帧/自报模型名（只留
	// 结构化计数，不留 raw 原文——原文含内部 token，不能进 502 body 或 requests 表）。
	var picked Proxy
	var cookieID int64
	var cookieLabel string
	var lastDiag upstreamDiagMeta
	attrib := func(err error) (*StreamResult, error) {
		return &StreamResult{
			AccountID: cookieID, AccountLabel: cookieLabel,
			ProxyID: picked.ID, ProxyName: picked.Name,
			Diag: lastDiag, UpstreamModel: lastDiag.Model,
		}, err
	}

	// 通过限流器拿一个 slot（代理或直连）。所有 slot 满 → 直接 429。
	p, slotOK, slotErr := acquireSlot(preferProxy)
	if !slotOK {
		return attrib(slotErr)
	}
	picked = p
	defer releaseSlot(picked.ID) // picked.ID=0 表示直连 slot

	// picked.URL 为空 = 直连 slot。代理只有代理池一个入口，没有别的兜底出口了
	// （原来那个「静态代理」字段已并进池子，见 seedProxiesFromConfig）。
	proxyURL := picked.URL
	pickedOK := picked.ID > 0 // 是否真用了代理池里的代理

	// 取 XSRF token。一个 cookie 失效不该让整个请求失败：当前号取不到就换下一个，
	// 最多试 maxCookieTries 个。不这么做的话，池子里 2 个号坏 1 个就会让大约一半
	// 请求挂掉，而每次失败看起来都像上游的问题，用户只会觉得"成功率莫名很低"。
	//
	// 换号**不换出口**：出口已经按第一个号的绑定选定了，同一个请求里再换出口没道理。
	cookieStr, sapisid, xsrfToken := "", "", ""
	authUser := 0 // 最终用上的那个号的账号序号（/u/N），匿名为 0
	var lastCookieErr error
	tried := map[int64]bool{}
	// 每个号只给一次「轮转后重试」的机会，避免在一个请求里反复打 accounts.google.com
	rotatedOnce := map[int64]bool{}
	for acct != nil && len(tried) < maxCookieTries {
		if err := ctx.Err(); err != nil {
			return attrib(err)
		}
		tried[acct.ID] = true
		// 归属先记上：这一轮失败了也留痕，面板上看得出是哪个号在坏
		cookieID, cookieLabel = acct.ID, accountDisplayName(acct)
		tok, err := getXSRF(acct.Cookie, acct.AuthUser, proxyURL)
		if err == nil {
			cookieStr, sapisid, xsrfToken = acct.Cookie, extractSAPISID(acct.Cookie), tok
			authUser = acct.AuthUser
			// 只在「还没绑过」或「绑的出口已经没了」时写绑定。
			//
			// 绝不因为"这次走的是别的出口"就覆盖：出口是按**本次第一个挑中的号**
			// 的绑定选的，而挑号会换（新加的号 last_used_at=0 排在最前，撞上坏号
			// 就会换）。拿别人的出口覆盖当前号的绑定，等于每次撞上坏号就把好号的
			// 粘性打散一次 —— 实测就是这么散掉的。
			if acct.ProxyID == 0 || !proxyUsableByID(acct.ProxyID) {
				bindAccountProxy(acct.ID, picked.ID)
			}
			break
		}
		// 取不到 SNlM0e 基本等于这个 cookie 已失效（页面把我们当匿名用户了）。
		// 换号之前先给它一次机会：强制轮转一次再重取。
		//
		// 轮转会换发 __Secure-1PSIDTS（约 30 分钟过期的那张票）并合并 *SIDCC。
		// 只是票过期、持久身份还在的号，这一步能救回来；救不回来再换号。
		// 只试一次，且只在这一轮。
		if !rotatedOnce[acct.ID] {
			rotatedOnce[acct.ID] = true
			if _, _, rerr := rotateAccount(*acct); rerr == nil {
				if fresh := accountByID(acct.ID); fresh != nil {
					if tok2, err2 := getXSRF(fresh.Cookie, fresh.AuthUser, proxyURL); err2 == nil {
						logf("[cookie] 账号 #%d 轮转后恢复可用", acct.ID)
						acct = fresh
						cookieStr, sapisid, xsrfToken = fresh.Cookie, extractSAPISID(fresh.Cookie), tok2
						authUser = fresh.AuthUser
						if fresh.ProxyID == 0 || !proxyUsableByID(fresh.ProxyID) {
							bindAccountProxy(fresh.ID, picked.ID)
						}
						break
					}
				}
			}
		}
		// 救不回来：是 cookie 的错就记一次失败，让面板上看得出是哪个号该换了；
		// 断网、出口被拦不记（见 xsrfErrBlamesCookie）。然后换下一个。
		if xsrfErrBlamesCookie(err) {
			markCookieByStatus(acct.ID, 401, err.Error())
		}
		lastCookieErr = err
		logf("[cookie] 账号 #%d 不可用，换下一个：%v", acct.ID, err)
		if mc.PinAccountID > 0 {
			acct = nil // pinned: never switch to another account mid-conversation
			continue
		}
		next := map[int64]bool{}
		for id := range skip {
			next[id] = true
		}
		for id := range tried {
			next[id] = true
		}
		acct, _ = pickCookieAccountExcept(next) // 取不到时返回 nil，循环自然结束
	}
	if lastCookieErr != nil && cookieStr == "" {
		if mc.PinAccountID > 0 {
			return attrib(&pinnedAccountError{ID: mc.PinAccountID, Err: lastCookieErr})
		}
		if !rtCfg().FallbackAnon {
			// 默认报错而不是降级：cookie 失效后上游不会拒绝，只是把你当匿名用户，
			// 纯文本请求照样 200 —— 于是 3.1 Pro 被静默降级成 3.5 Flash-Lite、
			// 思考链消失，客户端完全看不出来。宁可明确失败也不给假的成功。
			return attrib(fmt.Errorf("cookie 池里 %d 个账号都不可用（最后一个：%w）；"+
				"到面板「Cookie 池」用「检测」按钮逐个排查，或打开 fallback_anon 降级匿名",
				len(tried), lastCookieErr))
		}
		logf("[cookie] 试过的 %d 个账号都不可用，本次降级匿名（能力会退化到匿名档）", len(tried))
		cookieID, cookieLabel = 0, ""
	}

	// endpoint 要等号和出口都定下来才能拼：
	//   - 路径带 /u/N 才落到第 N 个号，而上面的循环可能换了号（序号跟着变）
	//   - currentBL 可能顺手踢一次后台抓取，那个抓取必须跟正式请求走同一个出口，
	//     否则配了代理池也会从本机 IP 漏一次
	reqid := time.Now().Unix() % 1000000
	endpoint := fmt.Sprintf(
		"%s/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=%s&hl=en&_reqid=%d&rt=c",
		geminiBase(authUser), currentBL(proxyURL), reqid,
	)

	// 图片附件：上传要 cookie，而且必须走跟正式请求同一个出口，所以排在这里。
	if len(pending) > 0 {
		if cookieStr == "" {
			return attrib(fmt.Errorf("image input needs a Google account cookie: " +
				"anonymous uploads succeed but referencing them in a conversation is " +
				"rejected upstream. Add a cookie in the admin panel (Cookie pool)"))
		}
		nImg, nVid := 0, 0
		for _, u := range pending {
			ref, uerr := uploadBytesCtx(ctx, cookieStr, authUser, proxyURL, u.Data, u.Name)
			if err := ctx.Err(); err != nil {
				return attrib(err)
			}
			if uerr != nil {
				return attrib(fmt.Errorf("上传附件 %s 失败: %w", u.Name, uerr))
			}
			files = append(files, fileRef{Ref: ref, Name: u.Name, Kind: u.Kind, Mime: u.Mime})
			if u.Kind == 2 {
				nVid++
			} else {
				nImg++
			}
		}
		logf("[vision] 上传了 %d 张图 / %d 个视频", nImg, nVid)
	}

	// prompt 超长时转成文本附件。要等挑完号和出口才能做：上传要 cookie，
	// 而且必须走跟正式请求同一个出口。
	budget := rtCfg().MaxPromptBytes
	if p, f, used, ferr := prepareContextFile(ctx, prompt, pc, budget, cookieStr, authUser, proxyURL); ferr != nil {
		if err := ctx.Err(); err != nil {
			return attrib(err)
		}
		return attrib(ferr)
	} else if used {
		prompt = p
		files = append(files, f...)
	}
	if budget > 0 && len(prompt) > budget {
		return attrib(&PromptTooLongError{
			Bytes: len(prompt), Budget: budget, HasCookie: cookieStr != "",
		})
	}

	inner := make([]interface{}, innerSlots)
	if len(files) > 0 {
		// 形状逐字取自浏览器抓包：
		//   [[[路径, 类型, null, mime], "文件名", null×6, [0]], …]
		// 类型位是 1=图片 / 3=文本文件 —— 拿 1 传文本文件等于告诉服务端"这是张图"。
		refs := make([]interface{}, 0, len(files))
		for _, f := range files {
			kind := f.Kind
			if kind == 0 {
				kind = 3
			}
			mime := f.Mime
			if mime == "" {
				mime = "text/plain"
			}
			refs = append(refs, []interface{}{
				[]interface{}{f.Ref, kind, nil, mime}, f.Name,
				nil, nil, nil, nil, nil, nil,
				[]interface{}{0},
			})
		}
		inner[0] = []interface{}{prompt, 0, nil, refs, nil, nil, 0}
	} else {
		inner[0] = []interface{}{prompt, 0, nil, nil, nil, nil, 0}
	}
	inner[1] = []interface{}{"en"}
	inner[2] = []interface{}{"", "", "", nil, nil, nil, nil, nil, nil, ""}
	inner[6] = []interface{}{0}
	inner[7] = 1
	inner[10] = 1
	inner[11] = 0
	// 会话内轮次索引；我们每次都是新会话，恒为 0。
	inner[17] = []interface{}{[]interface{}{0}}
	if mc.Cont != nil {
		// Continue an existing conversation: [cid, rid, rcid, null×6, tok26] + turn index
		// (same shape as conversation.go's multi-turn path).
		inner[2] = []interface{}{mc.Cont.CID, mc.Cont.RID, mc.Cont.RCID, nil, nil, nil, nil, nil, nil, ""}
		inner[17] = []interface{}{[]interface{}{mc.Cont.Turn}}
	}
	inner[18] = 0
	inner[27] = 1
	inner[30] = []interface{}{4}
	// 抓包里浏览器三种场景（有 cookie / 无 cookie / 扩展思考）全是 [1]。
	// 我们原来写 [2]，是早期抄来的值、协议层已被证伪。含义仍未知，
	// 匿名两个值都能通，但没有理由继续偏离浏览器。
	inner[41] = []interface{}{1}
	// 临时对话（#34）：inner[45]=1 让这条不进账号的会话列表、不入训练。只在有登录
	// cookie 时发——匿名本就不落账号，发了也没意义且没验过。multi_turn 开着时
	// temporaryChatEligible 直接为假（两者冲突，见那里）。
	if temporaryChatEligible() && cookieStr != "" {
		inner[45] = 1
	}
	inner[53] = 0
	reqUUID := uuid.NewString()
	inner[59] = reqUUID
	inner[61] = []interface{}{}
	inner[68] = 1
	inner[79] = mc.Mode
	tw := mc.thinkWire()
	inner[80] = tw
	inner[91] = 0
	inner[96] = 0
	if tw != thinkingNormal {
		inner[96] = 1
	}
	// 任务模式槽位：仅 mc.TaskMode 触发——Mode==8（gemini-4-carbon-task）走
	// argonMode8Slots，Mode!=8 走 taskModeSlots；对话模型（含 gemini-4-carbon*）
	// 一律发纯普通 inner。先于 Tool 写入，TaskMode 与 Tool 互斥（任务模型不设 Tool），
	// 若两者同设显式 Tool 仍优先。
	if mc.TaskMode {
		if mc.Mode == 8 {
			argonMode8Slots(inner)
		} else {
			taskModeSlots(inner)
		}
	}
	// 媒体工具开关。填了服务端就换后端模型出图/出乐（响应里带产物引用，字节要另取）。
	if mc.Tool > 0 {
		inner[49] = mc.Tool
	}
	// 视频还要在 inner[55] 指定画幅比例：[[16]]=16:9，[[17]]=9:16（抓包）。
	if mc.Tool == toolVideo {
		aspect := 16
		if mc.VideoAspect == 17 {
			aspect = 17
		}
		inner[55] = []interface{}{[]interface{}{aspect}}
	}

	innerJSON, err := json.Marshal(inner)
	if err != nil {
		return nil, err
	}
	outer := []interface{}{nil, string(innerJSON)}
	outerJSON, err := json.Marshal(outer)
	if err != nil {
		return nil, err
	}

	// 带 cookie 时必须多发一个表单字段 at（XSRF token），否则上游直接 400。
	// 匿名请求不需要，getXSRF 对空 cookie 返回空串。见 xsrf.go。
	buildBody := func(at string) string {
		form := url.Values{}
		form.Set("f.req", string(outerJSON))
		if at != "" {
			form.Set("at", at)
		}
		return form.Encode()
	}

	body := buildBody(xsrfToken)

	// 抓包里 header 下标 16 和 inner[59] 是两个不同的 uuid，各生成各的。
	modelHeader := buildModelHeader(mc.HexID, mc.Mode, mc.thinkWire(), uuid.NewString())
	sessionHeader := fmt.Sprintf(`["%s",1]`, reqUUID)

	geminiHeaders := buildGeminiHeaders(cookieStr, sapisid, mc.HexID)
	setAuthUserHeaders(geminiHeaders, authUser)
	geminiHeaders["x-goog-ext-525001261-jspb"] = modelHeader
	geminiHeaders["x-goog-ext-525005358-jspb"] = sessionHeader
	var lastErr error
	// 最后一次拿到的 HTTP 状态码，0 表示网络层就失败了没拿到响应。
	// cookie 健康度只认 401/403，别的状态不算 cookie 的错，见 markCookieByStatus。
	lastStatus := 0
	xsrfRetried := false // XSRF 自愈只做一次，避免死循环
	t0 := time.Now()

	strictCheck := strictLineCheck(mc, func() int64 { return cookieID })
	lh := newLineHandler(onDelta, onReasoning, stopWhen, strictCheck)
	tracker, rtracker := lh.tracker, lh.rtracker

	retryWait := func() error {
		return sleepCtx(ctx, time.Duration(rtCfg().RetryDelaySec)*time.Second)
	}

	// 新协议（#43）尚未接入按沉默计时（#33）、请求取消传播、工具提前收尾与扩展思考，
	// 当前版本锁死走 StreamGenerate。
	// 打开 useWC 前必须处理的前置清单见 .agents/notes/webchannel-enable-prerequisites.md。
	noteWebChannelIgnored(rtCfg().WebChannel)
	useWC := false
	wcReq := wcRequest{
		Inner: wcInnerWithoutPrompt(inner), Prompt: prompt,
		HexID: mc.HexID, Mode: mc.Mode, Think: mc.thinkWire(),
		Cookie: cookieStr, SAPISID: sapisid, AuthUser: authUser,
	}

	// 幽灵会话根治（#phantom）：temporary_chat（inner[45]=1）在带附件（超长 prompt 转
	// message.txt，inner[0][3] 有文件引用）或 Mode=8 等场景下**不能**阻止服务端建会话
	// —— 实测服务端仍返回非空 c_xxx 并在侧栏留下「带未读蓝点、hNvQHb 报 1167 打不开」的
	// 索引残片。它们不在 MaZiqc 常规列表里（RPC 看不到、delete 却能删），是服务端索引
	// 与正文不一致的残留。
	//
	// 旧逻辑两处漏洞让它漏网：
	//   1) 只在 rtCfg().AutoDeleteConversation 为真时才删，而它默认 false、且
	//      temporaryChatEligible() 为真时整条被跳过 —— 幽灵恰恰在 temporary_chat 开着时产生；
	//   2) 只在 200 成功分支里删。客户端中途取消（499）、上游超时、出错话术等任何提前
	//      return 的路径，即便 raw 首帧已带回 c_xxx，也直接走了，残片留在账号里。
	//
	// 修法：cleanupPhantom 挂在 retry 循环**外**，每次 streamGenerateOnce 返回前统一执行。
	// 条件：(TemporaryChat||AutoDeleteConversation) && !MultiTurn && 有 cookie —— 只要本次
	// 收集到的任一 raw 帧里提取出 c_xxx，就异步 GzXR5e 删掉。成功帧、取消帧、错误帧都管。
	// 无害性：正常会话不会被误删 —— 该条件本身就意味着「这条会话不该留在账号里」，删掉它
	// 正是本意；multi_turn 开着时这个函数整体不生效（会话要留给续接）。
	// XSRF 值在 retry 循环里可能被自愈换掉，用闭包读取保证收尾用的总是当前最新的。
	phantomCleanup := phantomCleanupFunc(func() (string, string, string, int) {
		return cookieStr, sapisid, xsrfToken, authUser
	}, proxyURL)
	defer phantomCleanup.flush()

	for attempt := 0; attempt < rtCfg().RetryAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return attrib(err)
		}
		// 每轮重试先清掉上一轮的帧现场：本轮若在拿到响应前就挂了，诊断得如实说
		// 「这一轮没帧」，不能把上一轮的（可能是别的原因失败的）帧数留进来。
		lastDiag = upstreamDiagMeta{}
		var statusCode int
		var raw []byte
		var ttfb int64
		var setCookie []string
		var err error
		if useWC {
			wcReq.Cookie = cookieStr
			statusCode, raw, ttfb, setCookie, err = doWebChannelRequest(wcReq, proxyURL, lh.cb)
		} else {
			statusCode, raw, ttfb, setCookie, err = doGeminiRequestStopMode(ctx, endpoint, body, geminiHeaders, proxyURL, lh.cb, &lh.stop, mc.TaskMode)
		}
		if len(setCookie) > 0 && cookieID > 0 {
			if merged := mergeSetCookie(cookieStr, setCookie); merged != cookieStr {
				cookieStr = merged
				updateAccountCookie(cookieID, merged)
			}
		}
		// 每次拿到一段 raw 就喂给 phantomCleanup：流式/非流式/提前 EOF 都覆盖。
		// raw 可能为 nil（网络层失败），extractConversationID 对空串返回 ""，安全。
		if len(raw) > 0 {
			phantomCleanup.feed(string(raw))
			// 完整 raw 就地解算成结构化帧统计：帧数按一行多帧逐条目数，内容帧复用
			// textsInLine/reasoningInLine，自报名复用 extractUpstreamModel——同一帧
			// 不会因截断而判错，也不会把原文带进失败结果。
			lastDiag.Frames, lastDiag.ContentFrame, lastDiag.Model = upstreamFrameStats(string(raw))
		}
		var aie *argonIdentityError
		if strictCheck != nil && errors.As(err, &aie) && modelDowngraded(mc, aie.name) {
			err = &ModelDowngradedError{Requested: mc.Upstream, Reported: aie.name, AccountID: cookieID}
		}
		// 降级不是出口或 cookie 的错，也不该重试：同一个号再发一次还是降级。
		var mde *ModelDowngradedError
		if errors.As(err, &mde) {
			res, _ := attrib(nil)
			res.UpstreamModel = mde.Reported
			res.TotalMs = time.Since(t0).Milliseconds()
			// 带上已发出的部分：调用方据此判断还能不能换号重来
			res.Emitted, res.EmittedReasoning = tracker.emitted, rtracker.emitted
			return res, err
		}
		if err != nil {
			if cerr := ctx.Err(); cerr != nil {
				return attrib(cerr) // 客户端断开，不是出口的错
			}
			lastErr = err
			// 出错话术不记进代理健康度：匿名直连也一样出，跟出口无关，算进去会误熔断。
			// 浏览器自身错误（browser_unavailable）同理：那是本地浏览器层的问题，
			// 跟出口质量无关，也不该算 cookie 的错。
			if pickedOK && !errors.Is(err, errUpstreamFailureReply) && !isBrowserLocalError(err) {
				recordProxyResult(picked.ID, false, err.Error())
			}
			// 已经往客户端吐过内容就不能重试，否则会重复（思考链也算吐过）。
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if !retryAfterTimeout(err) {
				logf("上游超时，retry_on_timeout 关着，不再重试: %v", err)
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				logf("retry %d/%d: %v", attempt+1, rtCfg().RetryAttempts, err)
				if werr := retryWait(); werr != nil {
					return attrib(werr)
				}
			}
			continue
		}
		if statusCode != 200 {
			// token 过期时上游回 400 + xsrf。作废缓存重取一次再打，
			// 这种自愈不算进重试预算，否则一次过期就吃掉全部重试。
			if statusCode == 400 && isXSRFError(string(raw)) && cookieStr != "" && !xsrfRetried {
				xsrfRetried = true
				invalidateXSRF(cookieStr)
				// 浏览器模式下不要再先 reloadResidentPage 又 getXSRF——两者都
				// 会页内 fetch /app 一次，连续抓两遍纯粹浪费一次上游往返。
				// getXSRF 的浏览器路径本身就会重抓页面拿新 token。
				if tok, e := getXSRF(cookieStr, authUser, proxyURL); e == nil {
					xsrfToken = tok // 同步给收尾：phantomCleanup 按闭包取当前值
					body = buildBody(tok)
					geminiHeaders = buildGeminiHeaders(cookieStr, sapisid, mc.HexID)
					setAuthUserHeaders(geminiHeaders, authUser)
					geminiHeaders["x-goog-ext-525001261-jspb"] = modelHeader
					geminiHeaders["x-goog-ext-525005358-jspb"] = sessionHeader
					attempt--
					continue
				}
			}
			lastErr = fmt.Errorf("upstream HTTP %d: %s", statusCode, truncate(string(raw), 200))
			lastStatus = statusCode
			// 浏览器自身问题回的非 200（不该发生，但保险）也不扣出口分。
			if pickedOK && !isBrowserLocalError(lastErr) {
				recordProxyResult(picked.ID, false, lastErr.Error())
			}
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				if werr := retryWait(); werr != nil {
					return attrib(werr)
				}
			}
			continue
		}
		// HTTP 200 但一个内容帧都没有 —— 上游的瞬时拒绝（响应里那个 1155）。
		// 它不是限流：干净 IP 间隔 1s 连打 15 次全过、同 IP 并发 10 共 18 次全过、
		// 打了 60+ 次的 IP 之后照样成功，没有可预测阈值，同样的请求有时成功有时失败。
		// 重发一次通常就好，所以必须纳入重试——不然一次抖动就变成客户端可见的 502。
		//
		// 判据是**有没有内容帧**，不是 BardErrorInfo：正常响应的结束帧里也带错误码
		// （1096 = 会话未持久化），拿它判错会把每个正常响应都判成失败。
		// Video: Gemini now acks with a keyed-object frame ({"7":["I'm generating your
		// video…",["video_generation",…]]}) that has no inner[4] text, so hasContentFrame
		// says "empty" — but the job is already queued upstream. Retrying here generates
		// one extra video per attempt (burning daily quota). Once a conversation id
		// exists, treat it as accepted and let fetchMediaArtifacts poll hNvQHb.
		if !hasContentFrame(string(raw)) && !videoAccepted(mc.Tool, string(raw)) {
			lastErr = fmt.Errorf("upstream returned no content frame (raw %d bytes)", len(raw))
			if pickedOK {
				// 记进代理健康度：1155 跟出口质量强相关（干净出口 60+ 次 0 发生，
				// 脏出口一天约 9 次），连续踩中说明这个出口该歇了。
				recordProxyResult(picked.ID, false, lastErr.Error())
			}
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				logf("retry %d/%d: 空响应（无内容帧，%d 字节）", attempt+1, rtCfg().RetryAttempts, len(raw))
				if werr := retryWait(); werr != nil {
					return attrib(werr)
				}
			}
			continue
		}
		// 非流式没有 lineCB，在这里补判出错话术。重试跟空响应同理：重发通常就好。
		if hasFailureReply(string(raw)) {
			lastErr = errUpstreamFailureReply
			if tracker.emitted != "" || rtracker.emitted != "" {
				break
			}
			if attempt < rtCfg().RetryAttempts-1 {
				logf("retry %d/%d: %v", attempt+1, rtCfg().RetryAttempts, lastErr)
				if werr := retryWait(); werr != nil {
					return attrib(werr)
				}
			}
			continue
		}
		if pickedOK {
			recordProxyResult(picked.ID, true, "")
		}
		markCookieByStatus(cookieID, 200, "")
		result := &StreamResult{
			Emitted:          tracker.emitted,
			EmittedReasoning: rtracker.emitted,
			Raw:              string(raw),
			Reasoning:        extractReasoning(string(raw)),
			UpstreamModel:    extractUpstreamModel(string(raw)),
			ProxyID:          picked.ID,
			ProxyName:        picked.Name,
			AccountID:        cookieID,
			AccountLabel:     cookieLabel,
			TTFBMs:           ttfb,
			TotalMs:          time.Since(t0).Milliseconds(),
		}
		// 媒体模型：生成的产物字节不在这条响应里，要用同一套 cookie / 出口再走一遍
		// hNvQHb + 下载 host 取回。取不到就记 MediaErr，让上层报错而不是返回半成品。
		if mc.Tool == toolImage || mc.Tool == toolMusic || mc.Tool == toolVideo {
			mime := "image/png"
			switch mc.Tool {
			case toolMusic:
				mime = "audio/mpeg"
			case toolVideo:
				mime = "video/mp4"
			}
			cid, rid, _, _ := parseConvIDs(string(raw))
			if cid == "" {
				cid = extractConversationID(string(raw))
			}
			result.ConvCID, result.ConvRID = cid, rid
			arts, rcid, aerr := fetchMediaArtifacts(
				mc.Tool, string(raw), cid, rid,
				cookieStr, sapisid, xsrfToken, proxyURL, mime, authUser)
			result.ConvRCID = rcid
			if aerr != nil {
				logf("[media] 取回产物失败: %v", aerr)
				result.MediaErr = aerr
			} else {
				result.Artifacts = arts
				logf("[media] 取回 %d 份产物", len(arts))
			}
		}
		// #19 自动删会话 / #phantom 幽灵清理：媒体路径的 cid 已在 raw 到达时由
		// phantomCleanup.feed 收集；删会话统一由 defer phantomCleanup.flush() 收尾
		// （含 temporary_chat 下服务端仍建档的幽灵、以及客户端取消/超时时已落库的那条）。
		// 不再在这里直调 deleteConversation：同一 cid 会被 defer 删第二次，多发一次
		// GzXR5e RPC。条件放宽为 (AutoDeleteConversation||temporaryChatEligible) ——
		// 后者开着时幽灵照样产生，旧版「temporary 就不用删」的前提被实测推翻。
		return result, nil
	}
	if lastErr != nil {
		markCookieByStatus(cookieID, lastStatus, lastErr.Error())
	}
	return attrib(lastErr)
}

// lineHandler 是每读到一行上游数据时的处理：推思考链、推正文增量、判断能否提前收尾。
//
// tracker / rtracker 记的是**已经发给客户端的**正文 / 思考链，调用方据此决定还能
// 不能重试（吐过内容就不能重试，否则重复）。非流式也要逐帧看正文才能判断能否提前
// 收尾，所以没有 onDelta 时另用一份 watch 记，不污染 tracker。
type lineHandler struct {
	tracker, rtracker *deltaTracker
	stop              atomic.Bool
	cb                func(string) error // nil 表示不需要逐行处理
}

func newLineHandler(onDelta, onReasoning func(string), stopWhen func(string) bool, check func(string) error) *lineHandler {
	lh := &lineHandler{tracker: &deltaTracker{}, rtracker: &deltaTracker{}}
	if onDelta == nil && onReasoning == nil && stopWhen == nil && check == nil {
		return lh
	}
	watch := lh.tracker
	if onDelta == nil {
		watch = &deltaTracker{}
	}
	lh.cb = func(line string) error {
		if check != nil {
			if err := check(line); err != nil {
				return err
			}
		}
		// 出错话术在吐给客户端之前拦下，中断读取走重试。
		if failureReplyInLine(line) {
			return errUpstreamFailureReply
		}
		// 思考链先推完才轮到正文，所以先处理它，客户端拿到的顺序才对。
		if onReasoning != nil {
			if r := reasoningInLine(line); r != "" {
				if d := lh.rtracker.Push(r); d != "" {
					onReasoning(d)
				}
			}
		}
		if onDelta == nil && stopWhen == nil {
			return nil
		}
		grew := false
		for _, t := range renderedTextsInLine(line, false) {
			if d := watch.Push(t); d != "" {
				grew = true
				if onDelta != nil {
					onDelta(d)
				}
			}
		}
		if grew && stopWhen != nil && stopWhen(watch.emitted) {
			lh.stop.Store(true)
		}
		return nil
	}
	return lh
}

// webChannelIgnoredLogged 记下「web_channel 开着但被忽略」是否已经提示过；开关关掉时清零，
// 再打开会再提示一次。
var webChannelIgnoredLogged atomic.Bool

// noteWebChannelIgnored 在 web_channel 开着时提示它被忽略：开关每打开一次只提示一次，
// 免得每个请求都刷一行日志。返回这次是否提示了。
func noteWebChannelIgnored(on bool) bool {
	if !on {
		webChannelIgnoredLogged.Store(false)
		return false
	}
	if !webChannelIgnoredLogged.CompareAndSwap(false, true) {
		return false
	}
	logf("[gemini] web_channel 已在配置中开启，但当前版本固定走 StreamGenerate（保留按沉默计时、请求取消、工具提前收尾与扩展思考）")
	return true
}

// upstreamModelRe 匹配响应帧里服务端自报的模型显示名（帧的 [42] 位）。
// 形如 ...,"fbb127bbb056c959",null,null,"3.6 Flash",true,...
var upstreamModelRe = regexp.MustCompile(`\\"[0-9a-f]{16}\\",null,null,\\"([^"\\]{1,40})\\"`)

// extractUpstreamModel 取服务端实际使用的模型名，取不到返回空串。
func extractUpstreamModel(raw string) string {
	m := upstreamModelRe.FindAllStringSubmatch(raw, -1)
	if len(m) == 0 {
		return ""
	}
	return m[len(m)-1][1]
}

// buildModelHeader 拼 x-goog-ext-525001261-jspb，形状逐槽取自抓包：
//
//	[1,null,null,null,"<hex>",null,null,0,[4,5,6,8],null,null,1,null,null,<mode>,<think>,"<uuid>"]
//	下标                4                8                          14      15       16
//
// 下标 14 跟 inner[79] 同值、下标 15 跟 inner[80] 同值 —— 模型和思考模式在 header 和
// payload 里各存一份。**服务端认的是 header**：只填 inner[80]=2 而 header 留最小形式，
// 三个模型实测思考链全是 0 字符；这跟模型选择本身"header 压过 inner[79]"是同一个规律。
//
// 下标 16 是**另一个** uuid，跟 inner[59] 不是一个值 —— 跟 inner[59] 同值的是
// x-goog-ext-525005358-jspb。两份抓包都是这个规律，别图省事复用同一个。
//
// uuid 留空时退回最小形式（匿名路径不需要这些槽位，少发一截更省事）。
func buildModelHeader(hexID string, mode, think int, uuid string) string {
	if uuid == "" {
		return fmt.Sprintf(`[1,null,null,null,"%s"]`, hexID)
	}
	return fmt.Sprintf(
		`[1,null,null,null,"%s",null,null,0,[4,5,6,8],null,null,1,null,null,%d,%d,"%s"]`,
		hexID, mode, think, uuid)
}

// buildGeminiHeaders 准备 StreamGenerate 必需的应用层 header。
// hexID 决定服务端用哪个模型；留空则服务端一律回落到 3.5 Flash-Lite。
func buildGeminiHeaders(cookieStr, sapisid, hexID string) map[string]string {
	h := map[string]string{
		"Accept":          "*/*",
		"Accept-Language": "en-US,en;q=0.9",
		"Content-Type":    "application/x-www-form-urlencoded;charset=UTF-8",
		"Origin":          "https://gemini.google.com",
		"Referer":         "https://gemini.google.com/app",
		"X-Same-Domain":   "1",
		"X-Goog-AuthUser": "0",
		// 这两个浏览器每次都发，值是固定的。
		"x-goog-ext-73010989-jspb": "[0]",
		"x-goog-ext-73010990-jspb": "[0,0,0]",
	}
	if hexID != "" {
		h["x-goog-ext-525001261-jspb"] = buildModelHeader(hexID, 0, 0, "")
	}
	if cookieStr != "" {
		h["Cookie"] = cookieStr
	}
	if sapisid != "" {
		h["Authorization"] = makeSAPISIDHash(sapisid)
	}
	return h
}

// setAuthUserHeaders 把请求头对齐到第 authuser 个号：Referer 换成 /u/N/app，
// X-Goog-AuthUser 写 N。选号靠的是请求路径（见 geminiBase），这个头实测被忽略，
// 写成 N 只是不让它跟路径自相矛盾。authuser=0 时保持原样。
func setAuthUserHeaders(h map[string]string, authuser int) {
	if authuser <= 0 {
		return
	}
	h["Referer"] = geminiBase(authuser) + "/app"
	h["X-Goog-AuthUser"] = strconv.Itoa(authuser)
}

// doGeminiRequest 发一次请求到 endpoint。proxyURL 非空走 stdlib（支持 socks5/http），
// 空走 tls-client（chrome146 真指纹）。返回 (HTTP status, body bytes, err)。
// 返回值多了 setCookie：服务端几乎每个响应都在刷新 SIDCC / __Secure-1PSIDCC /
// __Secure-3PSIDCC，浏览器收下再带回去。一直发旧值的客户端会被判定为过期会话，
// 实测号活一两小时就失效 —— 所以这些必须收下来并写回账号。
func doGeminiRequest(endpoint, body string, headers map[string]string, proxyURL string,
	onLine func(string) error) (int, []byte, int64, []string, error) {
	return doGeminiRequestStop(context.Background(), endpoint, body, headers, proxyURL, onLine, nil)
}

// doGeminiRequestStopMode 同 doGeminiRequestStop，另带 taskMode：任务模式下
// hexNeon4 上游自报裸名 "4 Neon"、hexArgon4 上游自报被脱后缀成裸名 "4 Carbon"，
// 身份核验的合法名集合跟对话模式不同（见 argon.go）。
func doGeminiRequestStopMode(ctx context.Context, endpoint, body string, headers map[string]string, proxyURL string,
	onLine func(string) error, stop *atomic.Bool, taskMode bool) (int, []byte, int64, []string, error) {
	return doGeminiRequestStopInner(ctx, endpoint, body, headers, proxyURL, onLine, stop, taskMode)
}

// sleepCtx 等 d；ctx 先结束就提前返回它的错误。
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// stopReader 在 stop 置位后让下一次 Read 直接返回 EOF：调用方已经拿到想要的内容，
// 剩下的上游帧不再等。已经读进 Scanner 缓冲的行照常处理完。
type stopReader struct {
	r    io.Reader
	stop *atomic.Bool
}

func (s stopReader) Read(p []byte) (int, error) {
	if s.stop != nil && s.stop.Load() {
		return 0, io.EOF
	}
	return s.r.Read(p)
}

// stallLimit 是 StreamGenerate 上游最多能沉默多久：request_timeout_sec，0 = 不限
// （同客户端 Timeout 的 0）。测试里换短。
var stallLimit = func() time.Duration { return time.Duration(rtCfg().RequestTimeout) * time.Second }

// StallError 说上游沉默了 Limit 这么久，这次请求被 stallWatch 掐掉了。
type StallError struct{ Limit time.Duration }

func (e *StallError) Error() string {
	return fmt.Sprintf("上游 %s 没有任何数据（request_timeout_sec）", e.Limit)
}

func (e *StallError) Timeout() bool   { return true }
func (e *StallError) Temporary() bool { return true }

// stallWatch 盯着一次 StreamGenerate：上游 limit 这么久一个字节都没来（连上了却迟迟
// 不回、或者回复写到一半停住）就取消这次请求，没出字的照常重试。计的是沉默，不是整次
// 请求。
//
// 原来客户端的 Timeout 管整次请求，而上游会让请求排很久的队：同一个号问一句「你是谁」，
// 3.6 Flash 首字在 48–393 秒之间（3.5 Flash-Lite 4 秒）。排队时上游每 60 秒发一个
// 38 字节的心跳帧，出字后 1 秒内写完。整次限时 180 秒，排队超过 180 秒的请求一律被
// 掐掉重发，重发又从头排队，三次都掐完才报错；还在出字的长回复也被拦腰截断。按沉默
// 计时，心跳就让排队的请求一直等下去，只有连心跳都断了（limit 默认 180 秒，等于连着
// 丢三个）才算卡住。
type stallWatch struct {
	ctx    context.Context
	limit  time.Duration
	timer  *time.Timer
	cancel context.CancelCauseFunc
}

func newStallWatch(ctx context.Context, limit time.Duration) *stallWatch {
	ctx, cancel := context.WithCancelCause(ctx)
	w := &stallWatch{ctx: ctx, limit: limit, cancel: cancel}
	if limit > 0 {
		w.timer = time.AfterFunc(limit, func() { cancel(&StallError{Limit: limit}) })
	}
	return w
}

// heard 说上游又来了数据，沉默从现在重新算。
func (w *stallWatch) heard() {
	if w.timer != nil {
		w.timer.Reset(w.limit)
	}
}

// done 结束盯梢。
func (w *stallWatch) done() {
	if w.timer != nil {
		w.timer.Stop()
	}
	w.cancel(nil)
}

// explain 把被 stallWatch 掐掉的请求报的 context canceled 换成 StallError；
// 别的错误（包括客户端断开）原样返回。
func (w *stallWatch) explain(err error) error {
	var stall *StallError
	if err != nil && errors.As(context.Cause(w.ctx), &stall) {
		return stall
	}
	return err
}

// heardReader 每读到数据就告诉 stallWatch 一声。
type heardReader struct {
	r io.Reader
	w *stallWatch
}

func (h heardReader) Read(p []byte) (int, error) {
	n, err := h.r.Read(p)
	if n > 0 {
		h.w.heard()
	}
	return n, err
}

// doGeminiRequestStop 同 doGeminiRequest，stop 置位后提前结束读取（见 stopReader），
// ctx 结束时中止连接和读取。整次请求不限时，上游沉默超过 stallLimit 才放弃（见 stallWatch）。
// 签名保持原样（taskMode=false）：对话模式与既有探针/测试调用不变。
func doGeminiRequestStop(ctx context.Context, endpoint, body string, headers map[string]string, proxyURL string,
	onLine func(string) error, stop *atomic.Bool) (int, []byte, int64, []string, error) {
	return doGeminiRequestStopInner(ctx, endpoint, body, headers, proxyURL, onLine, stop, false)
}

func doGeminiRequestStopInner(ctx context.Context, endpoint, body string, headers map[string]string, proxyURL string,
	onLine func(string) error, stop *atomic.Bool, taskMode bool) (int, []byte, int64, []string, error) {
	sendAt := time.Now()
	watch := newStallWatch(ctx, stallLimit())
	defer watch.done()
	ctx = watch.ctx
	// 常驻真实浏览器传输层：开着时一律走页内 fetch——浏览器的意义就是指纹一致，
	// 有代理也照走（流量经 browserProxyBridge 的 CONNECT 隧道转给当前上游代理）。
	if shouldUseBrowserTransport() {
		// 全链路代理接线：本次 acquireSlot 选定的 proxyURL 同步给代理桥，
		// fallback_direct=false + 代理池时桥绝不直连（真实 IP 不出桥）。
		syncBridgeUpstream(proxyURL)
		acct := browserAccountFromHeaders(headers)
		// 匿名多轮的会话载体 cookie 不在账号池里（acct==nil 但 headers 带
		// Cookie）：这种「会话 cookie」绝不能经浏览器页内 fetch——JS fetch
		// 禁止手工设置 Cookie 头，且 credentials:"omit" 也防它误带驻留
		// 主号的登录态。非池会话载体跳过浏览器走纯 Go HTTP，仅登录态流量
		// （池账号与匿名无 Cookie）走常驻浏览器。
		skipBrowser := acct == nil && headers["Cookie"] != ""
		var status int
		var pr io.ReadCloser
		var err error
		if !skipBrowser {
			status, pr, err = browserMgr.doBrowserStreamRequest(ctx, acct, endpoint, "POST", headers, []byte(body), true, nil)
		}
		if skipBrowser {
			// 会话载体走纯 Go HTTP：落到下方 stdlib/tls-client 分支。
		} else if err != nil {
			// 浏览器不可用（没装/拉不起来/CDP 断）：默认绝不静默降级回纯 Go HTTP，
			// 由 upstreamReason 归到 browser_unavailable 给 502；显式开了
			// browser_fallback_http 才落回下面的纯 Go 路径。
			if isBrowserLocalError(err) && !rtCfg().BrowserFallbackHTTP {
				return 0, nil, 0, nil, err
			}
			if isBrowserLocalError(err) {
				logf("[browser] 不可用，本次回退纯 Go HTTP（browser_fallback_http 已开）: %v", err)
			} else {
				return 0, nil, 0, nil, watch.explain(err)
			}
		} else {
			defer pr.Close() // Close 会 unregister + Abort 页内 fetch
			raw, ttfb, err := readModelCheckedBodyMode(
				stopReader{heardReader{pr, watch}, stop}, onLine, sendAt, status,
				headers["x-goog-ext-525001261-jspb"], taskMode)
			if err != nil {
				// raw 原样回传：已到的首帧可能带 c_xxx，上层 phantomCleanup 要靠它。
				return status, raw, ttfb, nil, watch.explain(err)
			}
			return status, raw, ttfb, nil, nil
		}
	}
	if proxyURL != "" {
		// 走 stdlib 的 http.ProxyURL，已知能过 socks5/socks5h。
		req, err := http.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(body))
		if err != nil {
			return 0, nil, 0, nil, err
		}
		applyChromeHeaders(req)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		client := getStdlibStreamClient(proxyURL)
		resp, err := client.Do(req)
		if err != nil {
			return 0, nil, 0, nil, watch.explain(err)
		}
		defer resp.Body.Close()
		raw, ttfb, err := readModelCheckedBodyMode(stopReader{heardReader{resp.Body, watch}, stop}, onLine, sendAt, resp.StatusCode, headers["x-goog-ext-525001261-jspb"], taskMode)
		if err != nil {
			// raw 要原样回传：取消/超时/身份不符时已到达的首帧里可能带着 c_xxx，
			// 上层 phantomCleanup 要靠它删幽灵会话，置 nil 这条残片就漏网了。
			return resp.StatusCode, raw, ttfb, resp.Header.Values("Set-Cookie"), watch.explain(err)
		}
		return resp.StatusCode, raw, ttfb, resp.Header.Values("Set-Cookie"), nil
	}

	// 直连 → tls-client，保留 chrome146 TLS/HTTP2 真指纹
	req, err := fhttp.NewRequestWithContext(ctx, "POST", endpoint, strings.NewReader(body))
	if err != nil {
		return 0, nil, 0, nil, err
	}
	setChromeHeaders(req.Header)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	client := getTLSStreamClient()
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, 0, nil, watch.explain(err)
	}
	defer resp.Body.Close()
	raw, ttfb, err := readModelCheckedBodyMode(stopReader{heardReader{resp.Body, watch}, stop}, onLine, sendAt, resp.StatusCode, headers["x-goog-ext-525001261-jspb"], taskMode)
	if err != nil {
		// 同代理分支：raw 里已到的 c_xxx 要留给 phantomCleanup。
		return resp.StatusCode, raw, ttfb, resp.Header.Values("Set-Cookie"), watch.explain(err)
	}
	return resp.StatusCode, raw, ttfb, resp.Header.Values("Set-Cookie"), nil
}

// readBody 读完整个响应体并原样返回；onLine 非 nil 时每读到一行就回调一次，
// 让上层能在上游还没写完时就往客户端转发。onLine 返回错误就停止读取、原样返回
// 这个错误（strict_model 看到降级时用它掐断，不再白等上游把整段生成完）。
//
// 始终走逐行扫描（而不是 onLine==nil 时图省事用 io.ReadAll），因为要拿到
// **第一行到达的时刻**当 TTFB。用 ReadAll 的话读完才返回，测出来的"首字节
// 耗时"实际是完整耗时，跟总耗时永远一样。
//
// start 必须是**请求发出前**的时刻，由调用方传入。放在本函数里取 time.Now()
// 是不对的：那时 client.Do 已经返回、响应头甚至部分 body 都到了，测出来恒为 0。
func readBody(r io.Reader, onLine func(string) error, start time.Time) ([]byte, int64, error) {
	var buf bytes.Buffer
	sc := bufio.NewScanner(io.TeeReader(r, &buf))
	// 单帧可能很大（实测见过 40 万字节的响应），默认 64KB 上限不够。
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var ttfb int64 = -1
	for sc.Scan() {
		if ttfb < 0 {
			ttfb = time.Since(start).Milliseconds()
		}
		if onLine != nil {
			if err := onLine(sc.Text()); err != nil {
				return buf.Bytes(), ttfb, err
			}
		}
	}
	if ttfb < 0 {
		ttfb = time.Since(start).Milliseconds()
	}
	if err := sc.Err(); err != nil {
		return buf.Bytes(), ttfb, err
	}
	return buf.Bytes(), ttfb, nil
}

// reasoningInLine 从单个 wrb.fr 行里取出思考链。
//
// 位置是 inner[4][0][37][0][0]，比正文（inner[4][0][1][0]）深两层。
// 只有 3.1 Pro 会产出，3.6 Flash / Flash-Lite 恒为空。
//
// 时序（实测一次过河谜题）：思考链在头 5 帧里累积到 660 字符，此时正文还是空；
// 从第 6 帧起正文开始增长而思考链冻结不再变。两者都是**累积全文**不是增量，
// 所以流式侧可以直接复用 deltaTracker 的前缀 diff。
func reasoningInLine(line string) string {
	if !strings.Contains(line, `"wrb.fr"`) || len(line) < 200 {
		return ""
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(line), &arr); err != nil || len(arr) == 0 {
		return ""
	}
	first, ok := arr[0].([]interface{})
	if !ok || len(first) < 3 {
		return ""
	}
	innerStr, ok := first[2].(string)
	if !ok || len(innerStr) < 50 {
		return ""
	}
	var inner []interface{}
	if err := json.Unmarshal([]byte(innerStr), &inner); err != nil || len(inner) <= 4 {
		return ""
	}
	parts, ok := inner[4].([]interface{})
	if !ok || len(parts) == 0 {
		return ""
	}
	p0, ok := parts[0].([]interface{})
	if !ok || len(p0) <= 37 {
		return ""
	}
	lvl1, ok := p0[37].([]interface{})
	if !ok || len(lvl1) == 0 {
		return ""
	}
	lvl2, ok := lvl1[0].([]interface{})
	if !ok || len(lvl2) == 0 {
		return ""
	}
	s, _ := lvl2[0].(string)
	return s
}

// extractReasoning 取整个响应里最长的那段思考链。
// 取最长而不是最后一个：思考链冻结后，后续帧该位置可能缺失或被截短。
//
// 必须跟 extractResponseText 一样过 cleanGeminiText：流式侧 deltaTracker 推的是
// 清洗过的文本，这里不清洗的话两者前缀对不上，收尾补发时会把整段思考链重发一遍
// （实测块顺序变成 R→C→R）。
func extractReasoning(raw string) string {
	best := ""
	for _, line := range strings.Split(raw, "\n") {
		if r := reasoningInLine(line); len(r) > len(best) {
			best = r
		}
	}
	return cleanGeminiText(best)
}

// textsInLine 从单个 wrb.fr 行里取出候选回复文本。
// 上游每帧带的是**累积全文**而不是增量，所以流式转发时要自己做前缀 diff。
func textsInLine(line string) []string {
	if !strings.Contains(line, `"wrb.fr"`) || len(line) < 200 {
		return nil
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(line), &arr); err != nil || len(arr) == 0 {
		return nil
	}
	first, ok := arr[0].([]interface{})
	if !ok || len(first) < 3 {
		return nil
	}
	innerStr, ok := first[2].(string)
	if !ok || len(innerStr) < 50 {
		return nil
	}
	var inner []interface{}
	if err := json.Unmarshal([]byte(innerStr), &inner); err != nil || len(inner) <= 4 {
		return nil
	}
	parts, ok := inner[4].([]interface{})
	if !ok {
		return nil
	}
	var texts []string
	for _, p := range parts {
		pl, ok := p.([]interface{})
		if !ok || len(pl) < 2 {
			continue
		}
		tl, ok := pl[1].([]interface{})
		if !ok {
			continue
		}
		for _, t := range tl {
			if s, ok := t.(string); ok && s != "" {
				texts = append(texts, s)
			}
		}
	}
	return texts
}

// errUpstreamFailureReply 是上游「出错话术」回复：HTTP 200、有正文，但正文是
// "I seem to be encountering an error" / "Sorry, something went wrong" 这类固定句子，
// 不是模型真回答（#68）。
var errUpstreamFailureReply = errors.New("upstream returned a failure reply (inner[31] set)")

// errUpstreamTimeout 标记新协议（processSession）等满 request_timeout_sec 的超时；
// 老协议的超时由 http 客户端报出，靠 net.Error.Timeout() 认（见 isUpstreamTimeout）。
var errUpstreamTimeout = errors.New("upstream timeout")

// isUpstreamTimeout 判断这次失败是不是上游超时（等满 request_timeout_sec 没收完）。
// 两种 http 客户端（stdlib、tls-client 的 fhttp）超时都返回 Timeout()=true 的错误，
// 连接建立超时也算在内：同一次重试仍走同一个出口，重发多半照样超时。
func isUpstreamTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errUpstreamTimeout) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// retryAfterTimeout 返回超时后还要不要再发（retry_on_timeout，#70）。上游等满
// request_timeout_sec 都没回完时，重发通常还是超时，关掉可省下几轮等待、尽快报错。
func retryAfterTimeout(err error) bool {
	return rtCfg().RetryOnTimeout || !isUpstreamTimeout(err)
}

// failureReplyInLine 判断这一帧是不是上游的出错话术回复。
//
// 判据是 inner[31] 为真（老协议 true、新协议 JSPB 里是 1）。实测（2026-11-05，匿名 +
// Ultra 号、新老协议）6/6 条出错话术都带它，36 条正常回答和 3 条内容政策拒答（"I cannot
// …"）都不带。不按文案匹配：文案至少 4 种、随语言变。这一位在第一个内容帧就有，所以
// 流式也能在吐给客户端之前拦下。
func failureReplyInLine(line string) bool {
	if !strings.Contains(line, `"wrb.fr"`) || len(line) < 200 {
		return false
	}
	var arr []interface{}
	if err := json.Unmarshal([]byte(line), &arr); err != nil || len(arr) == 0 {
		return false
	}
	first, ok := arr[0].([]interface{})
	if !ok || len(first) < 3 {
		return false
	}
	innerStr, ok := first[2].(string)
	if !ok || len(innerStr) < 50 {
		return false
	}
	var inner []interface{}
	if err := json.Unmarshal([]byte(innerStr), &inner); err != nil || len(inner) <= 31 {
		return false
	}
	switch v := inner[31].(type) {
	case bool:
		return v
	case float64:
		return v != 0
	}
	return false
}

func hasFailureReply(raw string) bool {
	for _, line := range strings.Split(raw, "\n") {
		if failureReplyInLine(line) {
			return true
		}
	}
	return false
}

// videoAccepted reports whether a video StreamGenerate response shows the job was
// queued upstream (a conversation was created), even without a text content frame.
func videoAccepted(tool int, raw string) bool {
	return tool == toolVideo && extractConversationID(raw) != ""
}

// hasContentFrame 判断响应里到底有没有内容帧。
//
// 故意不复用 extractResponseText：那个会先 cleanGeminiText 掉代码产物，一个纯代码
// 产物的回复在它眼里是空的，但那明明是上游正常出了内容。这里只问"有没有帧"，
// 判断的是链路成没成功，不是内容合不合用。
func hasContentFrame(raw string) bool {
	for _, line := range strings.Split(raw, "\n") {
		if len(textsInLine(line)) > 0 || reasoningInLine(line) != "" {
			return true
		}
	}
	return false
}

// extractResponseText parses StreamGenerate's wrb.fr stream and returns the
// last non-empty text chunk (matches Python extract_response_text behavior).
func extractResponseText(raw string) string {
	var texts []string
	for _, line := range strings.Split(raw, "\n") {
		texts = append(texts, renderedTextsInLine(line, true)...)
	}
	for i := len(texts) - 1; i >= 0; i-- {
		if strings.TrimSpace(texts[i]) != "" {
			cleaned := cleanGeminiText(texts[i])
			if cleaned == "" {
				// 整条回复都是代码执行产物（问数学/算式时模型直接跑代码），清洗后被剥空。
				// 别返回空——那样 callGemini 会当"无内容帧"报 502，用户看到的是调用失败。
				// 回退：只把 ?code_reference/stdout 标记去掉、保留代码和结果，当普通代码块给。
				cleaned = strings.TrimSpace(codeMarkerRe.ReplaceAllString(texts[i], "```$1"))
			}
			return cleaned
		}
	}
	return ""
}

var codeArtifactRe = regexp.MustCompile("(?s)```(?:python|javascript|text)\\?code_(?:reference|stdout)&code_event_index=\\d+\\n.*?```\\n?")

// codeMarkerRe 只匹配代码产物的**开围栏标记**（```python?code_reference&code_event_index=N），
// 用于清洗后为空时的回退：把标记降级成普通 ```python，保留代码/结果不整条清空。
var codeMarkerRe = regexp.MustCompile("```(python|javascript|text)\\?code_(?:reference|stdout)&code_event_index=\\d+")

func cleanGeminiText(text string) string {
	return strings.TrimSpace(codeArtifactRe.ReplaceAllString(text, ""))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

// ProbeResult 是 admin 测试接口返回的连通性诊断结果。
type ProbeResult struct {
	OK           bool   `json:"ok"`
	Status       string `json:"status"` // "success" / "blocked_sorry" / "rate_limited" / "upstream_error" / "network_error"
	HTTPCode     int    `json:"http_code"`
	TotalMs      int64  `json:"total_ms"`
	ProxyID      int64  `json:"proxy_id"`
	ProxyName    string `json:"proxy_name"`
	UseDirect    bool   `json:"use_direct"`
	ResponseText string `json:"response_text"` // 截断到 200 字符
	UpstreamSnip string `json:"upstream_snip"` // 上游原始响应前 300 字符
	Diagnostic   string `json:"diagnostic"`    // 中文诊断说明
	Impersonate  string `json:"impersonate"`
}

// probeGemini 直接调 Gemini StreamGenerate（绕过限流），返回详细诊断。
// 不写 db、不消耗限流 slot。
func probeGemini(prompt, proxyURL string) ProbeResult {
	res := ProbeResult{Impersonate: rtCfg().Impersonate}

	inner := make([]interface{}, innerSlots)
	inner[0] = []interface{}{prompt, 0, nil, nil, nil, nil, 0}
	inner[1] = []interface{}{"en"}
	inner[2] = []interface{}{"", "", "", nil, nil, nil, nil, nil, nil, ""}
	inner[6] = []interface{}{0}
	inner[7] = 1
	inner[10] = 1
	inner[11] = 0
	inner[17] = []interface{}{[]interface{}{0}}
	inner[18] = 0
	inner[27] = 1
	inner[30] = []interface{}{4}
	// 抓包里浏览器三种场景（有 cookie / 无 cookie / 扩展思考）全是 [1]。
	// 我们原来写 [2]，是早期抄来的值、协议层已被证伪。含义仍未知，
	// 匿名两个值都能通，但没有理由继续偏离浏览器。
	inner[41] = []interface{}{1}
	inner[53] = 0
	inner[59] = uuid.NewString()
	inner[61] = []interface{}{}
	inner[68] = 1
	probeModel := Models["gemini-3.6-flash"]
	inner[79] = probeModel.Mode

	innerJSON, _ := json.Marshal(inner)
	outer := []interface{}{nil, string(innerJSON)}
	outerJSON, _ := json.Marshal(outer)
	form := url.Values{}
	form.Set("f.req", string(outerJSON))
	body := form.Encode()

	reqid := time.Now().Unix() % 1000000
	endpoint := fmt.Sprintf(
		"https://gemini.google.com/_/BardChatUi/data/assistant.lamda.BardFrontendService/StreamGenerate?bl=%s&hl=en&_reqid=%d&rt=c",
		currentBL(proxyURL), reqid,
	)

	// probe 是旁路探测，不回写 cookie 健康度：它的失败原因跟 cookie 无关。
	// 但 at 必须带——否则挂了 cookie 之后连通性探测会一直报 400，假报故障。
	cookieStr, sapisid := loadCookie()
	if tok, e := getXSRF(cookieStr, 0, proxyURL); e == nil && tok != "" {
		form.Set("at", tok)
		body = form.Encode()
	}
	headers := buildGeminiHeaders(cookieStr, sapisid, probeModel.HexID)

	// 复用主流程同款 client 选择规则:有代理走 stdlib，没代理走 tls-client。
	// 但 probe 需要看 302 的 Location header,所以这里直接发不用 doGeminiRequest。
	var statusCode int
	var raw []byte
	var locHeader string
	var err error

	acct := browserAccountFromHeaders(headers)
	// 非池会话载体（acct==nil 但带 Cookie）不借浏览器 jar：页内 fetch 发不出
	// 手工 Cookie 头也不该误带驻留主号登录态，probe 旁路探测走纯 Go HTTP。
	skipBrowserProbe := acct == nil && headers["Cookie"] != ""
	if shouldUseBrowserTransport() && !skipBrowserProbe {
		// 浏览器传输模式：页内 fetch 探测，跟正式请求同一条链路。
		syncBridgeUpstream(proxyURL)
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		status, b, e := browserMgr.doBrowserPost(ctx, acct, endpoint, headers, []byte(body), false, nil)
		cancel()
		if e != nil {
			if isBrowserLocalError(e) {
				res.Status = "network_error"
				res.HTTPCode = 502
				res.Diagnostic = "常驻浏览器不可用: " + e.Error() +
					"。请安装 Chromium 或配置 browser_path；browser_fallback_http 开时才回退纯 Go HTTP"
				return res
			}
			res.Status = "network_error"
			res.Diagnostic = "网络层错误: " + e.Error()
			return res
		}
		statusCode = status
		raw = b
	} else if proxyURL != "" {
		req, e := http.NewRequest("POST", endpoint, strings.NewReader(body))
		if e != nil {
			res.Status = "network_error"
			res.Diagnostic = "构建请求失败: " + e.Error()
			return res
		}
		applyChromeHeaders(req)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		client := getStdlibClient(proxyURL)
		resp, e := client.Do(req)
		if e != nil {
			res.Status = "network_error"
			res.Diagnostic = "网络层错误（DNS/TCP/TLS 失败）: " + e.Error()
			return res
		}
		defer resp.Body.Close()
		statusCode = resp.StatusCode
		locHeader = resp.Header.Get("Location")
		raw, err = io.ReadAll(resp.Body)
	} else {
		req, e := fhttp.NewRequest("POST", endpoint, strings.NewReader(body))
		if e != nil {
			res.Status = "network_error"
			res.Diagnostic = "构建请求失败: " + e.Error()
			return res
		}
		setChromeHeaders(req.Header)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		client := getTLSClient()
		resp, e := client.Do(req)
		if e != nil {
			res.Status = "network_error"
			res.Diagnostic = "网络层错误（DNS/TCP/TLS 失败）: " + e.Error()
			return res
		}
		defer resp.Body.Close()
		statusCode = resp.StatusCode
		locHeader = resp.Header.Get("Location")
		raw, err = io.ReadAll(resp.Body)
	}
	_ = err
	res.HTTPCode = statusCode
	res.UpstreamSnip = truncate(string(raw), 300)

	switch {
	case statusCode == 302:
		res.Status = "blocked_sorry"
		res.Diagnostic = "IP 被 Google 风控（重定向到 sorry/index）。" +
			"通常 6-24 小时解除，或换 VPN/代理 IP 立即恢复。Location: " + truncate(locHeader, 200)
		return res
	case statusCode == 429:
		res.Status = "rate_limited"
		res.Diagnostic = "Google 直接返回 429 限流。同样是 IP 嫌疑，但风控分支不同（朴素 SDK 路径）。"
		return res
	case statusCode != 200:
		res.Status = "upstream_error"
		res.Diagnostic = fmt.Sprintf("上游返回非 200 (HTTP %d)，可能是协议变更或临时故障。", statusCode)
		return res
	}

	text := extractResponseText(string(raw))
	if text == "" {
		res.Status = "upstream_error"
		res.Diagnostic = "上游 200 但只回了结束帧、没有内容帧，通常是请求被服务端拒绝" +
			"（例如带了不被接受的会话 id 或工具开关），不是帧格式变更。"
		return res
	}

	res.OK = true
	res.Status = "success"
	res.ResponseText = truncate(text, 200)
	res.Diagnostic = "调用成功。延迟 / 内容见上面字段。"
	return res
}
