# WebChannel（useWC）前置清单与历史边界导航

- **启用 `useWC` 前必须完成的未闭环前置项与队列排空债务**：已统一归口至同目录 [inbox.md](inbox.md) 的 `[pattern:webchannel-enable-prerequisites]` 与 `[pattern:webchannel-queue-drop]`。
- **历史边界说明（供 `CHANGELOG.md` 4.28.0-magpie.3 O-2 引用核查）**：`wcSanitizeErr` 重建 `*url.Error` 时仅递归脱敏其直接 `Err` 字段；标准库 `*url.Error.Timeout()` 本身只检查直接一层 `Err`，在 `url.Error -> fmt -> net.Error` 深层包装链下 `Timeout()` 仍按既有标准库语义丢失，属已知非阻断边界（非启用前置项，未改动）。
