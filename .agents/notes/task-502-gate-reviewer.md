# task-502 门禁原件：reviewer 初轮报告（逐字）

来源：t146 reviewer 原始 hc 返回（主代理逐字转存），适用候选 c57860d / HEAD d416789。

备注：该轮唯一 must fix 为「缺当前快照独立证据」，现已由 verifier 补证覆盖（见 `.agents/notes/task-502-evidence/task-502-gate-verifier-supplement.md`）；最终裁决由协调方做出。

---

结论：未回退既有修复，无新增代码阻断问题；C档证据不足，不能收口，需补证、不需代码返工。
关键验证：17文件已核17/17。d416789仅改RESULT，源码仍为c57860d。独立round2是a8d1bc1+旧补丁，非当前候选。
既有修复（Go路径均为internal/app/）：server.go:198-205保留PR#72守卫，仅细分empty_content；canvas/媒体/合法调用/代码回退未破坏。webchannel.go与8647ba1的blob相同：205保留SID/gsessionid脱敏；247/272保留wcRedactErr/wcSanitizeErr；282-308未改动原样返回；325-339保留wcRedactedError深链Timeout/Temporary；570/595/621/678握手缺会话及非200仍不回body。
旧→新必要性：gemini_api.go:283/551/654/700改共享诊断，必要。sse.go:131补字段，必要。admin.go:670补前缀分类，必要。conversation.go:380/415/563补现场与reason，必要，ctx/重试/slot释放/会话作废时序不变。videos.go:156/162/278/413/474补前缀与快照，为授权race修复，必要。两workflow（.github/workflows/）：release.yml:25/47改构建注入，必要；docker.yml:42/57传版本，必要。Dockerfile:20/25承接注入，必要。README.md:109/164、README_EN.md:115/181分别补诊断/版本，必要。CHANGELOG.md:9-21记录行为，必要。其余无非必要项。
调用分母：writeUpstreamError 2/2、geminiErrorJSON 2/2、recordUpstreamFailure 6/6、diagErr构造6/6已核；4生成入口开流分支及video两失败落库分支无遗漏。MCP文本与video job.Error不改。upstream_err.go:187-217共享message及5诊断字段；Gemini走details，无缺字段。
最小纪律：toolgate_test.go:323-887新增567行、6顶层含3合法子例，非同构堆砌；未超生产新增386行的3倍，沿用旧文件，断言命中状态/字段/落库/空媒体。
致命（must fix）｜.agents/notes/task-502-verifier-round2.txt:12-16｜验证非当前快照｜补8647ba1→c57860d红绿对照、当前门禁与-race至少3轮证据｜未具备放行条件｜不要修改项：脱敏/重试/MCP锁。
