# douyin-danmu-service · 抖音直播弹幕采集服务

一条命令抓取抖音直播间的**全部互动信息**：弹幕、礼物（名称+数量）、点赞、进场、关注、在线人数，每条消息带**用户真实头像 URL** 和主色。启动后通过 WebSocket / SSE / REST 对外输出标准化 JSON，任何语言任何项目都能接。

> 核心方案来自 2026-11 实测跑通的「浏览器旁路抓帧」：用 Playwright 打开真实直播间页面，旁路监听其 `webcast/im/push/v2` WebSocket 二进制帧，自解 protobuf。签名、设备指纹、心跳全部由真实浏览器完成——**不逆向、不注入、不怕 `DEVICE_BLOCKED` 风控**。

## 事件格式

```jsonc
{"type": "chat",   "user": "昵称", "avatar": "https://p3-...jpeg", "avcolor": "#A3967C",
 "content": "弹幕文字", "ts": 1730000000}
{"type": "gift",   "user": "昵称", "avatar": "...", "avcolor": "...",
 "gift": "小心心", "count": 1, "ts": ...}
{"type": "like",   "user": "昵称", "avatar": "...", "avcolor": "...", "count": 12, "ts": ...}
{"type": "enter",  "user": "昵称", "avatar": "...", "avcolor": "...", "ts": ...}
{"type": "follow", "user": "昵称", "avatar": "...", "avcolor": "...", "ts": ...}
{"type": "stats",  "online": 635, "ts": ...}
{"type": "room",   "name": "主播昵称", "rid": "79423499229", "ts": ...}
{"type": "live",   "on": true, "ts": ...}          // 开播/下播状态
```

`avatar` 为空时（匿名号/默认灰图），用 `avcolor` + 昵称首字画色块即可，比一片灰好看。

## 快速开始

```bash
pip install -r requirements.txt
python -m server.service 79423499229        # 直播间号 = 分享链接里 live.douyin.com/ 后那串数字
```

启动后终端实时打印弹幕，同时开放：

| 通道 | 地址 | 说明 |
|---|---|---|
| WebSocket | `ws://127.0.0.1:8521/ws` | 每条事件一个 JSON 文本帧；重连自动补发最近 50 条 |
| SSE | `http://127.0.0.1:8520/stream` | 浏览器 `EventSource` 直接用；重连补发 30 条 |
| REST | `http://127.0.0.1:8520/events?limit=50` | 拉最近 N 条 |
| 状态 | `http://127.0.0.1:8520/status` | 房间/主播/帧计数/在线客户端 |
| 切房 | `POST /room` body `{"rid":"123456"}` | 运行时切换直播间，不用重启 |
| 落盘 | `data/<rid>.jsonl` | 逐条 JSONL，`--no-log` 关闭 |

端口参数：`--http-port 8520 --ws-port 8521 --host 0.0.0.0`（对外暴露时建议改 host）。

## 接入示例

```python
# Python: examples/client_ws.py
import asyncio, json, websockets
async def main():
    async with websockets.connect("ws://127.0.0.1:8521/ws") as ws:
        async for raw in ws:
            ev = json.loads(raw)
            if ev["type"] == "chat":
                print(ev["user"], ev["content"], ev["avatar"])
asyncio.run(main())
```

```js
// 浏览器: examples/client_sse.js
const es = new EventSource("http://127.0.0.1:8520/stream");
es.onmessage = (m) => console.log(JSON.parse(m.data));
```

## 私密 / 仅粉丝可见直播间

抓取端用的是独立无痕上下文，不共享你浏览器的登录态。需要登录时：

```bash
python -m server.douyin_fetcher --login    # 弹出可见浏览器，人工扫码，回车保存
# 生成的 login_state.json 放项目根目录，重启服务自动加载
```

## 单独复用抓取器（不跑服务）

```bash
python -m server.douyin_fetcher 79423499229          # 终端直印弹幕（headless）
python -m server.douyin_fetcher 79423499229 --head   # 弹出浏览器看页面
python -m server.douyin_fetcher 79423499229 --raw    # 协议直连（备用，可能被 DEVICE_BLOCKED 拒）
```

在自己的代码里：

```python
from server.douyin_fetcher import BrowserSniper
sniper = BrowserSniper("79423499229", on_event=lambda ev: print(ev))
sniper.start()
```

## 帧协议（2026-11 实测）

```
PushFrame { 1:seqId, 2:logId, 3:service, 4:method, 5:headers, 6:"pb", 7:"msg", 8:gzip(payload) }
  payload 解压 → WebcastResponse { 1: repeated WebcastMessage }
    WebcastMessage { 1: method(str), 2: payload }
      ChatMessage    { 2: user, 3: content }
      MemberMessage  { 2: user }
      SocialMessage  { 2: user }
      LikeMessage    { 2: count, 5: user }
      GiftMessage    { 5: count, 7: user, 15: gift{2: giftName} }
      User           { 1: id, 3: nickName, 9/10/11: avatarThumb/Medium/Large
                       Image { 1: repeated urlList, 2: uri, 5: avgColor } }
```

`server/pb_wire.py` 是零依赖 wire-format 解析器，字段号漂移时改 `douyin_fetcher.py` 顶部的常量即可。

## 设计要点 / 踩坑记录

- **协议直连已被设备风控拦截**（`415 DEVICE_BLOCKED`），即使 ttwid/md5 签名正确。所以默认走浏览器旁路，直连仅作备用（`--raw`）。
- 消息泵与解析分离：WSS 帧先入队（上限 800，满了丢最旧换实时性），另起线程解 protobuf，弹幕不滞后。
- 无效房间号会被抖音回落到「抖音直播电脑版」首页，靠页面标题识别并直接停止（重连无用）。
- 头像 URL 判定：长 ASCII 字符串可能被误判成嵌套 protobuf，`pb_wire._looks_like_message` 对「能解出 http 链接」的情况强制按 message 处理。
- 60 秒无帧看门狗自动重连；下播转 HTTP 轮询等开播（每 60 秒），开播自动重进。

## 已知限制

- 旁路依赖本机装有 Chrome 或 Edge（`DANMU_BROWSER_PATH` 环境变量 / 项目根 `browser.txt` 可显式指定）；headless 约占 200MB 内存。
- 抖音若改加密帧或字段号，需对照 `--debug` 输出微调字段号。
- 匿名（未实名）观众昵称被抖音脱敏（如 `梦***`），头像也只有默认灰图。

## License

MIT
