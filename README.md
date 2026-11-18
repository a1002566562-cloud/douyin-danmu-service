# danmu-service · 直播弹幕采集服务（抖音 / TikTok）

一条命令抓取直播间的**全部互动信息**：弹幕、礼物（名称+数量）、点赞、进场、关注、在线人数，每条消息带**用户真实头像 URL** 和主色。启动后通过 WebSocket / SSE / REST 对外输出标准化 JSON，任何语言任何项目都能接。

**国内抖音、国外 TikTok 同一套命令**，启动时自动按输入识别平台。

> 核心方案是 2026-11 实测跑通的「浏览器旁路抓帧」：用 Playwright 打开真实直播间页面，旁路监听其 `webcast/im` WebSocket 二进制帧，自行解析 protobuf。签名、设备指纹、心跳全部由真实浏览器完成——**不逆向、不注入、不怕 `DEVICE_BLOCKED` 风控**。
>
> TikTok 与抖音属于同一套 webcast 协议族（帧结构、消息 method 名、User/Image 字段都一致），所以两侧共用同一套解析器，只换入口地址与页面判定。

## 平台支持

| 平台 | 输入方式 | 示例 |
|---|---|---|
| 国内抖音 | 直播间号（纯数字） | `79423499229` |
| 国内抖音 | 完整链接 | `https://live.douyin.com/79423499229` |
| 国外 TikTok | `@用户名` | `@user70367980266313` |
| 国外 TikTok | 完整链接 | `https://www.tiktok.com/@user/live` |

## 事件格式

```jsonc
{"type": "chat",   "platform": "douyin", "user": "昵称", "avatar": "https://p3-...jpeg",
 "avcolor": "#A3967C", "content": "弹幕文字", "ts": 1730000000}
{"type": "gift",   "platform": "tiktok", "user": "昵称", "avatar": "...", "avcolor": "...",
 "gift": "Rose", "count": 1, "ts": ...}
{"type": "like",   "user": "昵称", "avatar": "...", "avcolor": "...", "count": 12, "ts": ...}
{"type": "enter",  "user": "昵称", "avatar": "...", "avcolor": "...", "ts": ...}
{"type": "follow", "user": "昵称", "avatar": "...", "avcolor": "...", "ts": ...}
{"type": "stats",  "online": 635, "ts": ...}          // 10 秒最多一条
{"type": "room",   "name": "主播昵称", "rid": "79423499229", "platform": "douyin", "ts": ...}
{"type": "live",   "on": true, "ts": ...}             // 开播/下播状态
```

`avatar` 为空时用 `avcolor` + 昵称首字画色块。抖音的 `avcolor` 来自协议里的 `Image.avgColor`；TikTok 不提供该字段，改用头像 URL 哈希取稳定色（同一个人永远同色）。

## 快速开始

```bash
pip install -r requirements.txt

# 国内抖音
python -m server.service 79423499229

# 国外 TikTok（@用户名 或整条直播间链接都行）
python -m server.service @user70367980266313

# 顺手把事件喂给星耀数字人工作台
python -m server.service @user70367980266313 --forward http://127.0.0.1:8010/human

# 强制指定平台（输入看不出平台时用）
python -m server.service user70367980266313 --platform tiktok
```

启动后终端实时打印，同时开放：

| 通道 | 地址 | 说明 |
|---|---|---|
| WebSocket | `ws://127.0.0.1:8521/ws` | 每条事件一个 JSON 文本帧；重连自动补发最近 50 条 |
| SSE | `http://127.0.0.1:8520/stream` | 浏览器 `EventSource` 直接用；重连补发 30 条 |
| REST | `http://127.0.0.1:8520/events?limit=50` | 拉最近 N 条 |
| 状态 | `http://127.0.0.1:8520/status` | 平台/房间/主播/帧计数/转发队列 |
| 切房 | `POST /room` body `{"room":"..."}` | 运行时切换，抖音号与 TikTok 链接都认 |
| 落盘 | `data/<房间>.jsonl` | 逐条 JSONL，`--no-log` 关闭 |

端口参数：`--http-port 8520 --ws-port 8521 --host 0.0.0.0`（对外暴露时建议改 host）。

## 转发到数字人工作台

```bash
python -m server.service 79423499229 --forward http://127.0.0.1:8010/human
```

内部事件会被映射成工作台 `/human` 认的格式（`chat` → `nickname`+`content`，`gift` → `giftName`+`giftCount`），由后台队列异步发送，不阻塞抓取。工作台侧原有的优先级排队、6.5 秒话轮冷却继续生效。

## 接入示例

```python
# Python: examples/client_ws.py
import asyncio, json, websockets
async def main():
    async with websockets.connect("ws://127.0.0.1:8521/ws") as ws:
        async for raw in ws:
            ev = json.loads(raw)
            if ev["type"] == "chat":
                print(f"[{ev.get('platform')}] {ev['user']}: {ev['content']}")
asyncio.run(main())
```

```js
// 浏览器: examples/client_sse.js
const es = new EventSource("http://127.0.0.1:8520/stream");
es.onmessage = (m) => console.log(JSON.parse(m.data));
```

## 单独复用抓取器（不跑服务）

```bash
# 抖音
python -m server.douyin_fetcher 79423499229          # 终端直印弹幕
python -m server.douyin_fetcher 79423499229 --head   # 弹出浏览器看页面
python -m server.douyin_fetcher 79423499229 --raw    # 协议直连（备用，可能被拒）

# TikTok
python -m server.tiktok_fetcher @user70367980266313
python -m server.tiktok_fetcher @user70367980266313 --head
```

在自己的代码里：

```python
from server.douyin_fetcher import BrowserSniper
from server.tiktok_fetcher import TikTokSniper

sniper = BrowserSniper("79423499229", on_event=lambda ev: print(ev))
# sniper = TikTokSniper("@user70367980266313", on_event=lambda ev: print(ev))
sniper.start()
```

## 登录（私密直播间 / TikTok 必需）

抓取端用独立无痕上下文，不共享你浏览器里的登录态。需要登录时：

```bash
python -m server.douyin_fetcher --login    # 抖音：扫码登录
python -m server.tiktok_fetcher --login    # TikTok：登录（游客常常收不到弹幕）
# 生成的 login_state.json 放项目根目录，重启服务自动加载
```

TikTok 尤其建议先登录：实测游客状态下直播间可能只推少量消息，登录后弹幕/礼物/点赞才会完整。

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

抖音与 TikTok 的字段号在上述范围内一致，两侧共用 `pb_wire` + `to_event`。差异只有三处，都已在 `tiktok_fetcher.py` 里处理：

1. 入口地址与页面判定文案（`LIVE has ended` vs `直播已结束`）
2. 礼物名：TikTok 的 `GiftStruct` 没有独立 name，`describe` 是 `Sent Rose` 形式，需要剥前缀
3. 头像主色：TikTok 的 `Image` 不带 `avgColor`，改用 URL 哈希稳定色

字段号再漂移时，设 `DANMU_DUMP=1` 会把每种消息的解析树打印一行，对照改常量即可。

## 调试工具

```bash
python tools/probe_tiktok_live.py           # 列出当前在播的 TikTok 房间
python tools/probe_douyin_live.py           # 列出当前在播的抖音房间
DANMU_DUMP=1 python -m server.tiktok_fetcher @xxx   # 打印每种消息的字段结构
curl http://127.0.0.1:8520/status           # 帧数 / 事件数 / 转发队列
```

## 设计要点 / 踩坑记录

- **协议直连已被设备风控拦截**（`415 DEVICE_BLOCKED`），即使 ttwid/md5 签名正确。所以默认走浏览器旁路，直连仅作备用（`--raw`）。
- 消息泵与解析分离：WSS 帧先入队（上限 800，满了丢最旧换实时性），另起线程解 protobuf，弹幕不滞后。
- 无效房间号会被抖音回落到「抖音直播电脑版」首页，靠页面标题识别并直接停止（重连无用）。
- 头像 URL 判定：长 ASCII 字符串可能被误判成嵌套 protobuf，`pb_wire._looks_like_message` 对「能解出 http 链接」的情况强制按 message 处理。
- 在线人数是高频推送（实测 27 帧里 16 条 `WebcastRoomUserSeqMessage`），服务层做了 10 秒节流，否则缓冲区会被 stats 刷满、真弹幕反被挤掉。
- TikTok 同一条消息会在相邻帧里重发，同人同内容 4 秒内只保留一条。
- 60 秒无帧看门狗自动重连；下播转 HTTP 轮询等开播（每 60 秒），开播自动重进。

## 已知限制

- 旁路依赖本机装有 Chrome 或 Edge（`DANMU_BROWSER_PATH` 环境变量 / 项目根 `browser.txt` 可显式指定）；headless 约占 200MB 内存。
- 抖音若改加密帧或字段号，需对照 `DANMU_DUMP=1` 输出微调字段号。
- TikTok 网页端在中国大陆通常需要代理才能访问；浏览器不走代理时会连不上直播间。
- TikTok 对游客限制较多，正式使用建议先 `--login`。
- 匿名（未实名）观众昵称被抖音脱敏（如 `梦***`），头像也只有默认灰图。

## License

MIT
