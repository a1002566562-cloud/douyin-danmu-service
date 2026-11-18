"""直播弹幕采集服务（国内抖音 / 国外 TikTok，独立通用版）。

一条命令启动后，向任何本机 / 局域网程序输出标准化互动事件：
    chat(弹幕) / enter(进场) / follow(关注) / like(点赞) /
    gift(礼物，含名称与数量) / stats(在线人数) / room(主播名) / live(开播状态)
每条带 user 的事件都含 avatar(真实头像 URL) 与 avcolor(主色，头像缺失时前端画色块用)。
事件里带 platform 字段（douyin / tiktok），转发或落盘时可直接区分来源。

输出通道（可同时使用）：
    WebSocket   ws://127.0.0.1:8521/ws           每条事件一个 JSON 文本帧
    SSE         http://127.0.0.1:8520/stream     text/event-stream
    REST        http://127.0.0.1:8520/events?limit=50   最近 N 条（断线补发用）
                http://127.0.0.1:8520/status     平台/房间/帧/事件计数
                POST /room {"room":"..."}        运行时切换直播间（抖音号或 TikTok 链接均可）
    JSONL       data/<房间>.jsonl                 逐条落盘，方便回放/分析
    转发         --forward http://127.0.0.1:8010/human   直接喂给星耀数字人工作台

启动：
    python -m server.service 79423499229
    python -m server.service https://www.tiktok.com/@user/live
    python -m server.service @user70367980266313 --forward http://127.0.0.1:8010/human
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import queue
import re
import sys
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.douyin_fetcher import BrowserSniper  # noqa: E402
from server.tiktok_fetcher import TikTokSniper, normalize_handle  # noqa: E402

HOST = "127.0.0.1"
HTTP_PORT = 8520
WS_PORT = 8521
BUFFER_MAX = 500

_app_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_data_dir = os.path.join(_app_root, "data")

START_TS = time.time()

# 哪些事件要转发给下游（工作台只认 chat/gift/follow，其余忽略）
FORWARD_TYPES = {"chat", "gift", "follow", "enter", "like"}


# ---------------- 平台识别 ----------------

def detect_platform(text: str) -> tuple[str, str]:
    """把用户输入解析成 (platform, target)。

    纯数字 → 抖音直播间号；tiktok.com 链接或 @handle → TikTok；其余按抖音处理。
    """
    t = (text or "").strip().strip('"').strip("'")
    if not t:
        return "", ""
    low = t.lower()
    if "tiktok.com" in low or t.startswith("@"):
        return "tiktok", normalize_handle(t)
    if "douyin.com" in low:
        m = re.search(r"live\.douyin\.com/(\d+)", t)
        return ("douyin", m.group(1)) if m else ("", "")
    # 纯数字 = 抖音直播间号
    if re.fullmatch(r"\d{5,}", t):
        return "douyin", t
    # 字母开头的 handle 形态 = TikTok 用户名（不带 @ 也要认，别落进抖音分支）
    if re.fullmatch(r"[A-Za-z][A-Za-z0-9._]{2,63}", t):
        return "tiktok", "@" + t
    return "", ""


def make_sniper(platform: str, target: str, on_event, headless: bool = True):
    if platform == "tiktok":
        return TikTokSniper(target, on_event, headless=headless)
    return BrowserSniper(target, on_event, headless=headless)


def room_label(platform: str, target: str) -> str:
    """落盘文件名/URL 用：抖音用房间号，TikTok 用去掉 @ 的 handle。"""
    if platform == "tiktok":
        return (target or "").lstrip("@") or "tiktok"
    return target or "room"


# ---------------- 事件中枢 ----------------

class Hub:
    """事件中枢：缓冲 + WS 广播 + 落盘 + 转发。fetcher 回调全进这里。"""

    def __init__(self) -> None:
        self.buffer: deque[dict] = deque(maxlen=BUFFER_MAX)
        self.clients: set = set()          # asyncio WebSocketServerProtocol
        self.loop: asyncio.AbstractEventLoop | None = None
        self.jsonl_path = ""
        self._lock = threading.Lock()
        self.total = 0

    def publish(self, ev: dict) -> None:
        self.total += 1
        with self._lock:
            self.buffer.append(ev)
        if self.jsonl_path:
            try:
                with open(self.jsonl_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(ev, ensure_ascii=False) + "\n")
            except Exception as e:
                print(f"[log] 写入失败: {e}")
        if self.loop and self.clients:
            payload = json.dumps(ev, ensure_ascii=False)
            self.loop.call_soon_threadsafe(self._broadcast, payload)
        _forward(ev)

    def _broadcast(self, payload: str) -> None:
        async def _send(ws) -> None:
            try:
                await ws.send(payload)
            except Exception:
                # 客户端已断开（半开连接）：静默清理，异常不能无人接收
                self.clients.discard(ws)

        for ws in list(self.clients):
            self.loop.create_task(_send(ws))

    def set_jsonl(self, label: str, enabled: bool) -> None:
        if not enabled:
            self.jsonl_path = ""
            return
        os.makedirs(_data_dir, exist_ok=True)
        self.jsonl_path = os.path.join(_data_dir, f"{label}.jsonl")


hub = Hub()
sniper = None
sniper_lock = threading.Lock()
current_rid = ""
current_platform = ""
switch_requested = threading.Event()
switch_target: tuple[str, str] = ("", "")

# ---------------- 转发到下游（工作台 / 自建服务） ----------------

_fwd_q: "queue.Queue[dict]" = queue.Queue(maxsize=2000)
_forward_url = ""


def _to_downstream(ev: dict) -> dict | None:
    """把内部事件映射成工作台 /human 认的格式。"""
    t = ev.get("type")
    if t not in FORWARD_TYPES:
        return None
    nick = ev.get("user") or "观众"
    if t == "chat":
        return {"type": "chat", "nickname": nick, "content": ev.get("content", ""),
                "platform": ev.get("platform", "")}
    if t == "gift":
        return {"type": "gift", "nickname": nick, "giftName": ev.get("gift", "礼物"),
                "giftCount": ev.get("count", 1), "diamondCount": 0,
                "platform": ev.get("platform", "")}
    return {"type": t, "nickname": nick, "platform": ev.get("platform", "")}


def _forward_worker() -> None:
    import requests
    session = requests.Session()
    while True:
        ev = _fwd_q.get()
        body = _to_downstream(ev)
        if not body or not body.get("nickname"):
            continue
        try:
            session.post(_forward_url, json=body, timeout=4)
        except Exception as e:
            print(f"[forward] 推送失败: {e}")


def _forward(ev: dict) -> None:
    if not _forward_url:
        return
    try:
        _fwd_q.put_nowait(ev)
    except queue.Full:
        pass


def pretty(ev: dict) -> str:
    t = ev.get("type")
    u = ev.get("user", "")
    p = "TT" if ev.get("platform") == "tiktok" else "DY"
    if t == "chat":
        return f"[{p} 弹幕] {u}: {ev.get('content','')}"
    if t == "enter":
        return f"[{p} 进场] {u}"
    if t == "follow":
        return f"[{p} 关注] {u}"
    if t == "like":
        return f"[{p} 点赞] {u} x{ev.get('count',1)}"
    if t == "gift":
        return f"[{p} 礼物] {u} -> {ev.get('gift','?')} x{ev.get('count',1)}"
    if t == "stats":
        return f"[{p} 在线] {ev.get('online')}"
    if t == "room":
        return f"[房间] {ev.get('name','')}（{ev.get('rid','')}）"
    if t == "live":
        return "[开播] 直播中" if ev.get("on") else f"[下播] {ev.get('msg','等待开播')}"
    return f"[{t}] {ev}"


# ---------------- WebSocket 广播线程 ----------------

def ws_thread() -> None:
    async def handler(ws):
        hub.clients.add(ws)
        try:
            await ws.send(json.dumps({"type": "hello", "room": current_rid,
                                      "platform": current_platform,
                                      "buffered": len(hub.buffer),
                                      "ts": int(time.time())}, ensure_ascii=False))
            for ev in list(hub.buffer)[-50:]:
                await ws.send(json.dumps(ev, ensure_ascii=False))
            await ws.wait_closed()
        except Exception:
            pass
        finally:
            hub.clients.discard(ws)

    async def main():
        import websockets
        hub.loop = asyncio.get_running_loop()
        async with websockets.serve(handler, HOST, WS_PORT):
            print(f"[ws]   弹幕广播: ws://{HOST}:{WS_PORT}/ws")
            await asyncio.Future()

    asyncio.run(main())


# ---------------- HTTP 接口 ----------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):  # 静掉访问日志
        pass

    def _json(self, code: int, obj) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        global switch_target
        if urlparse(self.path).path != "/room":
            self._json(404, {"error": "not found"})
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            self._json(400, {"error": "bad json"})
            return
        raw = str(data.get("room") or data.get("rid") or data.get("url") or "").strip()
        if not raw:
            self._json(400, {"error": "缺字段：room（抖音直播间号 / TikTok @handle 或链接）"})
            return
        forced = str(data.get("platform") or "").strip().lower()
        if forced in ("douyin", "tiktok"):
            platform, target = forced, (normalize_handle(raw) if forced == "tiktok" else raw)
        else:
            platform, target = detect_platform(raw)
        if not target:
            self._json(400, {"error": "无法识别直播间标识，请填抖音房间号或 TikTok @handle / 链接"})
            return
        switch_target = (platform, target)
        switch_requested.set()
        self._json(200, {"ok": True, "platform": platform, "room": target,
                         "msg": "正在切换直播间"})

    def do_GET(self):
        u = urlparse(self.path)
        q = parse_qs(u.query)
        if u.path == "/events":
            try:
                limit = min(int(q.get("limit", ["50"])[0]), BUFFER_MAX)
            except ValueError:
                limit = 50
            with hub._lock:
                evs = list(hub.buffer)[-limit:]
            self._json(200, {"room": current_rid, "platform": current_platform,
                             "count": len(evs), "events": evs})
        elif u.path == "/status":
            self._json(200, {
                "platform": current_platform,
                "room": current_rid,
                "anchor": getattr(sniper, "anchor_name", ""),
                "live_frames": getattr(sniper, "_frame_count", 0),
                "events_total": hub.total,
                "ws_clients": len(hub.clients),
                "forward": _forward_url or None,
                "forward_pending": _fwd_q.qsize(),
                "uptime_s": int(time.time() - START_TS),
                "jsonl": hub.jsonl_path or None,
            })
        elif u.path == "/stream":
            self._sse()
        else:
            self._json(404, {"error": "not found",
                             "endpoints": ["GET /events?limit=N", "GET /status",
                                           "GET /stream (SSE)", "POST /room {room}"]})

    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        q: "queue.Queue[dict]" = queue.Queue(maxsize=200)
        hub.loop.call_soon_threadsafe(_sse_register, q)
        try:
            for ev in list(hub.buffer)[-30:]:
                self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode())
            self.wfile.flush()
            while True:
                try:
                    ev = q.get(timeout=15)
                    self.wfile.write(f"data: {json.dumps(ev, ensure_ascii=False)}\n\n".encode())
                except Exception:
                    self.wfile.write(b": keepalive\n\n")
                self.wfile.flush()
        except Exception:
            pass
        finally:
            hub.loop.call_soon_threadsafe(_sse_unregister, q)


_sse_queues: set = set()


def _sse_register(q):
    _sse_queues.add(q)


def _sse_unregister(q):
    _sse_queues.discard(q)


_last_stats_ts = 0.0


def _hub_publish_with_sse(ev: dict) -> None:
    global _last_stats_ts
    # 抖音侧的抓取器不带 platform 字段，统一在这里补，保证下游拿到的事件都能区分来源
    if not ev.get("platform") and current_platform:
        ev["platform"] = current_platform
    # 在线人数是高频推送（实测 27 帧里 16 条），不节流会把缓冲区和下游刷满，
    # 真弹幕反而被挤掉。10 秒一条足够任何前端展示。
    if ev.get("type") == "stats":
        now = time.time()
        if now - _last_stats_ts < 10:
            return
        _last_stats_ts = now
    hub.publish(ev)
    for q in list(_sse_queues):
        try:
            q.put_nowait(ev)
        except Exception:
            pass


# ---------------- 房间管理 ----------------

def start_sniper(platform: str, target: str) -> None:
    global sniper, current_rid, current_platform
    stop_sniper()
    current_platform = platform
    current_rid = target
    label = room_label(platform, target)
    hub.set_jsonl(label, LOG_JSONL)
    with hub._lock:
        hub.buffer.clear()
    hub.buffer.append({"type": "room", "rid": target, "platform": platform,
                       "ts": int(time.time())})
    sniper = make_sniper(platform, target, _hub_publish_with_sse)
    sniper.start()
    name = "TikTok" if platform == "tiktok" else "抖音"
    print(f"[sniper] 目标{name}直播间: {target}（POST /room 可运行时切换）")


def stop_sniper() -> None:
    global sniper
    if sniper:
        sniper.close()
        sniper = None


def switch_watch() -> None:
    """看门狗线程：处理运行时切房请求。"""
    global switch_target
    while True:
        switch_requested.wait()
        platform, target = switch_target
        switch_requested.clear()
        print(f"[switch] 切换直播间 -> {platform} / {target}")
        try:
            start_sniper(platform, target)
        except Exception as e:
            print(f"[switch] 切换失败: {e}")


# ---------------- 入口 ----------------

LOG_JSONL = True


def main() -> None:
    global LOG_JSONL, HOST, HTTP_PORT, WS_PORT, _forward_url
    ap = argparse.ArgumentParser(description="直播弹幕采集服务（抖音 / TikTok）")
    ap.add_argument("room", nargs="?", default="",
                    help="抖音直播间号，或 TikTok @handle / 直播间链接")
    ap.add_argument("--platform", choices=["auto", "douyin", "tiktok"], default="auto",
                    help="强制平台（默认按输入自动识别）")
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--http-port", type=int, default=HTTP_PORT)
    ap.add_argument("--ws-port", type=int, default=WS_PORT)
    ap.add_argument("--forward", default="",
                    help="把事件转发到下游，如 http://127.0.0.1:8010/human（工作台）")
    ap.add_argument("--no-log", action="store_true", help="不写 JSONL 落盘")
    args = ap.parse_args()

    HOST = args.host
    HTTP_PORT = args.http_port
    WS_PORT = args.ws_port
    LOG_JSONL = not args.no_log
    _forward_url = args.forward.strip()

    print("=" * 60)
    print(" 直播弹幕采集服务 · 国内抖音 / 国外 TikTok（浏览器旁路）")
    print("=" * 60)

    threading.Thread(target=ws_thread, daemon=True).start()
    threading.Thread(target=switch_watch, daemon=True).start()
    if _forward_url:
        threading.Thread(target=_forward_worker, daemon=True).start()
        print(f"[forward] 事件转发 -> {_forward_url}")

    httpd = ThreadingHTTPServer((HOST, HTTP_PORT), Handler)
    print(f"[http]  REST/SSE:  http://{HOST}:{HTTP_PORT}/"
          f"  (events / status / stream)")
    if args.room:
        platform, target = detect_platform(args.room)
        if args.platform != "auto":
            platform = args.platform
            if platform == "tiktok":
                target = normalize_handle(args.room)
        if not target:
            print(f"[sniper] 直播间标识无法识别: {args.room!r}")
        else:
            start_sniper(platform, target)
    else:
        print("[sniper] 未指定直播间，等待 POST /room …")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[exit] 收到 Ctrl+C，退出")
        stop_sniper()


if __name__ == "__main__":
    main()
