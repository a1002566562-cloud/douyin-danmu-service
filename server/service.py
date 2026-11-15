"""抖音直播弹幕采集服务（独立通用版）。

一条命令启动后，向任何本机 / 局域网程序输出标准化互动事件：
    chat(弹幕) / enter(进场) / follow(关注) / like(点赞) /
    gift(礼物，含名称与数量) / stats(在线人数) / room(主播名) / live(开播状态)
每条带 user 的事件都含 avatar(真实头像 URL) 与 avcolor(主色，头像缺失时前端画色块用)。

输出通道（可同时使用）：
    WebSocket   ws://127.0.0.1:8521/ws           每条事件一个 JSON 文本帧
    SSE         http://127.0.0.1:8520/stream     text/event-stream
    REST        http://127.0.0.1:8520/events?limit=50   最近 N 条（断线补发用）
                http://127.0.0.1:8520/status     房间/帧/事件计数
                POST /room {"rid":"123456"}      运行时切换直播间
    JSONL       data/<rid>.jsonl                 逐条落盘，方便回放/分析

启动：
    python -m server.service 79423499229
    python -m server.service <直播间号> --host 0.0.0.0 --ws-port 8521 --no-log
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import queue
import sys
import threading
import time
from collections import deque
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.douyin_fetcher import BrowserSniper  # noqa: E402

HOST = "127.0.0.1"
HTTP_PORT = 8520
WS_PORT = 8521
BUFFER_MAX = 500

_app_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_data_dir = os.path.join(_app_root, "data")

START_TS = time.time()


class Hub:
    """事件中枢：缓冲 + WS 广播 + 落盘。fetcher 回调全进这里。"""

    def __init__(self) -> None:
        self.buffer: deque[dict] = deque(maxlen=BUFFER_MAX)
        self.clients: set = set()          # asyncio WebSocketServerProtocol
        self.loop: asyncio.AbstractEventLoop | None = None
        self.jsonl_path = ""
        self._lock = threading.Lock()
        self.total = 0

    # ---- fetcher 回调（抓取线程里调用） ----
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

    def _broadcast(self, payload: str) -> None:
        async def _send(ws) -> None:
            try:
                await ws.send(payload)
            except Exception:
                # 客户端已断开（半开连接）：静默清理，异常不能无人接收
                self.clients.discard(ws)

        for ws in list(self.clients):
            self.loop.create_task(_send(ws))

    def set_jsonl(self, rid: str, enabled: bool) -> None:
        if not enabled:
            self.jsonl_path = ""
            return
        os.makedirs(_data_dir, exist_ok=True)
        self.jsonl_path = os.path.join(_data_dir, f"{rid}.jsonl")


hub = Hub()
sniper: BrowserSniper | None = None
sniper_lock = threading.Lock()
current_rid = ""
switch_requested = threading.Event()
switch_rid = ""


def pretty(ev: dict) -> str:
    t = ev.get("type")
    u = ev.get("user", "")
    if t == "chat":
        return f"[弹幕] {u}: {ev.get('content','')}"
    if t == "enter":
        return f"[进场] {u}"
    if t == "follow":
        return f"[关注] {u}"
    if t == "like":
        return f"[点赞] {u} x{ev.get('count',1)}"
    if t == "gift":
        return f"[礼物] {u} -> {ev.get('gift','?')} x{ev.get('count',1)}"
    if t == "stats":
        return f"[在线] {ev.get('online')}"
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
                                      "buffered": len(hub.buffer),
                                      "ts": int(time.time())}, ensure_ascii=False))
            # 旧客户端断线重连时补发最近 50 条
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
        global sniper, current_rid, switch_rid
        if urlparse(self.path).path != "/room":
            self._json(404, {"error": "not found"})
            return
        try:
            n = int(self.headers.get("Content-Length", 0))
            data = json.loads(self.rfile.read(n) or b"{}")
        except Exception:
            self._json(400, {"error": "bad json"})
            return
        rid = str(data.get("rid", "")).strip()
        # 支持直接粘整条链接
        import re
        m = re.search(r"live\.douyin\.com/(\d+)", rid)
        if m:
            rid = m.group(1)
        if not rid.isdigit():
            self._json(400, {"error": "rid 必须是直播间号数字（或整条直播间链接）"})
            return
        switch_rid = rid
        switch_requested.set()
        self._json(200, {"ok": True, "rid": rid, "msg": "正在切换直播间"})

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
            self._json(200, {"room": current_rid, "count": len(evs), "events": evs})
        elif u.path == "/status":
            self._json(200, {
                "room": current_rid,
                "anchor": getattr(sniper, "anchor_name", ""),
                "live_frames": getattr(sniper, "_frame_count", 0),
                "events_total": hub.total,
                "ws_clients": len(hub.clients),
                "uptime_s": int(time.time() - START_TS),
                "jsonl": hub.jsonl_path or None,
            })
        elif u.path == "/stream":
            self._sse()
        else:
            self._json(404, {"error": "not found",
                             "endpoints": ["GET /events?limit=N", "GET /status",
                                           "GET /stream (SSE)", "POST /room {rid}"]})

    def _sse(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        q: "queue.Queue[dict]" = queue.Queue(maxsize=200)
        hub.loop.call_soon_threadsafe(_sse_register, q)
        try:
            # 先补发最近 30 条
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


def _hub_publish_with_sse(ev: dict) -> None:
    hub.publish(ev)
    for q in list(_sse_queues):
        try:
            q.put_nowait(ev)
        except Exception:
            pass


# ---------------- 房间管理 ----------------

def start_sniper(rid: str) -> None:
    global sniper, current_rid
    stop_sniper()
    current_rid = rid
    hub.set_jsonl(rid, LOG_JSONL)
    with hub._lock:
        hub.buffer.clear()
    hub.buffer.append({"type": "room", "rid": rid, "ts": int(time.time())})
    sniper = BrowserSniper(rid, _hub_publish_with_sse)
    sniper.start()
    print(f"[sniper] 目标直播间: {rid}（POST /room 可运行时切换）")


def stop_sniper() -> None:
    global sniper
    if sniper:
        sniper.close()
        sniper = None


def switch_watch() -> None:
    """看门狗线程：处理运行时切房请求。"""
    global switch_rid
    while True:
        switch_requested.wait()
        rid = switch_rid
        switch_requested.clear()
        print(f"[switch] 切换直播间 -> {rid}")
        try:
            start_sniper(rid)
        except Exception as e:
            print(f"[switch] 切换失败: {e}")


# ---------------- 入口 ----------------

LOG_JSONL = True


def main() -> None:
    global LOG_JSONL, HOST, HTTP_PORT, WS_PORT
    ap = argparse.ArgumentParser(description="抖音直播弹幕采集服务")
    ap.add_argument("room", nargs="?", default="", help="直播间号（live.douyin.com/后面那串数字）")
    ap.add_argument("--host", default=HOST)
    ap.add_argument("--http-port", type=int, default=HTTP_PORT)
    ap.add_argument("--ws-port", type=int, default=WS_PORT)
    ap.add_argument("--no-log", action="store_true", help="不写 JSONL 落盘")
    args = ap.parse_args()

    HOST = args.host
    HTTP_PORT = args.http_port
    WS_PORT = args.ws_port
    LOG_JSONL = not args.no_log

    print("=" * 56)
    print(" 抖音直播弹幕采集服务（浏览器旁路，输出全部互动事件）")
    print("=" * 56)

    threading.Thread(target=ws_thread, daemon=True).start()
    threading.Thread(target=switch_watch, daemon=True).start()

    httpd = ThreadingHTTPServer((HOST, HTTP_PORT), Handler)
    print(f"[http]  REST/SSE:  http://{HOST}:{HTTP_PORT}/"
          f"  (events / status / stream)")
    if args.room:
        start_sniper(args.room)
    else:
        print("[sniper] 未指定直播间号，等待 POST /room …")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\n[exit] 收到 Ctrl+C，退出")
        stop_sniper()


if __name__ == "__main__":
    main()
