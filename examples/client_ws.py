"""最小 Python 接入示例：WebSocket 订阅弹幕事件。

用法: python examples/client_ws.py [ws地址]
默认 ws://127.0.0.1:8521/ws
"""
import asyncio
import json
import sys

import websockets


async def main() -> None:
    try:
        sys.stdout.reconfigure(line_buffering=True)   # 重定向到文件/管道时也能实时看到
    except Exception:
        pass
    url = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8521/ws"
    async with websockets.connect(url) as ws:
        print(f"已连接 {url}")
        async for raw in ws:
            ev = json.loads(raw)
            t = ev.get("type")
            pf = "TT" if ev.get("platform") == "tiktok" else "DY"
            if t == "hello":
                print(f"[hello] 平台={ev.get('platform') or '?'} 房间={ev.get('room')} "
                      f"缓冲={ev.get('buffered')} 条")
            elif t == "chat":
                print(f"[{pf} 弹幕] {ev['user']}: {ev['content']}  头像={ev.get('avatar','')[:40]}")
            elif t == "gift":
                print(f"[{pf} 礼物] {ev['user']} 送 {ev['gift']} x{ev['count']}")
            elif t == "like":
                print(f"[{pf} 点赞] {ev['user']} x{ev['count']}")
            elif t in ("enter", "follow"):
                print(f"[{pf} {'进场' if t == 'enter' else '关注'}] {ev['user']}")
            elif t == "stats":
                print(f"[{pf} 在线] {ev['online']}")
            elif t == "live":
                print("[开播]" if ev.get("on") else f"[下播] {ev.get('msg', '')}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
