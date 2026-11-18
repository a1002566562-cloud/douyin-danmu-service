"""探测当前正在直播的 TikTok 直播间（用于校准抓取器）。

用法：
    python tools/probe_tiktok_live.py            # 打开 /live 页，列出在播房间
    python tools/probe_tiktok_live.py @handle    # 只检查某个 handle 是否在播

只做只读探测，不改任何东西。
"""
from __future__ import annotations

import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server.douyin_fetcher import UA, _launch_any  # noqa: E402


def main() -> None:
    from playwright.sync_api import sync_playwright

    target = sys.argv[1] if len(sys.argv) > 1 else ""
    url = (f"https://www.tiktok.com/{target}/live" if target
           else "https://www.tiktok.com/live")
    with sync_playwright() as p:
        b = _launch_any(p, dict(headless=True, args=[
            "--disable-blink-features=AutomationControlled", "--mute-audio"]))
        ctx = b.new_context(user_agent=UA, viewport={"width": 1400, "height": 900},
                            locale="en-US", timezone_id="Asia/Shanghai")
        page = ctx.new_page()
        ws_urls: list[str] = []
        page.on("websocket", lambda ws: ws_urls.append(ws.url))
        page.goto(url, timeout=90000, wait_until="domcontentloaded")
        page.wait_for_timeout(9000)

        print("TITLE:", page.title())
        body = page.inner_text("body")[:400]
        print("BODY:", " | ".join(body.splitlines())[:300])
        print("WS:", len(ws_urls), "webcast:", sum(1 for u in ws_urls if "webcast" in u))
        for u in ws_urls[:6]:
            print("  -", u.split("?")[0][:110])

        links = page.eval_on_selector_all(
            'a[href*="/live"], a[href*="@"]',
            "els => els.map(e => ({h: e.getAttribute('href'), t: (e.innerText||'').trim().slice(0,50)}))")
        seen, out = set(), []
        for it in links:
            h = it.get("h") or ""
            m = re.search(r"/@([A-Za-z0-9._]+)/live", h)
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                out.append({"handle": "@" + m.group(1), "text": it.get("t", "")})
        print("LIVE_LINKS:", json.dumps(out[:15], ensure_ascii=False, indent=1))

        html = page.content()
        ids = re.findall(r'"uniqueId":"([A-Za-z0-9._]{2,40})"', html)
        print("UNIQUE_IDS:", json.dumps(list(dict.fromkeys(ids))[:12], ensure_ascii=False))
        b.close()


if __name__ == "__main__":
    main()
