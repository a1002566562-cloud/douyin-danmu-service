"""探测当前正在直播的抖音直播间（用于回归测试）。

用法：python tools/probe_douyin_live.py
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

    with sync_playwright() as p:
        b = _launch_any(p, dict(headless=True, args=[
            "--disable-blink-features=AutomationControlled", "--mute-audio"]))
        ctx = b.new_context(user_agent=UA, viewport={"width": 1400, "height": 900},
                            locale="zh-CN", timezone_id="Asia/Shanghai")
        page = ctx.new_page()
        page.goto("https://live.douyin.com/", timeout=90000, wait_until="domcontentloaded")
        page.wait_for_timeout(8000)
        print("TITLE:", page.title())
        hrefs = page.eval_on_selector_all(
            "a[href*='live.douyin.com']",
            "els => els.map(e => ({h: e.getAttribute('href'), t: (e.innerText||'').trim().slice(0,40)}))")
        seen: set[str] = set()
        out = []
        for it in hrefs:
            m = re.search(r"live\.douyin\.com/(\d{6,})", it.get("h") or "")
            if m and m.group(1) not in seen:
                seen.add(m.group(1))
                out.append({"rid": m.group(1), "text": it.get("t", "")})
        print("ROOMS:", json.dumps(out[:12], ensure_ascii=False, indent=1))
        html = page.content()
        rids = re.findall(r'"web_rid"\s*:\s*"(\d{6,})"', html)
        print("WEB_RIDS:", list(dict.fromkeys(rids))[:10])
        b.close()


if __name__ == "__main__":
    main()
