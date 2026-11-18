"""TikTok 直播弹幕抓取（浏览器旁路）。

TikTok 与抖音属于同一套 webcast 协议族：
    PushFrame { 1:seqId, 2:logId, 5:headers, 6:"pb", 7:"msg", 8:gzip(payload) }
    payload 解压 = WebcastResponse { 1: repeated Message }
    Message   { 1: method, 2: payload, 3: msgId }
    ChatMessage { 2: user, 3: content }
TikTok 的消息 method 命名同样是 WebcastChatMessage / WebcastGiftMessage /
WebcastLikeMessage / WebcastMemberMessage / WebcastSocialMessage，
所以帧解析与事件映射直接复用 douyin_fetcher 里已经实测跑通的那套，
本文件只负责「怎么把 TikTok 直播间打开、怎么判断在播、怎么取主播名」。

与抖音侧的差别：
    - 直播间地址：https://www.tiktok.com/@handle/live（房间标识是 @handle，不是数字房间号）
    - 页面语言 en-US，下播文案是 "LIVE has ended"
    - 未登录时 TikTok 常常不推弹幕，需要 --login 生成 login_state.json
"""
from __future__ import annotations

import json
import os
import queue
import re
import sys
import threading
import time
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from server import pb_wire as pb  # noqa: E402
from server.douyin_fetcher import (  # noqa: E402
    UA, _brief, _launch_any, _load_storage_state, iter_messages, to_event,
)

END_MARKS = ("LIVE has ended", "直播已结束", "LIVE ended", "This LIVE has ended",
             "已经结束", "暂未开播", "This live has ended")
LIVE_MARKS = ("Send a comment", "Viewers", "Liked", "LIVE", "在线观众", "说点什么")

# TikTok 的 GiftStruct 没有独立 name 字段，礼物名取自 describe（形如 "Sent GG"）
_GIFT_PREFIX = re.compile(r"^(?:Sent|Send|送出了?|赠送)\s+", re.I)
_GIFT_TAIL = re.compile(r"\s*[x×]\s*\d+$", re.I)

# 头像主色：TikTok 的 Image 不带 avgColor（抖音才有），用 URL 哈希取稳定色，
# 保证同一个人在界面上永远是同一个色块，不会每次刷新换颜色。
_PALETTE = ("#7F77DD", "#1D9E75", "#D85A30", "#D4537E",
            "#378ADD", "#639922", "#BA7517", "#993556")


def clean_gift_name(name: str) -> str:
    n = _GIFT_PREFIX.sub("", (name or "").strip())
    n = _GIFT_TAIL.sub("", n).strip()
    return n or "礼物"


def stable_color(key: str) -> str:
    if not key:
        return _PALETTE[0]
    h = 0
    for ch in key:
        h = (h * 131 + ord(ch)) & 0xFFFFFFFF
    return _PALETTE[h % len(_PALETTE)]


def summarize(node, depth: int = 0):
    """把解析树压成可读结构，用于字段号校准（DANMU_DUMP=1 时打印）。"""
    if not isinstance(node, dict) or depth > 3:
        return node if not isinstance(node, bytes) else f"<bytes {len(node)}>"
    out = {}
    for k, vals in node.items():
        items = []
        for v in vals:
            if isinstance(v, dict):
                items.append(summarize(v, depth + 1))
            elif isinstance(v, bytes):
                items.append(f"<bytes {len(v)}>")
            elif isinstance(v, str):
                items.append(v[:60])
            else:
                items.append(v)
        out[k] = items
    return out


def find_user_with_avatar(msg: dict) -> dict:
    """在一条消息里找「带真实头像的 User」。

    抖音和 TikTok 的 User 都是 {1:id, 3:nickName, 9:avatarThumb(Image)}，
    而且一定有头像。礼物消息里 user 字段号偶尔漂移（实测拿到过长度为 1
    的假昵称 "t"），所以不认字段号，认结构：昵称非空 + 9 号字段带 http 头像。
    """
    best: dict = {}
    stack: list[tuple[dict, int]] = [(msg, 0)]
    while stack:
        node, depth = stack.pop()
        if depth > 3 or not isinstance(node, dict):
            continue
        nick = pb.get_str(node, 3)
        imgs = node.get(9)
        if nick and imgs:
            img = imgs[0] if isinstance(imgs[0], dict) else None
            urls = (img or {}).get(1) or []
            if any(isinstance(u, str) and u.startswith("http") for u in urls):
                if not best or len(nick) > len(pb.get_str(best, 3)):
                    best = node
        for vals in node.values():
            for v in vals:
                if isinstance(v, dict):
                    stack.append((v, depth + 1))
    return best


def avatar_of(user: dict) -> str:
    for fno in (9, 10, 11):
        img = pb.get(user, fno)
        if not isinstance(img, dict):
            continue
        for item in (img.get(1) or []):
            url = item if isinstance(item, str) else pb.get_str(item, 1)
            if isinstance(url, str) and url.startswith("http") and "DefAvatar" not in url:
                return url.replace("http://", "https://", 1)
    return ""





class InvalidRoom(RuntimeError):
    """既不是直播间也不是有效 handle，重连没有意义。"""


def normalize_handle(target: str) -> str:
    """把各种输入统一成 @handle。

    支持：@user70367980266313 / user70367980266313 /
          https://www.tiktok.com/@user70367980266313/live?...
    """
    t = (target or "").strip().strip('"').strip("'")
    if not t:
        return ""
    m = re.search(r"tiktok\.com/@([A-Za-z0-9._]+)", t)
    if m:
        return "@" + m.group(1)
    t = t.split("?")[0].split("#")[0].rstrip("/")
    if "/" in t:
        t = t.rsplit("/", 1)[-1]
    if t.startswith("@"):
        return t
    if re.fullmatch(r"[A-Za-z0-9._]{2,64}", t):
        return "@" + t
    return ""


class TikTokSniper:
    """用真实 Chromium 打开 TikTok 直播间，旁路监听 webcast WSS 帧。"""

    def __init__(self, target: str, on_event: Callable[[dict], None],
                 headless: bool = True, debug: bool = False):
        self.handle = normalize_handle(target)
        self.live_url = f"https://www.tiktok.com/{self.handle}/live" if self.handle else ""
        self.on_event = on_event
        self.headless = headless
        self.debug = debug
        self._pw = None
        self._browser = None
        self._page = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._nudge = threading.Event()
        self._frame_count = 0
        self._event_count = 0
        self._rawq: queue.Queue = queue.Queue(maxsize=800)
        self._parser: threading.Thread | None = None
        self._dropped = 0
        self._ws_urls: list[str] = []
        self._webcast_count = 0
        self._storage_state = _load_storage_state()
        self.room_title = ""
        self.anchor_name = ""
        self.methods_seen: dict[str, int] = {}
        self._recent: dict[str, float] = {}
        self._dup = 0
        self._dumped: set[str] = set()

    # ---------- 生命周期 ----------
    def start(self) -> None:
        if not self.handle:
            raise InvalidRoom(
                f"TikTok 直播间标识无效: {self.handle!r}。"
                "请填 @用户名 或 https://www.tiktok.com/@用户名/live")
        self._parser = threading.Thread(target=self._parse_loop, daemon=True)
        self._parser.start()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _parse_loop(self) -> None:
        """只解析，不碰浏览器：Playwright 消息泵绝不能被 gzip+protobuf 拖住。"""
        while not self._stop.is_set():
            try:
                raw = self._rawq.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                for method, msg in iter_messages(raw):
                    if self.debug and method:
                        self.methods_seen[method] = self.methods_seen.get(method, 0) + 1
                    if (os.environ.get("DANMU_DUMP") and method
                            and method not in self._dumped):
                        self._dumped.add(method)
                        try:
                            print(f"[dump] {method} = "
                                  f"{json.dumps(summarize(msg), ensure_ascii=False)[:3000]}",
                                  flush=True)
                        except Exception:
                            pass
                    ev = to_event(method, msg)
                    if not ev:
                        continue
                    ev.setdefault("platform", "tiktok")
                    self._normalize(ev, msg)
                    if self._is_dup(ev):
                        self._dup += 1
                        continue
                    self._event_count += 1
                    self.on_event(ev)
            except Exception as e:
                if self.debug:
                    print(f"[tiktok] 帧解析异常: {e}")

    def _normalize(self, ev: dict, msg: dict) -> None:
        """补齐 TikTok 与抖音不同源的部分：礼物名、可疑昵称、头像主色。"""
        if ev.get("type") == "gift":
            ev["gift"] = clean_gift_name(ev.get("gift", ""))
        # 礼物消息的 user 字段号偶发漂移（实测解析出过昵称 "t"）：
        # 只要昵称短得像噪声、或根本没取到头像，就按结构重新找带头像的 User
        if ev.get("user") and (len(ev["user"]) <= 1 or not ev.get("avatar")):
            u = find_user_with_avatar(msg)
            if u:
                ev["user"] = pb.get_str(u, 3) or ev["user"]
                ev["avatar"] = avatar_of(u) or ev.get("avatar", "")
        if ev.get("user") and not ev.get("avcolor"):
            ev["avcolor"] = stable_color(ev.get("avatar") or ev.get("user", ""))

    def _is_dup(self, ev: dict) -> bool:
        """同一条消息有时会在相邻帧里重发（连击/补推），4 秒内同人同内容只留一条。"""
        t = ev.get("type")
        if t not in ("chat", "gift", "enter", "follow"):
            return False
        body = ev.get("content") or ev.get("gift") or ""
        if not body:
            return False
        key = f"{t}|{ev.get('user','')}|{body}"
        now = time.time()
        last = self._recent.get(key, 0)
        if now - last < 4:
            return True
        self._recent[key] = now
        if len(self._recent) > 400:
            cutoff = now - 30
            self._recent = {k: v for k, v in self._recent.items() if v >= cutoff}
        return False


    def _run(self) -> None:
        from playwright.sync_api import sync_playwright
        backoff = 3
        with sync_playwright() as p:
            self._pw = p
            while not self._stop.is_set():
                try:
                    self._browser = _launch_any(p, dict(headless=self.headless, args=[
                        "--disable-blink-features=AutomationControlled",
                        "--mute-audio", "--autoplay-policy=no-user-gesture-required",
                    ]))
                    self._ws_urls.clear()
                    self._webcast_count = 0
                    ctx_kwargs = dict(
                        user_agent=UA, viewport={"width": 1280, "height": 800},
                        locale="en-US", timezone_id="Asia/Shanghai")
                    if self._storage_state:
                        ctx_kwargs["storage_state"] = self._storage_state
                    ctx = self._browser.new_context(**ctx_kwargs)
                    self._page = ctx.new_page()
                    self._page.on("websocket", self._attach_ws)
                    print(f"[tiktok] 打开 {self.live_url}", flush=True)
                    self._page.goto(self.live_url, timeout=60000, wait_until="domcontentloaded")
                    self.room_title = self._safe_title()

                    if not self._wait_ready():
                        self.anchor_name = self._safe_anchor()
                        self.on_event({"type": "room", "name": self.anchor_name,
                                       "rid": self.handle, "platform": "tiktok",
                                       "ts": int(time.time())})
                        print("[tiktok] 主播未开播，等待开播…（每 60 秒查一次页面）", flush=True)
                        self.on_event({"type": "live", "on": False, "platform": "tiktok",
                                       "ts": int(time.time())})
                        try:
                            self._browser.close()
                        except Exception:
                            pass
                        if not self._wait_live_by_page():
                            break
                        self.on_event({"type": "live", "on": True, "platform": "tiktok",
                                       "ts": int(time.time())})
                        continue

                    self.anchor_name = self._safe_anchor()
                    self.on_event({"type": "room", "name": self.anchor_name,
                                   "rid": self.handle, "platform": "tiktok",
                                   "ts": int(time.time())})
                    print(f"[tiktok] 已进入直播间 {self.handle} | "
                          f"{self.anchor_name or self.room_title}", flush=True)
                    self.on_event({"type": "live", "on": True, "platform": "tiktok",
                                   "ts": int(time.time())})

                    last_live_check = time.time()
                    last_move = time.time()
                    last_frames = self._frame_count
                    entered_at = time.time()
                    diag_shown = False
                    while not self._stop.is_set():
                        self._page.wait_for_timeout(120)
                        if (self._webcast_count == 0 and not diag_shown
                                and time.time() - entered_at > 25):
                            diag_shown = True
                            print(self._diagnose(), flush=True)
                        if self._page.is_closed():
                            raise RuntimeError("页面已关闭")
                        if self._nudge.is_set():
                            self._nudge.clear()
                            raise RuntimeError("外部看门狗要求重连")
                        if self._frame_count != last_frames:
                            last_frames = self._frame_count
                            last_move = time.time()
                        elif time.time() - last_move > 60:
                            raise RuntimeError("60 秒未收到任何推送帧")
                        if time.time() - last_live_check > 30:
                            last_live_check = time.time()
                            if not self._live_on():
                                raise RuntimeError("直播已结束")
                    try:
                        self._browser.close()
                    except Exception:
                        pass
                except Exception as e:
                    if self._stop.is_set():
                        break
                    if isinstance(e, InvalidRoom):
                        print(str(e), flush=True)
                        self.on_event({"type": "live", "on": False, "platform": "tiktok",
                                       "msg": "TikTok 直播间无效或需要登录", "ts": int(time.time())})
                        try:
                            if self._browser:
                                self._browser.close()
                        except Exception:
                            pass
                        break
                    if "直播已结束" in str(e):
                        print("[tiktok] 检测到下播，转为等待开播", flush=True)
                        self.on_event({"type": "live", "on": False, "platform": "tiktok",
                                       "ts": int(time.time())})
                    else:
                        print(f"[tiktok] 异常: {e}；{backoff}s 后重连", flush=True)
                    try:
                        if self._browser:
                            self._browser.close()
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                else:
                    backoff = 3

    # ---------- 页面判定 ----------
    def _safe_title(self) -> str:
        try:
            return self._page.title() or ""
        except Exception:
            return ""

    def _body_text(self, limit: int = 600) -> str:
        try:
            return self._page.inner_text("body")[:limit]
        except Exception:
            return ""

    def _live_on(self) -> bool:
        txt = self._body_text()
        if not txt:
            return True
        return not any(k in txt for k in END_MARKS)

    def _wait_ready(self, timeout: int = 30) -> bool:
        """True=已进入直播；False=未开播/已结束。"""
        end = time.time() + timeout
        while time.time() < end and not self._stop.is_set():
            # 最硬的判据：webcast WSS 已建立
            if self._webcast_count > 0:
                return True
            txt = self._body_text()
            if any(k in txt for k in END_MARKS):
                return False
            if any(k in txt for k in LIVE_MARKS) and "live" in self._safe_title().lower():
                return True
            try:
                self._page.wait_for_timeout(600)
            except Exception:
                return True
        return self._webcast_count > 0 and self._live_on()

    def _wait_live_by_page(self, period: int = 60) -> bool:
        """下播后用 HTTP 查页面文案，等开播。返回 False 表示被要求停止。"""
        import requests
        while not self._stop.is_set():
            for _ in range(period):
                if self._stop.is_set():
                    return False
                time.sleep(1)
            try:
                r = requests.get(self.live_url, timeout=15, headers={"User-Agent": UA})
                txt = r.text
                if not any(k in txt for k in END_MARKS) and "isLive" in txt:
                    return True
            except Exception:
                pass
        return False

    def _safe_anchor(self) -> str:
        """从 title 里取主播名：'小羊🐑 正在直播 - TikTok 直播' / 'xxx is LIVE'。"""
        title = self._safe_title()
        if not title:
            return ""
        for pat in (r"^(.{1,40}?)\s*(?:正在直播|is LIVE|LIVE)", r"^(.{1,40}?)\s*[-|]"):
            m = re.search(pat, title, re.I)
            if m:
                name = m.group(1).strip(" -|·")
                if 1 < len(name) <= 30 and "tiktok" not in name.lower():
                    return name
        return ""

    def _diagnose(self) -> str:
        return (
            f"[诊断] TikTok {self.handle} 没有建立 webcast 弹幕通道。\n"
            f"[诊断] 页面标题: {self._safe_title()[:90]}\n"
            f"[诊断] 页面文字: {' | '.join(self._body_text(300).splitlines())[:200]}\n"
            f"[诊断] WebSocket {len(self._ws_urls)} 条，其中 webcast {self._webcast_count} 条，"
            f"帧 {self._frame_count}。\n"
            "[诊断] 常见原因: (1) 未登录（TikTok 对游客常常不推弹幕）；"
            "(2) 本机访问 tiktok.com 需代理而浏览器没走代理；"
            "(3) 直播间已结束或地区限制。\n"
            "[诊断] 登录: 运行「python -m server.tiktok_fetcher --login」扫码，"
            "生成 login_state.json 后重启服务。"
        )

    # ---------- 帧接入 ----------
    def _attach_ws(self, ws) -> None:
        try:
            self._ws_urls.append(ws.url)
        except Exception:
            pass
        if "webcast" not in ws.url:
            if self.debug:
                print(f"[tiktok] 忽略 WS: {ws.url[:80]}")
            return
        self._webcast_count += 1
        print(f"[tiktok] 弹幕通道已接管 ({ws.url.split('?')[0]})", flush=True)
        ws.on("framereceived", self._on_frame)

    def _on_frame(self, payload) -> None:
        self._frame_count += 1
        raw = payload if isinstance(payload, (bytes, bytearray)) else str(payload).encode()
        try:
            self._rawq.put_nowait(bytes(raw))
        except queue.Full:
            try:
                self._rawq.get_nowait()
            except queue.Empty:
                pass
            self._dropped += 1
            try:
                self._rawq.put_nowait(bytes(raw))
            except queue.Full:
                pass

    def close(self) -> None:
        self._stop.set()
        try:
            if self._browser:
                self._browser.close()
        except Exception:
            pass

    @property
    def alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())


# ---------------- CLI ----------------

def _login_flow() -> None:
    """打开可见浏览器登录 TikTok，把 cookie 存成 login_state.json。"""
    from playwright.sync_api import sync_playwright
    out = os.environ.get("DANMU_LOGIN_STATE") or os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "login_state.json")
    print("[login] 浏览器即将打开，请完成 TikTok 登录（扫码/账号密码均可）。")
    print("[login] 登录成功后回到控制台按回车保存并退出。")
    with sync_playwright() as p:
        b = _launch_any(p, dict(headless=False,
                                args=["--disable-blink-features=AutomationControlled"]))
        ctx = b.new_context(user_agent=UA, viewport={"width": 1280, "height": 800},
                            locale="en-US")
        page = ctx.new_page()
        page.goto("https://www.tiktok.com/", timeout=60000, wait_until="domcontentloaded")
        input("[login] 完成登录后按回车保存登录态… ")
        ctx.storage_state(path=out)
        b.close()
    print(f"[login] 已保存: {out}")


def _cli() -> None:
    if "--login" in sys.argv:
        _login_flow()
        return
    target = next((a for a in sys.argv[1:] if not a.startswith("-")), "")
    if not target:
        print("用法: python -m server.tiktok_fetcher <@handle|直播间链接> [--head] [--raw] [--login]")
        return

    def show(ev: dict) -> None:
        t = ev.get("type")
        if t == "chat":
            print(f"[弹幕] {ev.get('user')}: {ev.get('content')}")
        elif t == "enter":
            print(f"[进场] {ev.get('user')}")
        elif t == "follow":
            print(f"[关注] {ev.get('user')}")
        elif t == "gift":
            print(f"[礼物] {ev.get('user')} -> {ev.get('gift')} x{ev.get('count')}")
        elif t == "like":
            print(f"[点赞] {ev.get('user')} x{ev.get('count')}")

    s = TikTokSniper(target, show, headless="--head" not in sys.argv, debug=True)
    s.start()
    try:
        while True:
            time.sleep(5)
            methods = ",".join(f"{k.split('.')[-1]}x{v}" for k, v in
                               sorted(s.methods_seen.items(), key=lambda x: -x[1])[:6])
            print(f"[状态] 帧={s._frame_count} 事件={s._event_count} 去重={s._dup} "
                  f"丢弃={s._dropped} | method: {methods or '-'}")
    except KeyboardInterrupt:
        s.close()


if __name__ == "__main__":
    _cli()
