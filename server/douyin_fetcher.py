"""抖音直播弹幕抓取（浏览器旁路 + 协议直连双通道）。

主用通道：Playwright 打开真实直播间页面，旁路监听 webcast WSS 帧
          —— 签名/设备指纹交给浏览器，不碰风控。
备用通道：直连 WSS（部分网络/时段可用，可能被 DEVICE_BLOCKED 拒）。

帧结构（2026-11 实测）：
    PushFrame { 1:seqId, 2:logId, 3:service, 4:method, 5:headers, 6:"pb", 7:"msg", 8:gzip(payload) }
    payload 解压后 = WebcastResponse { 1: repeated WebcastMessage, ... }
    WebcastMessage { 1: method(str), 2: payload(msg), 3: msgId }
    ChatMessage    { 2: user, 3: content }
    MemberMessage  { 2: user }
    User           { 1: id, 3: nickName }
"""
from __future__ import annotations

import gzip
import json
import os
import queue
import random
import re
import sys
import threading
import time
from typing import Callable

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pb_wire as pb  # noqa: E402

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")

USER_FIELDS = {"chat": (2,), "enter": (2,), "follow": (2,), "like": (5, 2), "gift": (7, 2)}

# 无效/失效的直播间号会被抖音回落到「抖音直播电脑版」通用首页。
# 那个页面没有直播 SDK，一条 webcast WSS 都不会建，帧永远是 0；
# 以前程序只查「直播已结束」，首页里没这几个字就误判成「已进入直播间」，
# 然后陷入 60 秒无帧 → 看门狗 → 重连的死循环。
HOME_MARKS = ("抖音直播电脑版", "抖音直播网页版入口", "京ICP备16016397号",
              "下载抖音精选", "电子营业执照", "信息网络传播视听节目许可证")


class InvalidRoom(RuntimeError):
    """页面不是直播间（房间号无效 / 被回落到首页），重连多少次都没用。"""


# ---------------- 帧解析 ----------------

def iter_messages(frame_bytes: bytes):
    """从一帧二进制里取出 [(method, payload_dict), ...]。"""
    frame = pb.parse(frame_bytes)
    body = None
    # payload 可能在 f8（新协议），也可能在 f5（旧协议）
    for fno in (8, 5):
        v = pb.get(frame, fno)
        if isinstance(v, bytes) and v[:2] == b"\x1f\x8b":
            body = v
            break
        if isinstance(v, dict) and 1 in v:
            # 旧协议 payload 直接被解析成 WebcastResponse
            return _messages_from_resp(v)
    if body is None:
        return []
    try:
        resp = pb.parse(gzip.decompress(body))
    except Exception:
        return []
    return _messages_from_resp(resp)


def _messages_from_resp(resp: dict) -> list[tuple[str, dict]]:
    out: list[tuple[str, dict]] = []
    for m in resp.get(1) or []:
        if not isinstance(m, dict):
            continue
        method = pb.get_str(m, 1)
        if not method:
            # 旧协议：f2 是 method，f3 是 payload
            method = pb.get_str(m, 2)
            payload = pb.get(m, 3)
        else:
            payload = pb.get(m, 2)
        if isinstance(payload, bytes):
            payload = pb.parse(payload)
        if method and isinstance(payload, dict):
            out.append((method, payload))
    return out


def _find_user(msg: dict, method: str) -> dict:
    for fno in USER_FIELDS.get(method, (2,)):
        v = pb.get(msg, fno)
        if isinstance(v, dict) and pb.get_str(v, 3):
            return v
    # 兜底：树里找带昵称的 user 结构
    stack = [msg]
    while stack:
        node = stack.pop()
        if isinstance(node, dict):
            if pb.get_str(node, 3) and pb.get_int(node, 1) and len(node) >= 2:
                return node
            for vals in node.values():
                stack.extend(v for v in vals if isinstance(v, dict))
    return {}


def _avatar_of(user: dict) -> tuple[str, str]:
    """从 User 里取头像，返回 (url, avg_color)。

    实测结构（2026-11）：
        User   9/10/11 = avatarThumb / avatarMedium / avatarLarge
        Image  1: repeated urlList（每项可能是 str，也可能是 {1: url} 结构）
               2: uri（没设头像时是 "webcast/small_DefAvatar.png"）
               5: avgColor（如 "#A3967C"，抖音按图算出的主色）
    缩略图优先——下栏头像才 22px，用大图纯属浪费流量。
    没有真实头像时 url 返回空、只给颜色，前端拿颜色垫底色块，比一片灰好看。
    """
    color = ""
    for fno in (9, 10, 11):
        img = pb.get(user, fno)
        if not isinstance(img, dict):
            continue
        if not color:
            color = pb.get_str(img, 5)
        for item in (img.get(1) or []):
            url = item if isinstance(item, str) else pb.get_str(item, 1)
            if not url.startswith("http"):
                continue
            # 没设头像的人，抖音发的是 small_DefAvatar.png。这种不如用首字色块
            if "DefAvatar" in url:
                return "", color
            return url.replace("http://", "https://", 1), color
        uri = pb.get_str(img, 2)
        if uri.startswith("http"):
            return uri.replace("http://", "https://", 1), color
        # 只有默认头像（small_DefAvatar.png）时不入库，交给前端画的色块
        return "", color
    return "", color


def to_event(method: str, msg: dict) -> dict | None:
    """把协议消息转成前端事件。"""
    now = int(time.time())
    kind = {
        "WebcastChatMessage": "chat",
        "WebcastMemberMessage": "enter",
        "WebcastSocialMessage": "follow",
        "WebcastLikeMessage": "like",
        "WebcastGiftMessage": "gift",
        "WebcastRoomUserSeqMessage": "stats",
    }.get(method)
    if not kind:
        return None

    if kind == "stats":
        total = pb.get_int(msg, 3) or pb.get_int(pb.get(msg, 2), 1)
        return {"type": "stats", "online": total, "ts": now} if total else None

    user = _find_user(msg, "chat" if kind == "chat" else kind)
    nick = pb.get_str(user, 3) or "游客"
    av, avc = _avatar_of(user)

    if kind == "chat":
        content = pb.get_str(msg, 3)
        if not content:
            cands = [s for s in pb.walk_strings(msg) if s != nick]
            content = max(cands, key=len) if cands else ""
        content = content.strip()
        # 协议里偶尔夹带资源路径（如 webcast/xxx.png），不是真弹幕，丢掉
        if re.search(r"(\.png|\.jpg|\.jpeg|\.webp|\.gif)$", content, re.I) or \
           content.startswith(("webcast/", "http://", "https://")):
            return None
        return {"type": "chat", "user": nick, "avatar": av, "avcolor": avc,
                "content": content, "ts": now} if content else None
    if kind == "enter":
        return {"type": "enter", "user": nick, "avatar": av, "avcolor": avc, "ts": now}
    if kind == "follow":
        return {"type": "follow", "user": nick, "avatar": av, "avcolor": avc, "ts": now}
    if kind == "like":
        return {"type": "like", "user": nick, "avatar": av, "avcolor": avc, "ts": now,
                "count": pb.get_int(msg, 2, default=1) or 1}
    if kind == "gift":
        gift = pb.get(msg, 15)
        gname = pb.get_str(gift, 2) if isinstance(gift, dict) else ""
        cnt = pb.get_int(msg, 5) or pb.get_int(msg, 4) or pb.get_int(msg, 6) or 1
        return {"type": "gift", "user": nick, "avatar": av, "avcolor": avc,
                "gift": gname or "礼物", "count": cnt, "ts": now}
    return None


# ---------------- 浏览器旁路抓取（主通道） ----------------

def _setup_bundled_browser() -> None:
    """让打包版用自带的 Chromium。

    浏览器路径实际由 Playwright 的 node 驱动解析，它继承本进程环境变量，
    所以必须在 sync_playwright().start() 之前设好。否则 frozen 环境下它会
    去 _MEIPASS/playwright/driver/package/.local-browsers/ 找一个不存在的
    chromium_headless_shell-1248 —— 客户机上报的就是这个错。
    """
    if not getattr(sys, "frozen", False):
        return
    base = getattr(sys, "_MEIPASS", "")
    if not base:
        return
    bundled = os.path.join(base, "ms-playwright")
    if os.path.isdir(bundled):
        os.environ.setdefault("PLAYWRIGHT_BROWSERS_PATH", bundled)


_setup_bundled_browser()


def _brief(e: Exception) -> str:
    lines = str(e).strip().splitlines()
    return (lines[0] if lines else repr(e))[:160]


def _configured_exe() -> str:
    """客户机显式指定浏览器：环境变量 DANMU_BROWSER_PATH 或 exe 同目录 browser.txt。"""
    p = (os.environ.get("DANMU_BROWSER_PATH") or "").strip().strip('"')
    if p and os.path.isfile(p):
        return p
    try:
        base = (os.path.dirname(sys.executable) if getattr(sys, "frozen", False)
                else os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        cfg = os.path.join(base, "browser.txt")
        if os.path.isfile(cfg):
            with open(cfg, "r", encoding="utf-8-sig") as f:
                lines = [x.strip().strip('"') for x in f.read().splitlines() if x.strip()]
            if lines and os.path.isfile(lines[0]):
                return lines[0]
    except Exception:
        pass
    return ""


def _app_dir() -> str:
    """配置文件放 exe 同目录（源码运行时放项目根）。"""
    if getattr(sys, "frozen", False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def login_state_path() -> str:
    return os.environ.get("DANMU_LOGIN_STATE") or os.path.join(_app_dir(), "login_state.json")


def _parse_netscape_cookies(path: str) -> list[dict]:
    """浏览器插件导出的 Netscape cookies.txt -> Playwright cookie 列表。"""
    out: list[dict] = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\r\n")
                if not line or line.startswith("#") or "\t" not in line:
                    continue
                cols = line.split("\t")
                if len(cols) < 7:
                    continue
                domain, _flag, cpath, secure, expires, name, value = cols[:7]
                try:
                    exp = float(expires)
                except ValueError:
                    exp = -1
                out.append({"name": name, "value": value, "domain": domain,
                            "path": cpath or "/", "expires": exp,
                            "httpOnly": False, "secure": secure.upper() == "TRUE",
                            "sameSite": "Lax"})
    except Exception as e:
        print(f"[login] cookies.txt 解析失败: {e}")
    return out


def _load_storage_state():
    """找登录态，返回可直接传给 new_context(storage_state=...) 的值。

    程序用的是 Playwright 新建的独立无痕上下文，跟用户在自己 Chrome/Edge
    里登录的账号完全不共享 cookie。私密 / 仅粉丝可见的直播间必须显式灌入登录态。
    """
    st = login_state_path()
    if os.path.isfile(st):
        try:
            with open(st, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"[login] 已加载登录态 {os.path.basename(st)}（{len(data.get('cookies') or [])} 条 cookie）",
                  flush=True)
            return st
        except Exception as e:
            print(f"[login] login_state.json 读取失败: {e}", flush=True)
    ck = os.path.join(_app_dir(), "cookies.txt")
    if os.path.isfile(ck):
        cookies = _parse_netscape_cookies(ck)
        if cookies:
            print(f"[login] 已加载 cookies.txt（{len(cookies)} 条 cookie）", flush=True)
            return {"cookies": cookies, "origins": []}
    print("[login] 未找到登录态，以游客身份访问；私密/需登录的直播间会抓不到弹幕。"
          "解决办法：运行「python -m server.douyin_fetcher --login」生成 login_state.json。", flush=True)
    return None


def _common_exes() -> list[str]:
    """Chrome/Edge 标准安装位置，channel 查找失败时兜底。"""
    rels = (
        r"Google\Chrome\Application\chrome.exe",
        r"Microsoft\Edge\Application\msedge.exe",
    )
    roots = (os.environ.get("PROGRAMFILES"), os.environ.get("PROGRAMFILES(X86)"),
             os.environ.get("LOCALAPPDATA"))
    out = [os.path.join(r, rel) for r in roots if r for rel in rels]
    return [p for p in out if os.path.isfile(p)]


def _launch_any(p, kwargs: dict):
    """按 指定路径 -> 系统 Chrome/Edge -> 常见安装目录 -> 安装包自带 Chromium 启动。

    分发到客户机不能假定对方装过浏览器，所以安装包里带了 headless shell。
    """
    errs: list[str] = []

    configured = _configured_exe()
    if configured:
        try:
            print(f"[sniper] 使用指定浏览器: {configured}")
            return p.chromium.launch(executable_path=configured, **kwargs)
        except Exception as e:
            errs.append(f"指定浏览器 {configured}: {_brief(e)}")

    for ch in ("chrome", "msedge"):
        try:
            return p.chromium.launch(channel=ch, **kwargs)
        except Exception as e:
            errs.append(f"channel={ch}: {_brief(e)}")

    for path in _common_exes():
        try:
            print(f"[sniper] 使用系统浏览器: {path}")
            return p.chromium.launch(executable_path=path, **kwargs)
        except Exception as e:
            errs.append(f"{path}: {_brief(e)}")

    try:
        b = p.chromium.launch(**kwargs)
        print("[sniper] 使用安装包自带的 Chromium")
        return b
    except Exception as e:
        errs.append(f"自带 Chromium: {_brief(e)}")

    raise RuntimeError(
        "没找到可用浏览器。已依次尝试：\n"
        "  1) DANMU_BROWSER_PATH / exe 同目录 browser.txt 指定的路径\n"
        "  2) 系统 Chrome、Edge（Playwright channel）\n"
        "  3) Chrome/Edge 常见安装目录\n"
        "  4) 安装包自带的 Chromium\n"
        "解决：安装 Chrome 或 Edge；或在 exe 同目录建 browser.txt，第一行写浏览器完整路径；"
        "或设环境变量 DANMU_BROWSER_PATH。\n"
        "明细：\n  " + "\n  ".join(errs)
    )


class BrowserSniper:
    """用真实 Chromium 打开直播间，监听其 WSS 帧。

    不参与握手，不注入脚本，页面照常播放（静音、headless）。
    """

    def __init__(self, web_rid: str, on_event: Callable[[dict], None],
                 headless: bool = True, debug: bool = False):
        self.web_rid = web_rid
        self.on_event = on_event
        self.headless = headless
        self.debug = debug
        self._pw = None
        self._browser = None
        self._page = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        # 外部（服务端看门狗）可以置位这个标记，让抓取线程自己回滚重建连接，
        # 不用跨线程去新建 Playwright 实例（两个同步实例并存会互相抢事件循环）。
        self._nudge = threading.Event()
        self._frame_count = 0
        self._event_count = 0
        # 原始帧先入队、另起线程解析：Playwright 的消息泵绝不能卡在 gzip+protobuf 上，
        # 不然一帧解析慢一点，后面的帧就全排在 driver 里，弹幕会越看越晚。
        self._rawq: queue.Queue = queue.Queue(maxsize=800)
        self._parser: threading.Thread | None = None
        self._dropped = 0
        self._ws_urls: list[str] = []
        self._webcast_count = 0
        self._storage_state = _load_storage_state()
        self.room_title = ""
        self.anchor_name = ""

    def start(self) -> None:
        self._parser = threading.Thread(target=self._parse_loop, daemon=True)
        self._parser.start()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    # ---- 解析线程：只管把队列里的帧变成事件，不碰浏览器 ----
    def _parse_loop(self) -> None:
        while not self._stop.is_set():
            try:
                raw = self._rawq.get(timeout=0.2)
            except queue.Empty:
                continue
            try:
                for method, msg in iter_messages(raw):
                    ev = to_event(method, msg)
                    if ev:
                        self._event_count += 1
                        self.on_event(ev)
            except Exception as e:
                if self.debug:
                    print(f"[sniper] 帧解析异常: {e}")

    def _run(self) -> None:
        from playwright.sync_api import sync_playwright
        backoff = 3
        # 关键：Playwright 的同步实例在**一个线程里只能进一次**。
        # 以前写成 `while: with sync_playwright():`，只要重连过一次（换直播间 / 网络抖一下），
        # 第二次进入就会把 greenlet 事件循环搞死 —— 表现正是「换完房间只收到握手那一帧，
        # 之后页面彻底不动，弹幕卡住」。现在外层只开一次，重连只重建 browser/page。
        with sync_playwright() as p:
            self._pw = p
            while not self._stop.is_set():
                try:
                    kwargs = dict(headless=self.headless, args=[
                        "--disable-blink-features=AutomationControlled",
                        "--mute-audio", "--autoplay-policy=no-user-gesture-required",
                    ])
                    self._browser = _launch_any(p, kwargs)
                    self._ws_urls.clear()
                    self._webcast_count = 0
                    ctx_kwargs = dict(
                        user_agent=UA, viewport={"width": 1280, "height": 800},
                        locale="zh-CN", timezone_id="Asia/Shanghai")
                    if self._storage_state:
                        ctx_kwargs["storage_state"] = self._storage_state
                    ctx = self._browser.new_context(**ctx_kwargs)
                    self._page = ctx.new_page()
                    self._page.on("websocket", self._attach_ws)
                    self._page.goto(f"https://live.douyin.com/{self.web_rid}",
                                    timeout=60000, wait_until="domcontentloaded")
                    self.room_title = self._safe_title()

                    # 等页面把状态渲染出来（过早只会看到「直播加载中」）
                    if not self._wait_ready():
                        # 下播也尽量把主播名取到手，界面上别显示占位
                        self.anchor_name = self._safe_anchor()
                        if self.anchor_name:
                            self.on_event({"type": "room", "name": self.anchor_name,
                                           "ts": int(time.time())})
                        print("[sniper] 主播当前未开播，等待开播…（每 60 秒查一次）")
                        self.on_event({"type": "live", "on": False, "ts": int(time.time())})
                        # 下播时不留浏览器，避免长时间空转把 headless 拖崩
                        try:
                            self._browser.close()
                        except Exception:
                            pass
                        if not self._wait_live_http():
                            break
                        self.on_event({"type": "live", "on": True, "ts": int(time.time())})
                        print("[sniper] 检测到开播，重新进入直播间")
                        continue

                    self.anchor_name = self._safe_anchor()
                    if self.anchor_name:
                        self.on_event({"type": "room", "name": self.anchor_name,
                                       "ts": int(time.time())})
                    print(f"[sniper] 已进入直播间 {self.web_rid} | {self.anchor_name or self.room_title}")
                    self.on_event({"type": "live", "on": True, "ts": int(time.time())})
                    last_live_check = time.time()
                    # 看门狗：在线人数 / 进场帧是持续不断的，60 秒一帧都没有只可能是通道断了，
                    # 不能干等着（这就是之前「页面不动、弹幕卡住」的另一种表现）。
                    last_move = time.time()
                    last_frames = self._frame_count
                    entered_at = time.time()
                    diag_shown = False
                    while not self._stop.is_set():
                        # 120ms 轮询一次：Playwright 的同步 API 只在调用时才把驱动层
                        # 攒着的帧交付上来。原来写 1000ms，帧要在管道里干等到下一轮，
                        # 实测帧交付间隔中位 605ms、最坏 2 秒 —— 这就是弹幕滞后的主因。
                        self._page.wait_for_timeout(120)
                        # 兜底：真直播间几秒内就会建 webcast WSS。标题若仍是通用首页，
                        # 说明房间号无效，重连多少次都没用，直接停。
                        # 标题正常却没 WSS（网络/风控）只打一次诊断，交给原有重连逻辑。
                        if (self._webcast_count == 0 and not diag_shown
                                and time.time() - entered_at > 25):
                            diag_shown = True
                            if self._looks_like_home():
                                raise InvalidRoom(self._diagnose())
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
                        # 每 30 秒确认一次还在播，下播了就回外层等开播
                        if time.time() - last_live_check > 30:
                            last_live_check = time.time()
                            if not self._live_on():
                                raise RuntimeError("直播已结束")
                    self._browser.close()
                except Exception as e:
                    if self._stop.is_set():
                        break
                    if isinstance(e, InvalidRoom):
                        print(str(e), flush=True)
                        print("[sniper] 已停止重连：这个房间号重连多少次都进不去，"
                              "请填正确的直播间号（服务端 POST /room 可运行时切换）。", flush=True)
                        self.on_event({"type": "room", "name": "", "rid": self.web_rid,
                                       "ts": int(time.time())})
                        self.on_event({"type": "live", "on": False,
                                       "msg": "未能进入直播间：检查房间号；私密直播间需先运行 --login 登录",
                                       "ts": int(time.time())})
                        try:
                            if self._browser:
                                self._browser.close()
                        except Exception:
                            pass
                        break
                    if "直播已结束" in str(e):
                        print("[sniper] 检测到下播，转为等待开播")
                        self.on_event({"type": "live", "on": False, "ts": int(time.time())})
                    else:
                        print(f"[sniper] 异常: {e}；{backoff}s 后重连")
                    try:
                        if self._browser:
                            self._browser.close()
                    except Exception:
                        pass
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 30)
                else:
                    backoff = 3

    def _safe_title(self) -> str:
        try:
            return self._page.title() or ""
        except Exception:
            return ""

    def _looks_like_home(self) -> bool:
        """是不是被回落到了抖音直播通用首页（房间号无效的典型表现）。

        只看标题：无效号是「抖音直播电脑版_抖音直播网页版入口_抖音直播」，
        有效号是「XX的抖音直播间 - 抖音直播」。
        千万不能用 body 里的备案号/导航词判 —— 所有抖音页面底部都有备案，
        加载早期 body 也一律是通用骨架，会把正常直播间一起误杀。
        """
        title = self._safe_title()
        return bool(title) and any(m in title for m in HOME_MARKS[:2])

    def _diagnose(self) -> str:
        """把页面真实状态打成一段人话，客户机截屏就能定位。"""
        try:
            title = self._page.title() or ""
        except Exception:
            title = ""
        try:
            txt = " | ".join(self._page.inner_text("body")[:300].splitlines())
        except Exception:
            txt = ""
        return (
            f"[诊断] 房间号 {self.web_rid} 没有进入直播间。\n"
            f"[诊断] 页面标题: {title[:80]}\n"
            f"[诊断] 页面文字: {txt[:220]}\n"
            f"[诊断] WebSocket {len(self._ws_urls)} 条，其中 webcast "
            f"{self._webcast_count} 条，帧 {self._frame_count}。\n"
            "[诊断] 原因: 没有建立 webcast 弹幕通道。可能是 (1) 房间号无效/已下播；"
            "(2) 直播间是私密/仅粉丝可见，而本程序当前是未登录游客；"
            "(3) 网络、代理或风控拦了 WSS。\n"
            "[诊断] 私密直播间: 运行「python -m server.douyin_fetcher --login」扫码登录，"
            "把生成的 login_state.json 拷到项目根目录后重启。\n"
            "[诊断] 房间号: 从直播分享链接里取 live.douyin.com/ 后面那串纯数字，"
            "改房间号后重试。"
        )

    def _wait_ready(self, timeout: int = 18) -> bool:
        """轮询到页面状态可判定为止：True=直播中，False=已结束/未开播。"""
        end = time.time() + timeout
        while time.time() < end and not self._stop.is_set():
            if self._looks_like_home():
                raise InvalidRoom(self._diagnose())
            try:
                txt = self._page.inner_text("body")[:400]
            except Exception:
                txt = ""
            if "直播已结束" in txt or "已经结束" in txt or "暂未开播" in txt:
                return False
            if any(k in txt for k in ("在线观众", "本场点赞", "小时榜", "人气榜")):
                return True
            try:
                self._page.wait_for_timeout(600)
            except Exception:
                return True
        if self._looks_like_home():
            raise InvalidRoom(self._diagnose())
        return self._live_on()

    def _live_on(self) -> bool:
        """页面是否处于直播中（下播后抖音只留「直播已结束」）。"""
        try:
            txt = self._page.inner_text("body")[:400]
        except Exception:
            return True
        return not ("直播已结束" in txt or "已经结束" in txt or "暂未开播" in txt)

    def _live_by_http(self) -> bool | None:
        """用普通 HTTP 查开播状态（不占浏览器）。None=判断不出来。

        页面 SSR 数据里 roomStore.roomInfo.room.status：2=直播中，4=已结束。
        """
        try:
            import requests
            r = requests.get(f"https://live.douyin.com/{self.web_rid}", timeout=15,
                             headers={"User-Agent": UA, "Referer": "https://live.douyin.com/"})
            h = r.text.replace('\\"', '"')
        except Exception:
            return None
        for pat in (r'"roomInfo"\s*:\s*\{[^{}]{0,400}?"room"\s*:\s*\{[^{}]{0,400}?"status"\s*:\s*(\d)',
                    r'"room"\s*:\s*\{[^{}]{0,400}?"status"\s*:\s*(\d)'):
            m = re.search(pat, h)
            if m:
                return m.group(1) == "2"
        return None

    def _wait_live_http(self, period: int = 60) -> bool:
        """不开浏览器，定时用 HTTP 轮询，直到开播。返回 False 表示被要求停止。"""
        while not self._stop.is_set():
            for _ in range(period):
                if self._stop.is_set():
                    return False
                time.sleep(1)
            if self._live_by_http() is True:
                return True
        return False

    # 页面上的通用 UI 词，绝不可能当主播名
    _BAD_NAMES = (
        "精选", "推荐", "热门", "直播", "直播已结束", "已结束", "关注", "更多",
        "小时榜", "人气榜", "在线观众", "进入直播间", "登录", "下载", "打开",
        "客户端", "抖音", "分享", "举报", "礼物", "排行榜", "读屏", "静音",
        "原画", "加载", "本场点赞", "浏览器限制", "点击", "未知",
        # 抖音直播通用首页的导航词，以前「朋友」被兜底当成了主播名
        "朋友", "我的", "放映厅", "短剧", "小游戏", "AI抖音", "下载抖音精选",
        "开启读屏标签", "读屏标签已关闭", "抖音直播电脑版", "网页版入口",
    )

    def _ok_name(self, name: str) -> bool:
        name = (name or "").strip()
        if not (1 < len(name) <= 20):
            return False
        if name.startswith(("webcast", "http", "$", "点击", "直播", "{", "[")):
            return False
        if "$" in name or "undefined" in name.lower() or "{" in name:
            return False
        return not any(w in name for w in self._BAD_NAMES)

    def _safe_anchor(self) -> str:
        """从页面数据里取主播昵称（失败就返回空，前端保留默认值）。"""
        try:
            html = self._page.content()
        except Exception:
            html = ""
        h = html.replace('\\"', '"')
        # 1) 结构化数据里的 owner.nickname 最可靠
        for pat in (r'"owner"\s*:\s*\{[^{}]{0,800}?"nickname"\s*:\s*"([^"]{1,24})"',
                    r'"roomOwner"\s*:\s*\{[^{}]{0,800}?"nickname"\s*:\s*"([^"]{1,24})"',
                    r'"nickname"\s*:\s*"([^"]{1,24})"',
                    r'"nickName"\s*:\s*"([^"]{1,24})"'):
            for m in re.finditer(pat, h):
                if self._ok_name(m.group(1)):
                    return m.group(1).strip()
        # 2) 兜底：页首文字里第一行像人名的
        try:
            txt = self._page.inner_text("body")[:400]
        except Exception:
            return ""
        for line in txt.splitlines():
            line = line.strip()
            if self._ok_name(line):
                return line
        return ""

    def _attach_ws(self, ws) -> None:
        try:
            self._ws_urls.append(ws.url)
        except Exception:
            pass
        if "webcast" not in ws.url:
            if self.debug:
                print(f"[sniper] 忽略 WS: {ws.url[:80]}")
            return
        self._webcast_count += 1
        print(f"[sniper] 弹幕通道已接管 ({ws.url.split('?')[0]})")
        ws.on("framereceived", self._on_frame)

    def _on_frame(self, payload) -> None:
        """消息泵回调：只做一次入队，几微秒就返回。

        以前在这里直接 gzip 解压 + protobuf 解析，帧一多就把消息泵拖住，
        Playwright 只能把后续帧堆在管道里 —— 表现就是弹幕越刷越滞后。
        """
        self._frame_count += 1
        raw = payload if isinstance(payload, (bytes, bytearray)) else str(payload).encode()
        try:
            self._rawq.put_nowait(raw if isinstance(raw, bytes) else bytes(raw))
        except queue.Full:
            # 已经堆积到 800 帧，说明解析确实跟不上：丢掉最旧的一帧换实时性。
            # 弹幕宁可少一条旧的，也不能整体滞后几十秒。
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


# ---------------- 协议直连（备用通道） ----------------

class RawFetcher:
    """直连 wss://webcast*.../webcast/im/push/v2/（可能被 DEVICE_BLOCKED 拒绝）。"""

    def __init__(self, web_rid: str, on_event: Callable[[dict], None], debug: bool = False):
        import hashlib
        import requests
        import websocket
        self.hashlib, self.requests, self.websocket = hashlib, requests, websocket
        self.web_rid = web_rid
        self.on_event = on_event
        self.debug = debug
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": UA, "Referer": "https://live.douyin.com/"})
        self.room_id = ""
        self.uid = str(random.randint(10 ** 18, 10 ** 19 - 1))
        self.ttwid = ""
        self._ws = None
        self._stop = threading.Event()

    def prepare(self) -> bool:
        r = self.session.get(f"https://live.douyin.com/{self.web_rid}", timeout=20)
        html = r.text.replace('\\"', '"')
        self.ttwid = self.session.cookies.get("ttwid", "")
        m = re.search(r'"roomId"\s*:\s*"(\d{10,})"', html)
        m2 = re.search(r'"user_unique_id"\s*:\s*"(\d{10,})"', html)
        if m2:
            self.uid = m2.group(1)
        if not m:
            return False
        self.room_id = m.group(1)
        return True

    def _url(self) -> str:
        tpl = ("live_id=1,aid=6383,version_code=180800,webcast_sdk_version=1.0.14,"
               "room_id={r},sub_room_id=,sub_channel_id=,did_rule=3,"
               "user_unique_id={u},device_platform=web,language=zh")
        sig = self.hashlib.md5((tpl.format(r=self.room_id, u=self.uid) + "webcast").encode()).hexdigest()
        import urllib.parse
        q = {
            "app_name": "douyin_web", "version_code": "180800",
            "webcast_sdk_version": "1.0.14", "compress": "gzip",
            "device_platform": "web", "cookie_enabled": "true",
            "browser_language": "zh-CN", "browser_platform": "Win32",
            "browser_name": "Chrome", "browser_version": "131.0.0.0",
            "live_id": "1", "did_rule": "3", "endpoint": "live_pc",
            "support_wrds": "1", "identity": "audience",
            "need_persist_msg_count": "15", "room_id": self.room_id,
            "signature": sig, "aid": "6383", "user_unique_id": self.uid,
            "language": "zh-CN",
        }
        return ("wss://webcast5-ws-web-lf.douyin.com/webcast/im/push/v2/?"
                + urllib.parse.urlencode(q))

    def run_forever(self) -> None:
        backoff = 2
        while not self._stop.is_set():
            try:
                if not self.room_id and not self.prepare():
                    print("[raw] 取 room_id 失败，30s 后重试")
                    time.sleep(30)
                    continue
                headers = {"User-Agent": UA, "Origin": "https://live.douyin.com"}
                if self.ttwid:
                    headers["Cookie"] = f"ttwid={self.ttwid}"
                self._ws = self.websocket.WebSocketApp(
                    self._url(), header=headers,
                    on_message=self._on_msg, on_error=lambda w, e: print("[raw] 错误:", e),
                    on_close=lambda w, c, r: print("[raw] 关闭", c))
                print("[raw] 尝试直连弹幕通道…")
                self._ws.run_forever(ping_interval=0, skip_utf8_validation=True)
            except Exception as e:
                print("[raw] 异常:", e)
            if self._stop.is_set():
                break
            self.room_id = ""
            time.sleep(backoff)
            backoff = min(backoff * 2, 30)

    def _on_msg(self, ws, message) -> None:
        raw = message if isinstance(message, (bytes, bytearray)) else str(message).encode()
        for method, msg in iter_messages(bytes(raw)):
            ev = to_event(method, msg)
            if ev:
                self.on_event(ev)

    def close(self) -> None:
        self._stop.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass


# ---------------- CLI 调试 ----------------

def _cli() -> None:
    if "--login" in sys.argv:
        _login_flow()
        return
    web_rid = next((a for a in sys.argv[1:] if a.isdigit()), "")

    def show(ev: dict) -> None:
        t = ev["type"]
        if t == "chat":
            print(f"[弹幕] {ev['user']}: {ev['content']}")
        elif t == "enter":
            print(f"[进场] {ev['user']}")
        elif t == "follow":
            print(f"[关注] {ev['user']}")
        elif t == "gift":
            print(f"[礼物] {ev['user']} -> {ev['gift']} x{ev['count']}")
        elif t == "like":
            print(f"[点赞] {ev['user']} x{ev['count']}")
        elif t == "stats":
            print(f"[在线] {ev['online']}")

    if "--raw" in sys.argv:
        if not web_rid:
            print("用法: python -m server.douyin_fetcher <直播间号> --raw")
            return
        RawFetcher(web_rid, show, debug=True).run_forever()
        return
    if not web_rid:
        print("用法: python -m server.douyin_fetcher <直播间号> [--head] [--raw] [--login]")
        return
    s = BrowserSniper(web_rid, show, headless="--head" not in sys.argv, debug=True)
    s.start()
    try:
        while True:
            time.sleep(5)
            print(f"[状态] 帧={s._frame_count} 事件={s._event_count}")
    except KeyboardInterrupt:
        s.close()


def _login_flow() -> None:
    """弹出一个可见浏览器打开抖音直播页，人工扫码登录，把 cookie 存成 login_state.json。

    私密 / 仅粉丝可见直播间需要登录态：把生成的 login_state.json 放到
    项目根目录（或设环境变量 DANMU_LOGIN_STATE 指向它），抓取端会自动加载。
    """
    from playwright.sync_api import sync_playwright
    out = login_state_path()
    print(f"[login] 浏览器即将打开，请在页面里完成登录（扫码/手机号均可）。")
    print(f"[login] 登录成功后回到本控制台按回车保存并退出。")
    with sync_playwright() as p:
        b = _launch_any(p, dict(headless=False, args=["--disable-blink-features=AutomationControlled"]))
        ctx = b.new_context(user_agent=UA, viewport={"width": 1280, "height": 800},
                            locale="zh-CN", timezone_id="Asia/Shanghai")
        page = ctx.new_page()
        page.goto("https://live.douyin.com/", timeout=60000, wait_until="domcontentloaded")
        input("[login] 完成登录后按回车保存登录态… ")
        ctx.storage_state(path=out)
        b.close()
    print(f"[login] 已保存: {out}")


if __name__ == "__main__":
    _cli()
