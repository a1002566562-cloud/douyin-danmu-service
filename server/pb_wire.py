"""极简 protobuf wire-format 解析/编码器（不依赖 protoc / pb2）。

抖音弹幕协议字段号会漂移，所以这里不做 schema 绑定：
把任意二进制按 wire format 解成 {field_no: [value, ...]}，值可能是
int / bytes / 嵌套 dict / str，由调用方按需取值。
"""
from __future__ import annotations

import gzip
from typing import Any

WT_VARINT, WT_64BIT, WT_LEN, WT_32BIT = 0, 1, 2, 5


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        if pos >= len(buf):
            raise ValueError("varint 越界")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint 过长")


def encode_varint(n: int) -> bytes:
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n:
            out.append(b | 0x80)
        else:
            out.append(b)
            return bytes(out)


def _try_text(raw: bytes) -> str | None:
    try:
        s = raw.decode("utf-8")
    except UnicodeDecodeError:
        return None
    if not s:
        return None
    printable = sum(1 for ch in s if ch.isprintable() or ch in "\n\r\t")
    if printable / len(s) < 0.95:
        return None
    return s


def parse(buf: bytes, depth: int = 0) -> dict[int, list[Any]]:
    """解析一层 message，返回 {字段号: [值...]}。"""
    out: dict[int, list[Any]] = {}
    pos, n = 0, len(buf)
    while pos < n:
        try:
            key, pos = _read_varint(buf, pos)
        except ValueError:
            break
        field, wt = key >> 3, key & 0x07
        if field == 0:
            break
        if wt == WT_VARINT:
            try:
                val, pos = _read_varint(buf, pos)
            except ValueError:
                break
        elif wt == WT_64BIT:
            if pos + 8 > n:
                break
            val = int.from_bytes(buf[pos:pos + 8], "little")
            pos += 8
        elif wt == WT_32BIT:
            if pos + 4 > n:
                break
            val = int.from_bytes(buf[pos:pos + 4], "little")
            pos += 4
        elif wt == WT_LEN:
            try:
                ln, pos = _read_varint(buf, pos)
            except ValueError:
                break
            if pos + ln > n:
                break
            raw = buf[pos:pos + ln]
            pos += ln
            val = _decode_len_value(raw, depth)
        else:
            break
        out.setdefault(field, []).append(val)
    return out


def _decode_len_value(raw: bytes, depth: int) -> Any:
    """length-delimited 值：可能是嵌套 message，也可能是字符串/二进制。"""
    # gzip 数据必须先原样返回，否则会被误解析成 message
    if raw[:2] == b"\x1f\x8b":
        return raw
    if depth < 8 and raw:
        try:
            sub = parse(raw, depth + 1)
            if _looks_like_message(sub, raw):
                return sub
        except Exception:
            pass
    txt = _try_text(raw)
    return txt if txt is not None else raw


def _has_http(sub: dict[int, list[Any]]) -> bool:
    for vals in sub.values():
        for v in vals:
            if isinstance(v, str) and v.startswith("http"):
                return True
    return False


def _looks_like_message(sub: dict[int, list[Any]], raw: bytes) -> bool:
    """判断 parse 结果像不像真的 message。

    两条判据：
    1. 字节解不成可打印文本 → 一定是嵌套 message（真 message 里必然夹 0x08/0x0a 类控制字节）
    2. 能解成文本、但解析出来里面有 http 链接 → 仍然是 message

    第 2 条是踩坑补的：Image{1: repeated urlList} 的字节形如
    `\\x0a <len> https://p3-...`，因为 0x0a 正好是换行、长度字节又恰好是可打印 ASCII，
    整段能通过文本检查，于是头像 URL 被当成一个字符串吞掉。之前的版本更糟——
    只把「短 ASCII」排除在外，长 URL 全部被当 message 解析，最后只剩
    `{13: [116]}` 这种残渣（'h'=0x68 被读成 field 13、't'=0x74 读成 116）。
    """
    if not sub:
        return False
    if _try_text(raw) is None:
        return True
    return _has_http(sub)


def get(d: dict[int, list[Any]] | Any, field: int, idx: int = 0, default: Any = None) -> Any:
    if not isinstance(d, dict):
        return default
    vals = d.get(field)
    if not vals or len(vals) <= idx:
        return default
    return vals[idx]


def get_str(d: dict[int, list[Any]] | Any, field: int, idx: int = 0, default: str = "") -> str:
    v = get(d, field, idx)
    if isinstance(v, str):
        return v
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8", "ignore")
        except Exception:
            return default
    return default


def get_int(d: dict[int, list[Any]] | Any, field: int, idx: int = 0, default: int = 0) -> int:
    v = get(d, field, idx)
    return v if isinstance(v, int) else default


def maybe_gunzip(payload: bytes) -> bytes:
    if payload[:2] == b"\x1f\x8b":
        try:
            return gzip.decompress(payload)
        except Exception:
            return payload
    return payload


def walk_strings(node: Any, out: list[str] | None = None, depth: int = 0) -> list[str]:
    """深度收集所有字符串，用于字段号漂移时兜底提取。"""
    if out is None:
        out = []
    if depth > 10:
        return out
    if isinstance(node, str):
        if node:
            out.append(node)
    elif isinstance(node, dict):
        for vals in node.values():
            for v in vals:
                walk_strings(v, out, depth + 1)
    return out


# ---------- 编码（心跳 / ACK） ----------

def encode_field_len(field: int, data: bytes) -> bytes:
    return encode_varint((field << 3) | WT_LEN) + encode_varint(len(data)) + data


def encode_field_varint(field: int, value: int) -> bytes:
    return encode_varint((field << 3) | WT_VARINT) + encode_varint(value)


def make_push_frame(payload: bytes = b"", log_id: int = 0,
                    payload_type: str = "", seq_id: int = 0) -> bytes:
    """构造 PushFrame。心跳 = payload_type "hb"；ACK = payload_type "ack" + internalExt。"""
    buf = b""
    if seq_id:
        buf += encode_field_varint(1, seq_id)
    if log_id:
        buf += encode_field_varint(2, log_id)
    if payload:
        buf += encode_field_len(5, payload)
    if payload_type:
        buf += encode_field_len(6, payload_type.encode())
    return buf
