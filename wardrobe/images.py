"""可信图片来源、字节/像素限制、解码校验、安全重编码与托管存储。

整改要点：
- R5/G3：`prepare()` 只做“处理 + 临时文件”，不做事务外去重判定——旧图
  可能在处理期间被合法删除，去重在最终事务内按 digest 重新判定；
  服务层在单事务写入后由调用方清理未采用文件，失败/取消补偿只清理
  本次登记的文件。`recover_untracked_files()` 受控清理托管目录中
  “有文件无 DB 行”的残留（仅扫描本插件生成的 managed 键形态，不逃根）。
- G4：临时写入/rename 经 shield 提交，取消后调用方先等待后台完成再补偿，
  晚完成的写入逃不掉清理清单；解码工人许可与线程绑定（工人完成前
  槽位不释放）；停用受控结算活跃工人。
- R8/G5：HTTPS 用 `ssl.create_default_context()` 建立真 TLS——连接固定到
  已验证 IP，同时以原始 host 作 `server_hostname` 完成证书/主机校验与
  SNI；https 拒绝降级 http；重定向逐跳复核；DNS 全部经事件循环
  （getaddrinfo 无同步调用逃逸）；chunked 解析严格（hex 正则、定长短读
  拒绝、块尾 CRLF、trailer 拒绝、块数/预算上限）；下载准入为共享实例
  BoundGate（有限工人+有限队列，raw 读取前置）。
- R9/G6：解码前先做格式白名单（Image.open(formats=...) 使白名单先于
  Pillow 插件 header 解析生效）、header 尺寸/像素/动画检查；base64 在
  解码前限编码长度；本地读取先 stat 限额再限长读取；解码准入为共享
  BoundGate（image_concurrency 工人 + image_queue_capacity 队列，满时
  busy，raw 读取在准入之后）；重编码用全新 Image 对象，不继承
  info/ICC/EXIF；暂存/发布中的图片有进程内 pending 预算（单进程边界）。

MIT 复用片段：_vision_thumb_size 几何逻辑来自
Inoryu7z/astrbot_plugin_wardrobe @ da9e62a14b814aefdfae6a4a4417b469dd59cafd
core/image_store.py::_vision_thumb_size（132–144 行）及关联常量（14–15 行），
改为模块级私有 helper；适配差异：输入先经正整数校验并受本项目上限约束。
原始版权与 MIT 许可见 THIRD_PARTY_NOTICES.md。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import io
import ipaddress
import os
import re
import socket
import ssl
import uuid
from pathlib import Path
from urllib.parse import urlparse

from PIL import Image, ImageOps, UnidentifiedImageError

from .config import WardrobeConfig
from .models import (
    HandleInvalid,
    ImageRejected,
    SourceHandle,
    utc_now_iso,
)

ALLOWED_MIME = {"image/jpeg": "jpg", "image/png": "png", "image/webp": "webp"}
# Pillow Image.open 的 formats 参数：白名单之外的格式插件（含 FITS/PSD
# 等）在 header 解析前即不可达（G6）
_ALLOWED_FORMATS = ("JPEG", "PNG", "WEBP")
_HEADER_LIMIT = 64 * 1024          # HTTP 头部读取上限
_CHUNK_SIZE_LIMIT = 1024 * 1024    # 单个 chunk 声明上限
_MAX_REDIRECTS = 5
_TOTAL_DEADLINE_MARGIN = 1.0       # 总时限之外的缓冲
_CHUNK_HEX_RE = re.compile(rb"^[0-9a-fA-F]+$")


class _BoundGate:
    """共享准入闸门：有限工人 + 有限等待队列（G5/G6）。

    队列满时立即 busy 拒绝（在读取任何 raw 字节之前生效）；等待者取消
    时从队列退出，不占用工人槽位。
    """

    def __init__(self, capacity: int, queue_capacity: int) -> None:
        self._cap = max(1, capacity)
        self._qcap = max(0, queue_capacity)
        self._active = 0
        self._waiting = 0
        self._cond = asyncio.Condition()

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return self._waiting

    async def __aenter__(self) -> "_BoundGate":
        async with self._cond:
            if self._active < self._cap:
                self._active += 1
                return self
            if self._waiting >= self._qcap:
                raise ImageRejected("并发处理队列已满，请稍后重试")
            self._waiting += 1
            try:
                await self._cond.wait_for(
                    lambda: self._active < self._cap)
            except BaseException:
                self._waiting = max(0, self._waiting - 1)
                raise
            self._waiting -= 1
            self._active += 1
            return self

    async def __aexit__(self, *exc) -> None:
        async with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()


class _RequestBudget:
    """I5：单请求 processed 字节总额记账（与全服务 pending 预算分列）。

    charge(n) 必须在对应物理写盘**之前**原子扣减：超过请求上限即
    ImageRejected（随后整批拒绝）；并发 prepare 的扣减经条件锁串行化，
    不允许"先超额写盘、后拒绝"。
    """

    def __init__(self, cap: int) -> None:
        self._cap = max(1, int(cap))
        self._used = 0
        self._cond = asyncio.Condition()

    @property
    def used(self) -> int:
        return self._used

    async def charge(self, n: int) -> None:
        n = max(0, int(n))
        async with self._cond:
            if self._used + n > self._cap:
                raise ImageRejected(
                    "本批图片重编码输出超过每请求总额上限，已整批拒绝")
            self._used += n


async def _settled_worker(task: asyncio.Task):
    """旧语义兼容保留的模块级工具：等待真实工人完成再传播取消（G4→H5）。

    H5 起 prepare 内部改用不可外部取消的后台任务 + 后台结算，本函数
    不再被产品路径调用；保留供既有引用与行为对拍。
    """
    try:
        return await asyncio.shield(task)
    except asyncio.CancelledError:
        try:
            await task
        except BaseException:
            pass
        raise


# ---------------------------------------------------------------------------
# MIT 批准复用片段（纯几何逻辑）
# ---------------------------------------------------------------------------

# 原上游常量：短边目标 768、长边兜底 1536（视觉缩略图用途，不放大只缩小）
_VISION_THUMB_SHORT_EDGE = 768
_VISION_THUMB_MAX_LONG_EDGE = 1536


def _checked_positive_int(value: object, name: str, upper: int) -> int:
    """复用片段的前置校验：必须是正整数且在本项目上限内。"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ImageRejected(f"尺寸字段 {name} 必须是整数")
    if value <= 0 or value > upper:
        raise ImageRejected(f"尺寸字段 {name} 超出允许范围")
    return value


def vision_thumb_size(width: int, height: int) -> tuple[int, int]:
    """按短边缩放（不放大），再用长边上限兜底。

    几何逻辑复自上游 _vision_thumb_size；调用前须先校验宽高为正整数。
    """
    short = min(width, height)
    scale = 1.0
    if short > _VISION_THUMB_SHORT_EDGE:
        scale = _VISION_THUMB_SHORT_EDGE / short
    w = max(1, int(width * scale))
    h = max(1, int(height * scale))
    if max(w, h) > _VISION_THUMB_MAX_LONG_EDGE:
        s2 = _VISION_THUMB_MAX_LONG_EDGE / max(w, h)
        w = max(1, int(w * s2))
        h = max(1, int(h * s2))
    return w, h


# ---------------------------------------------------------------------------
# 网络地址安全（R8）
# ---------------------------------------------------------------------------

def _is_forbidden_ip(ip: str) -> bool:
    addr = ipaddress.ip_address(ip)
    return (
        addr.is_private or addr.is_loopback or addr.is_link_local
        or addr.is_multicast or addr.is_reserved or addr.is_unspecified
        or addr in ipaddress.ip_network("169.254.169.254/32")
        or addr in ipaddress.ip_network("fd00:ec2::254/128")
    )


def resolve_remote_target(url: str) -> tuple[str, int, str]:
    """解析并校验远程 URL；返回 (host, port, normalized_url)。

    G5：只做 scheme/host/port 校验，不做同步 DNS——socket.getaddrinfo 是
    阻塞调用，会阻塞事件循环且逃出总 deadline；DNS 统一走
    _resolve_addresses（事件循环 executor，可被总 deadline 取消）。
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ImageRejected("远程图片仅允许 http/https")
    host = parsed.hostname
    if not host:
        raise ImageRejected("远程图片缺少主机名")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    if port not in (80, 443):
        raise ImageRejected("远程图片仅允许 80/443 端口")
    return host, port, url


async def _resolve_addresses(host: str, port: int) -> set[str]:
    """事件循环内的 DNS 解析（executor 线程执行 getaddrinfo）。

    等待受调用方的总 deadline（asyncio.timeout）覆盖：超时/取消时等待
    被打断，不再阻塞事件循环（G5）。
    """
    loop = asyncio.get_running_loop()
    infos = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    return {info[4][0] for info in infos}


async def _read_bounded(reader: asyncio.StreamReader, n: int,
                        timeout_s: float) -> bytes:
    """定长读取：短读（EOF 截断）受控拒绝；禁止 read(-1)/负数读取。"""
    if n <= 0:
        raise ImageRejected("非法读取长度")
    try:
        data = await asyncio.wait_for(reader.readexactly(n), timeout=timeout_s)
    except asyncio.IncompleteReadError as exc:
        raise ImageRejected("响应体在完成前截断") from exc
    return data


async def _read_headers(reader: asyncio.StreamReader, timeout_s: float) -> bytes:
    """有界读取 HTTP 头（不超过 _HEADER_LIMIT）；截断转受控拒绝。"""
    try:
        data = await asyncio.wait_for(reader.readuntil(b"\r\n\r\n"),
                                      timeout=timeout_s)
    except asyncio.IncompleteReadError as exc:
        raise ImageRejected("响应在头部完成前截断") from exc
    except asyncio.LimitOverrunError as exc:
        raise ImageRejected("HTTP 头部超过上限") from exc
    if len(data) > _HEADER_LIMIT:
        raise ImageRejected("HTTP 头部超过上限")
    return data


async def _read_chunk_size_line(reader: asyncio.StreamReader,
                                timeout_s: float) -> bytes:
    try:
        return await asyncio.wait_for(reader.readuntil(b"\r\n"),
                                      timeout=timeout_s)
    except asyncio.IncompleteReadError as exc:
        raise ImageRejected("响应在 chunk 完成前截断") from exc


def _parse_chunk_size(size_line: bytes, remaining_budget: int) -> int:
    """G5：chunk 长度行严格解析——hex 正则、禁止负数/符号、预算核对。"""
    token = size_line.strip().split(b";")[0]
    if not token or not _CHUNK_HEX_RE.match(token):
        raise ImageRejected("chunked 长度非法（负数/符号/非 hex）")
    chunk_size = int(token, 16)
    if chunk_size > _CHUNK_SIZE_LIMIT or chunk_size > remaining_budget:
        raise ImageRejected("单个 chunk 超过上限")
    return chunk_size


async def fetch_remote_bytes(url: str, max_bytes: int, timeout_s: float) -> bytes:
    """受限远程下载（R8/G5）：真 TLS + 固定 IP + 总 deadline + 严格有界解析。

    - DNS 解析结果与实际连接地址逐次复核（连接绑定已验证 IP，防重绑定）；
    - HTTPS 用默认信任的 ssl context 并以原始 host 做 server_hostname，
      证书/主机校验与 SNI 同时成立；
    - https 源拒绝降级到 http；重定向不得更换主机；
    - 整个函数（DNS/连接/TLS/读写/重定向）受 timeout_s 总时限（调用方
      可再以含排队时间的更大 deadline 包裹）；
    - chunked：长度行 hex 严格、块尾 CRLF 校验、trailer 拒绝、块数与
      剩余预算核对、定长短读拒绝；Content-Length 与 Transfer-Encoding
      冲突拒绝。
    """
    total_budget = timeout_s
    async with asyncio.timeout(total_budget + _TOTAL_DEADLINE_MARGIN):
        first_parsed = urlparse(url)
        https_origin = first_parsed.scheme == "https"
        host, port, current_url = resolve_remote_target(url)
        try:
            resolved = await _resolve_addresses(host, port)
        except OSError as exc:
            raise ImageRejected(f"远程主机解析失败: {host}") from exc
        if not resolved or any(_is_forbidden_ip(a) for a in resolved):
            raise ImageRejected("远程主机地址不允许")
        redirects = 0
        for _hop in range(_MAX_REDIRECTS + 1):
            parsed = urlparse(current_url)
            if parsed.scheme not in ("http", "https"):
                raise ImageRejected("重定向仅允许 http/https")
            if https_origin and parsed.scheme != "https":
                raise ImageRejected("拒绝 HTTPS→HTTP 降级重定向")
            hop_host = parsed.hostname or ""
            hop_port = parsed.port or (443 if parsed.scheme == "https" else 80)
            if hop_port not in (80, 443):
                raise ImageRejected("重定向仅允许 80/443 端口")
            if hop_host != host:
                raise ImageRejected("重定向改变主机，未授权")
            try:
                now_resolved = await _resolve_addresses(hop_host, hop_port)
            except OSError as exc:
                raise ImageRejected("重定向主机解析失败") from exc
            if not now_resolved or any(_is_forbidden_ip(a) for a in now_resolved):
                raise ImageRejected("远程主机地址变化或不应允许")
            if not now_resolved <= resolved:
                raise ImageRejected("重定向解析结果变化，未授权")
            target_ip = sorted(now_resolved)[0]
            ssl_arg: ssl.SSLContext | bool | None = False
            server_hostname = None
            if parsed.scheme == "https":
                ssl_arg = ssl.create_default_context()  # 校验证书+主机
                server_hostname = hop_host             # SNI 用原始主机
            try:
                reader, writer = await asyncio.open_connection(
                    target_ip, hop_port, ssl=ssl_arg,
                    server_hostname=server_hostname, limit=_HEADER_LIMIT)
            except (OSError, ssl.SSLError, asyncio.TimeoutError,
                    ConnectionError) as exc:
                raise ImageRejected("远程图片连接失败") from exc
            try:
                path = parsed.path or "/"
                if parsed.query:
                    path += "?" + parsed.query
                request = (
                    f"GET {path} HTTP/1.1\r\n"
                    f"Host: {hop_host}\r\n"
                    "User-Agent: astrbot-plugin-character-wardrobe/0.2\r\n"
                    "Accept: image/*\r\n"
                    "Connection: close\r\n\r\n"
                ).encode("ascii")
                writer.write(request)
                await writer.drain()
                header_data = await _read_headers(reader, timeout_s)
                status_line = header_data.split(b"\r\n", 1)[0].decode(
                    "latin-1")
                parts = status_line.split(" ")
                if len(parts) < 2 or not parts[1].isdigit():
                    raise ImageRejected("远程响应格式错误")
                status = int(parts[1])
                headers: dict[str, str] = {}
                for line in header_data.split(b"\r\n")[1:]:
                    if b":" in line:
                        k, _, v = line.decode("latin-1").partition(":")
                        headers[k.strip().lower()] = v.strip()
                if status in (301, 302, 303, 307, 308):
                    location = headers.get("location")
                    if not location:
                        raise ImageRejected("重定向缺少目标")
                    redirects += 1
                    if redirects > _MAX_REDIRECTS:
                        raise ImageRejected("重定向次数过多")
                    current_url = location if "://" in location else (
                        f"{parsed.scheme}://{hop_host}{location}")
                    continue
                if status != 200:
                    raise ImageRejected(
                        f"远程图片下载失败（HTTP {status}）")
                data = await _read_bounded_body(reader, headers, max_bytes,
                                                timeout_s)
                if not data:
                    raise ImageRejected("空响应体")
                return data
            finally:
                writer.close()
        raise ImageRejected("重定向次数过多")


async def _read_bounded_body(reader: asyncio.StreamReader,
                             headers: dict[str, str], max_bytes: int,
                             timeout_s: float) -> bytes:
    """严格有界正文读取：chunked 逐块核验 / Content-Length 一致性。"""
    is_chunked = "chunked" in headers.get("transfer-encoding", "").lower()
    declared = headers.get("content-length")
    if is_chunked and declared is not None:
        raise ImageRejected("Content-Length 与 Transfer-Encoding 冲突")
    if declared is not None and not declared.isdigit():
        raise ImageRejected("Content-Length 非法")
    if declared is not None and int(declared) > max_bytes:
        raise ImageRejected("远程图片超过字节上限")
    chunks: list[bytes] = []
    total = 0
    if is_chunked:
        max_chunks = max_bytes // 3 + 16   # 最小块 1 字节 + CRLF 的预算上界
        read_chunks = 0
        while True:
            size_line = await _read_chunk_size_line(reader, timeout_s)
            chunk_size = _parse_chunk_size(size_line, max_bytes - total)
            if chunk_size == 0:
                terminator = await _read_chunk_size_line(reader, timeout_s)
                if terminator.strip():
                    raise ImageRejected("chunked trailer 未被接受")
                break
            read_chunks += 1
            if read_chunks > max_chunks:
                raise ImageRejected("chunk 数量超过预算")
            block = await _read_bounded(reader, chunk_size + 2, timeout_s)
            if block[-2:] != b"\r\n":
                raise ImageRejected("chunk 块尾缺少 CRLF")
            total += chunk_size
            if total > max_bytes:
                raise ImageRejected("远程图片超过字节上限")
            chunks.append(block[:-2])
    else:
        # CL 模式为“至多”读取（可短读，EOF 终止）；截断以 declared 一致性核验
        while True:
            chunk = await asyncio.wait_for(reader.read(65536),
                                           timeout=timeout_s)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise ImageRejected("远程图片超过字节上限")
            chunks.append(chunk)
        if declared is not None and total != int(declared):
            raise ImageRejected("响应体与 Content-Length 不一致")
    return b"".join(chunks)


# ---------------------------------------------------------------------------
# 解码与重编码（R9）
# ---------------------------------------------------------------------------

def decode_and_validate(data: bytes, config: WardrobeConfig) -> Image.Image:
    """解码验证：白名单先于 Pillow 插件 header 解析，再加载并复核（R9/G6）。

    Image.open(formats=...) 限定 Pillow 只尝试 JPEG/PNG/WebP 的识别插件——
    FITS/PSD 等其它格式的 header 解析（插件 _open）在此之前即不可达，
    而不是"先无限制打开再读 format 拒绝"。
    """
    if not data:
        raise ImageRejected("空图片数据")
    header = data[:512]
    if header.lstrip().startswith((b"<?xml", b"<svg")):
        raise ImageRejected("SVG 等主动内容不作为图片资产")
    try:
        img = Image.open(io.BytesIO(data), formats=list(_ALLOWED_FORMATS))
    except (UnidentifiedImageError, OSError, ValueError,
            binascii.Error, Image.DecompressionBombError) as exc:
        raise ImageRejected("图片解码失败或格式不受支持") from exc
    fmt = (img.format or "").upper()
    if fmt not in _ALLOWED_FORMATS:
        raise ImageRejected(f"图片格式 {fmt or '未知'} 不受支持")
    # load 前的头部尺寸/像素检查（Pillow 解析头部即得 size）
    width = _checked_positive_int(img.width, "width", config.max_long_edge)
    height = _checked_positive_int(img.height, "height", config.max_long_edge)
    if width * height > config.max_megapixels * 1_000_000:
        raise ImageRejected("图片像素超过上限")
    frames = getattr(img, "n_frames", 1)
    if frames and frames > 1:
        raise ImageRejected("动画图片统一拒绝；如需使用请上传静态帧")
    try:
        img.load()
    except (OSError, ValueError, Image.DecompressionBombError) as exc:
        raise ImageRejected("图片数据损坏或超限") from exc
    # load 后复核
    width = _checked_positive_int(img.width, "width", config.max_long_edge)
    height = _checked_positive_int(img.height, "height", config.max_long_edge)
    if width * height > config.max_megapixels * 1_000_000:
        raise ImageRejected("图片像素超过上限")
    return img


def reencode_static(img: Image.Image, config: WardrobeConfig) -> tuple[bytes, str, int, int]:
    """重编码：全新 Image 对象（R9：不继承 info/ICC/EXIF）。"""
    width = _checked_positive_int(img.width, "width", config.max_long_edge)
    height = _checked_positive_int(img.height, "height", config.max_long_edge)
    if width * height > config.max_megapixels * 1_000_000:
        raise ImageRejected("图片像素超过上限")
    with ImageOps.exif_transpose(img) as oriented:
        if oriented.mode == "RGBA":
            mode, fmt, mime = "RGBA", "PNG", "image/png"
        else:
            mode, fmt, mime = "RGB", "JPEG", "image/jpeg"
        # 全新画布 + 逐像素搬运：info/ICC/EXIF 一律不携带
        clean = Image.new(mode, oriented.size)
        clean.paste(oriented.convert(mode))
        buf = io.BytesIO()
        save_kwargs: dict = {"quality": 90} if fmt == "JPEG" else {}
        clean.save(buf, format=fmt, **save_kwargs)
        return buf.getvalue(), mime, clean.width, clean.height


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# 图片资产服务
# ---------------------------------------------------------------------------

class _StagedMove:
    """一次 暂存→发布 rename 的真实完成追踪（I4）。

    task_done()：成功路径等待真实完成（rename 失败/等待者取消照常传播，
    但等待者取消不取消真实工人）；wait_settled()：补偿/未知分支等待
    真实结算（吞掉 rename 失败）。两者都用 shield 循环——多次取消绝不
    把真实工人当已结束。
    """

    def __init__(self, task: asyncio.Task, temp_path: Path,
                 final_path: Path) -> None:
        self.task = task
        self.temp_path = temp_path
        self.final_path = final_path

    async def task_done(self) -> None:
        """成功路径等待真实完成；等待者取消不取消真实工人。

        取消语义保留：真实完成（或失败）后再向上传播等待者的取消——
        请求按取消进入补偿，真实写入的最终落点由补偿所有者接管，
        不丢归属；等待期间多次取消同样只继续等待真实结算。
        """
        cancelled = False
        while not self.task.done():
            try:
                await asyncio.shield(self.task)
            except asyncio.CancelledError:
                cancelled = True
                continue
        try:
            await self.task
        except asyncio.CancelledError:
            cancelled = True
        except BaseException:
            if not cancelled:
                raise
        if cancelled:
            raise asyncio.CancelledError

    async def wait_settled(self) -> None:
        while not self.task.done():
            try:
                await asyncio.shield(self.task)
            except asyncio.CancelledError:
                continue
            except BaseException:
                return  # rename 失败同样视为已结算（清理按最终状态采样）


class ImageAssetService:
    """托管图片资产的处理、读取与受控恢复。

    G4/G5/G6：解码与下载准入均为共享实例 BoundGate（有限工人+有限队列，
    队列满在 raw 读取前 busy 拒绝）；pending 磁盘预算为进程内计数
    （单进程边界，多进程部署不覆盖——见 TECHNICAL.md）。
    """

    def __init__(self, root: Path, repository, config: WardrobeConfig,
                 trusted_roots: list[Path] | None = None) -> None:
        self.root = Path(root).resolve()
        self.assets_root = self.root / "assets"
        self.tmp_root = self.root / "tmp"
        # 允许读取本地图片文件的根：插件数据根 + 管理员显式登记的可信媒体根
        self.trusted_roots = [self.root] + [Path(p).resolve() for p in (trusted_roots or [])]
        self._repo = repository
        self._config = config
        self._decode_gate = _BoundGate(config.image_concurrency,
                                       config.image_queue_capacity)
        self._download_gate = _BoundGate(config.download_concurrency,
                                         config.image_queue_capacity)
        self._pending_cap = config.pending_image_budget_bytes
        # H7：pending 预留按请求记账（rid → 预留字节）；释放幂等且只扣自身，
        # 不能用一个全局计数做减法（会扣掉别的请求的预留）。
        self._pending_reservations: dict[str, int] = {}
        self._pending_lock = asyncio.Lock()
        self._active_workers: set[asyncio.Task] = set()
        # H5/H6：后台结算任务（取消后的清理、许可随工人释放的包装）；
        # close() 等待其完成，停用不遗漏在途文件操作。
        self._settlements: set[asyncio.Task] = set()

    # ---- 生命周期（G4/H6：停用受控结算活跃工人与后台结算） -------------

    async def close(self) -> None:
        """等待仍在运行的解码/下载工人与后台结算任务全部完成后返回。"""
        while self._active_workers or self._settlements:
            waiting = list(self._active_workers | self._settlements)
            if not waiting:
                break
            await asyncio.wait(waiting, return_when=asyncio.ALL_COMPLETED)
            # 等待期间可能登记新的结算任务；循环直至两组皆空
            leftover = list(self._active_workers | self._settlements)
            if not leftover:
                break

    def _bg(self, coro) -> asyncio.Task:
        """提交后台结算任务（不可被调用方取消），close 会等待其完成。"""
        task = asyncio.ensure_future(coro)
        self._settlements.add(task)
        task.add_done_callback(self._settlements.discard)
        return task

    # ---- pending 磁盘预算（G6/H7，进程内边界、按请求所有权） ------------

    def make_request_budget(self, cap: int) -> "_RequestBudget":
        """I5：请求级 processed 字节总额记账器（物理写盘前扣减）。"""
        return _RequestBudget(cap)

    async def reserve_pending(self, amount: int) -> str | None:
        """为本请求预留 pending 字节，返回预留 id；超上限时返回 None。"""
        amount = max(0, int(amount))
        async with self._pending_lock:
            if sum(self._pending_reservations.values()) + amount > self._pending_cap:
                return None
            rid = uuid.uuid4().hex
            self._pending_reservations[rid] = amount
            return rid

    async def adjust_pending(self, rid: str, actual: int) -> bool:
        """按实际输出字节调整本请求预留（H7：processed 可能大于 raw 上界）。

        actual 大于当前预留时先补足差额（受全服务 pending 上限约束），
        补足失败返回 False（调用方原子拒绝该图）；小于时缩减。
        rid 不存在时幂等返回 True（已释放）。
        """
        actual = max(0, int(actual))
        async with self._pending_lock:
            if rid not in self._pending_reservations:
                return True
            current = self._pending_reservations[rid]
            if actual > current:
                others = sum(self._pending_reservations.values()) - current
                if others + actual > self._pending_cap:
                    return False
            self._pending_reservations[rid] = actual
            return True

    async def release_pending(self, rid: str) -> None:
        """释放本请求预留（幂等：重复释放/未知 rid 均为 no-op）。"""
        async with self._pending_lock:
            self._pending_reservations.pop(rid, None)

    @property
    def pending_bytes(self) -> int:
        return sum(self._pending_reservations.values())

    async def check_role_budget(self, role_id: str) -> None:
        """角色总额预检（最终事务仍是权威；预检避免整批白处理）。"""
        committed = await self._repo.role_image_bytes(role_id)
        pending = self.pending_bytes
        if committed + pending > self._config.role_image_budget_bytes:
            raise ImageRejected("角色图片总额已满；请先清理或联系管理者")

    # ---- 路径安全 -------------------------------------------------------

    def _containment_check(self, path: Path) -> Path:
        resolved = Path(os.path.normpath(str(path)))
        try:
            resolved.relative_to(self.root)
        except ValueError as exc:
            raise ImageRejected("路径逃离托管根，已拒绝") from exc
        return resolved

    def _trusted_root_check(self, path: Path) -> Path:
        resolved = Path(os.path.normpath(str(path)))
        for root in self.trusted_roots:
            try:
                resolved.relative_to(root)
                return resolved
            except ValueError:
                continue
        raise HandleInvalid("本地图片不在允许的媒体根内")

    def role_dir(self, role_id: str) -> Path:
        """角色资产目录：目录名由 namespace+role 派生哈希。"""
        opaque = hashlib.sha256(
            f"{self._repo.namespace}|{role_id}".encode()).hexdigest()[:16]
        return self.assets_root / opaque

    # ---- 句柄获取（R9/G6：准入与预算前置） ------------------------------

    async def read_from_handle(self, handle: SourceHandle) -> bytes:
        """按句柄类型读取原始字节；base64/本地都在动作前做预算检查。

        remote 经共享下载闸门（有限工人+有限队列），排队满时在读取任何
        字节前 busy 拒绝；排队+下载受总 deadline 覆盖。
        """
        if handle.kind == "base64":
            payload = handle.payload or ""
            # 解码前限制编码长度（4/3 膨胀 + 包装余量）
            if len(payload) > self._config.max_image_bytes * 4 // 3 + 16:
                raise ImageRejected("图片超过字节上限")
            try:
                raw = base64.b64decode(payload, validate=True)
            except (binascii.Error, ValueError) as exc:
                raise HandleInvalid("图片句柄 base64 数据无效") from exc
            if len(raw) > self._config.max_image_bytes:
                raise ImageRejected("图片超过字节上限")
            return raw
        if handle.kind == "local":
            path = Path(handle.payload)
            if not path.is_absolute():
                raise HandleInvalid("本地图片路径必须来自可信媒体根")
            real = Path(os.path.realpath(path))
            if real != Path(os.path.normpath(str(path))):
                raise ImageRejected("符号链接路径已拒绝")
            self._trusted_root_check(real)
            if not real.is_file():
                raise HandleInvalid("本地图片不存在")
            size = real.stat().st_size
            if size > self._config.max_image_bytes:
                raise ImageRejected("图片超过字节上限")
            loop = asyncio.get_running_loop()

            def _capped_read() -> bytes:
                with real.open("rb") as fh:
                    data = fh.read(self._config.max_image_bytes + 1)
                return data

            data = await loop.run_in_executor(None, _capped_read)
            if len(data) > self._config.max_image_bytes:
                raise ImageRejected("图片超过字节上限")
            return data
        if handle.kind == "remote":
            timeout = float(self._config.download_timeout_s)
            async with asyncio.timeout(timeout + _TOTAL_DEADLINE_MARGIN):
                async with self._download_gate:
                    return await fetch_remote_bytes(
                        handle.payload, self._config.max_image_bytes, timeout)
        raise HandleInvalid(f"未知图片句柄类型 {handle.kind}")

    # ---- 处理（G3/G4/G6：准入内处理、许可绑定工人、总是落 temp） --------

    async def prepare(self, role_id: str, handle: SourceHandle,
                      charge=None) -> dict:
        """处理图片：准入→读取→解码→重编码→哈希→预算→临时文件。

        H5：读+解码+写盘整体作为一个**不可被调用方取消**的后台任务执行
        （gate 许可与真实工人绑定，取消等待者不提前归还许可）；调用方
        取消时由后台结算任务等待真实完成后清理 temp 并释放预留，任何
        返回路径之前的失败都不遗留 .part。
        H7/I5：预留按请求 rid 记账；重编码输出可能大于 raw 上界，**物理
        写盘之前**先原子补足预留（补足失败即拒绝，不产生任何超额文件），
        再按本请求 processed 总额记账（charge 为 None 表示无请求预算的
        内部直调）。不在事务外判定去重——最终事务内按 digest 重新判定。
        """
        reserved = self._config.max_image_bytes
        rid = await self.reserve_pending(reserved)
        if rid is None:
            raise ImageRejected("暂存中的图片已达到待提交预算上限，请稍后重试")

        async def _work() -> dict:
            tmp_path: Path | None = None
            try:
                async with self._decode_gate:
                    raw = await self.read_from_handle(handle)
                    loop = asyncio.get_running_loop()

                    def _pipeline(data: bytes) -> tuple[bytes, str, int, int]:
                        img = decode_and_validate(data, self._config)
                        return reencode_static(img, self._config)

                    processed, mime, width, height = (
                        await loop.run_in_executor(None, _pipeline, raw))
                # I5：物理写盘前原子调整预留到实际输出字节；不足即拒绝，
                # 不写超额文件（旧实现写盘后才补足，I5 反例已证太迟）
                if not await self.adjust_pending(rid, len(processed)):
                    raise ImageRejected(
                        "图片重编码输出超过待提交预算上限，已拒绝该图片")
                if charge is not None:
                    await charge(len(processed))
                digest = sha256_hex(processed)
                ext = ALLOWED_MIME[mime]
                asset_id = f"asset_{uuid.uuid4().hex}"
                storage_key = f"assets/{self.role_dir(role_id).name}/{asset_id}.{ext}"
                tmp_path = self._containment_check(
                    self.tmp_root / f"{uuid.uuid4().hex}.part")
                tmp_path.parent.mkdir(parents=True, exist_ok=True)
                await asyncio.get_running_loop().run_in_executor(
                    None, tmp_path.write_bytes, processed)
                return {"asset_id": asset_id, "storage_key": storage_key,
                        "sha256": digest, "mime": mime, "width": width,
                        "height": height, "byte_size": len(processed),
                        "temp_path": tmp_path}
            except BaseException:
                # 工人不会被外部取消：失败时自清理已生成的临时文件
                if tmp_path is not None:
                    self.discard_temp(tmp_path)
                raise

        worker = self._bg(_work())
        self._active_workers.add(worker)
        settled = False
        try:
            result = await asyncio.shield(worker)
            settled = True
            result["reservation_id"] = rid
            return result
        except BaseException:
            # 取消或失败：结算责任转后台任务——等待真实工人完成、清理
            # temp、释放本请求预留；多次取消不中断该结算
            async def _finalize():
                try:
                    outcome = await asyncio.shield(worker)
                    if not settled:
                        self.discard_temp(outcome.get("temp_path"))
                except BaseException:
                    pass
                await self.release_pending(rid)

            self._bg(_finalize())
            raise
        finally:
            self._active_workers.discard(worker)

    # ---- 文件就位与清理（G3/G4/I4） ---------------------------------------
    # ---- 文件就位与清理（G3/G4） -----------------------------------------

    async def move_into_place(self, temp_path: Path, final_path: Path) -> None:
        """把已处理完成的临时文件 rename 到最终托管路径（executor 执行）。"""
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(
            None, os.replace, str(temp_path), str(final_path))

    def stage_move(self, temp_path: Path, final_path: Path) -> "_StagedMove":
        """I4：rename 提交为 owned 结算任务（纳入 _settlements）。

        真实 os.replace 在线程完成才算结算；等待者取消（含多次）不会
        取消该任务，close()/stop 等待其真实完成——不存在"future 已取消
        即工人已结束"的误判。
        """
        task = self._bg(self.move_into_place(temp_path, final_path))
        return _StagedMove(task, temp_path, final_path)

    def remove_if_exists(self, path: Path | None) -> bool:
        """真实存在采样删除（I4）：返回是否实际删除，幂等。"""
        if path is None:
            return False
        try:
            if path.exists():
                path.unlink()
                return True
        except OSError:
            pass
        return False

    def remove_gone(self, path: Path | None) -> bool:
        """严格清理原语（J2/J3）：返回文件是否**确认已不存在**。

        删除失败（仍存在）返回 False——调用方据此保持该资源的预留与
        受控待结算状态，不得吞掉失败后释放计数。
        """
        if path is None:
            return True
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass
        try:
            return not path.exists()
        except OSError:
            return False

    def temp_path_of(self, name: str) -> Path:
        """受信临时文件名（uuid.part）→ 受控 tmp 根内路径。"""
        return self._containment_check(self.tmp_root / name)

    async def cleanup_moves(self, moves) -> int:
        """I4：真实 rename 全部结算后，对最终落点与临时路径各做一次真实
        存在采样删除——晚完成的 rename 逃不出清理清单，零残留。"""
        removed = 0
        for move in moves:
            await move.wait_settled()
        for move in moves:
            if self.remove_if_exists(move.final_path):
                removed += 1
            self.remove_if_exists(move.temp_path)
        return removed

    def discard_temp(self, temp_path: Path | None) -> None:
        """清理遗留临时文件（幂等）。"""
        if temp_path is None:
            return
        try:
            if temp_path.exists():
                temp_path.unlink()
        except OSError:
            pass

    # ---- 读取 / 清理 / 受控恢复 -----------------------------------------

    def storage_path(self, storage_key: str) -> Path:
        """受控键 → 实际路径；最终操作前复核 containment。"""
        if not storage_key.startswith("assets/") or ".." in storage_key:
            raise ImageRejected("存储键不合法")
        if storage_key.count("/") != 2:
            raise ImageRejected("存储键不合法")
        return self._containment_check(self.root / storage_key)

    async def cleanup_orphans(self, role_id: str | None = None) -> int:
        """删除失去引用的资产（R6：DB 认领在事务内，文件删除在提交后）。"""
        removed = 0
        loop = asyncio.get_running_loop()
        for row in await self._repo.claim_orphan_assets():
            try:
                path = self.storage_path(row["storage_key"])
            except ImageRejected:
                continue
            if await loop.run_in_executor(None, path.exists):
                await loop.run_in_executor(None, path.unlink, True)
                removed += 1
        return removed

    def _is_managed_key(self, path: Path) -> bool:
        """识别本插件生成的托管文件形态：assets/<16hex>/asset_<32hex>.<ext>。"""
        pattern = re.compile(
            r"^assets/[0-9a-f]{16}/asset_[0-9a-f]{32}\.(jpg|png|webp)$")
        rel = path.relative_to(self.root).as_posix()
        return bool(pattern.match(rel))

    async def recover_untracked_files(self) -> list[str]:
        """受控恢复（R5）：托管目录中“有文件无 DB 行”的残留清理。

        - assets/：只扫描自身生成键形态的文件；崩溃窗口（rename 后 DB
          提交前）留下的无主文件在此移除。
        - tmp/：清理遗留的 *.part 临时文件（处理中途崩溃/取消残留）。
        返回清理的存储键。
        """
        loop = asyncio.get_running_loop()
        cleaned: list[str] = []
        if self.tmp_root.is_dir():
            for path in self.tmp_root.glob("*.part"):
                await loop.run_in_executor(None, path.unlink, True)
        if not self.assets_root.is_dir():
            return cleaned
        for path in self.assets_root.rglob("*"):
            if not path.is_file() or not self._is_managed_key(path):
                continue
            storage_key = path.relative_to(self.root).as_posix()
            row = await self._repo.get_asset_by_key(storage_key)
            if row is None:
                await loop.run_in_executor(None, path.unlink, True)
                cleaned.append(storage_key)
        return cleaned
