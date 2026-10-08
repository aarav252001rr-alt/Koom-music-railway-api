"""
🎵 YouTube Music API v1.0
FastAPI + yt-dlp | Render.com Ready
"""

import os
import re
import time
import uuid
import asyncio
import secrets
import logging
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Header, Depends, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl, Field
import httpx
import yt_dlp



# ============================================
# Railway-optimized config
# ============================================
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("koom-api")

API_KEY = os.getenv("API_KEY", "")
STREAM_TTL = int(os.getenv("STREAM_TTL", "300"))
INFO_TTL = int(os.getenv("INFO_TTL", "300"))
COOKIES_FILE = os.getenv("COOKIES_FILE", "/app/cookies.txt")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
PORT = int(os.getenv("PORT", "8080"))

# In-memory caches. Railway Free is normally a single small service, so this
# keeps hot tracks extremely fast without adding Redis/database cost.
INFO_CACHE: Dict[str, Dict[str, Any]] = {}
STREAM_CACHE: Dict[str, Dict[str, Any]] = {}
CACHE_LOCK = asyncio.Lock()


async def cleanup_expired():
    while True:
        now = time.time()
        for key, value in list(INFO_CACHE.items()):
            if value["expires"] < now:
                INFO_CACHE.pop(key, None)
        for key, value in list(STREAM_CACHE.items()):
            if value["expires"] < now:
                STREAM_CACHE.pop(key, None)
        await asyncio.sleep(30)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(cleanup_expired())
    logger.info("Koom Railway API started on port %s", PORT)
    yield
    task.cancel()


app = FastAPI(
    title="Koom Music API",
    description="Fast metadata and temporary audio streaming API",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def verify_key(x_api_key: Optional[str] = Header(None)):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(401, "Invalid or missing X-API-Key")
    return True


class FormatInfo(BaseModel):
    format_id: str
    ext: str
    acodec: Optional[str] = None
    abr: Optional[float] = None
    asr: Optional[int] = None
    filesize: Optional[int] = None
    quality_label: Optional[str] = None


class InfoResponse(BaseModel):
    id: str
    title: str
    uploader: Optional[str] = None
    duration: Optional[int] = None
    duration_string: Optional[str] = None
    view_count: Optional[int] = None
    upload_date: Optional[str] = None
    thumbnail: Optional[str] = None
    webpage_url: str
    is_playlist: bool = False
    formats: List[FormatInfo] = []


class StreamRequest(BaseModel):
    url: HttpUrl
    format_id: str = Field(..., description="Audio format ID from /info")
    convert_to: Optional[str] = Field(None, description="mp3, m4a, opus, flac, wav, ogg, aac")


class StreamResponse(BaseModel):
    stream_url: str
    expires_in: int
    format_id: str
    ext: str
    convert_to: Optional[str] = None
    token: str


def _get_ydl_opts(extra: Optional[dict] = None) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 15,
        "retries": 1,
        "fragment_retries": 1,
        "concurrent_fragment_downloads": 4,
        "js_runtimes": {"deno": {}},
    }
    if os.path.exists(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    if extra:
        opts.update(extra)
    return opts


def _detect_quality_label(abr: Optional[float]) -> str:
    if abr is None:
        return "unknown"
    if abr >= 256:
        return f"{int(abr)}kbps (Very High)"
    if abr >= 192:
        return f"{int(abr)}kbps (High)"
    if abr >= 128:
        return f"{int(abr)}kbps (Medium)"
    if abr >= 96:
        return f"{int(abr)}kbps (Low)"
    return f"{int(abr)}kbps (Very Low)"


def validate_youtube_url(url: str):
    if not ("youtube.com" in url or "youtu.be" in url):
        raise HTTPException(400, "Only YouTube URLs allowed")


def normalize_url(url: str) -> str:
    # Cache equivalent requests under the same key.
    return url.strip().split("&list=")[0]


def fetch_video_info(url: str) -> dict:
    with yt_dlp.YoutubeDL(_get_ydl_opts()) as ydl:
        return ydl.extract_info(url, download=False)


async def get_video_info(url: str) -> dict:
    key = normalize_url(url)
    now = time.time()

    cached = INFO_CACHE.get(key)
    if cached and cached["expires"] > now:
        return cached["data"]

    # Avoid duplicate yt-dlp calls for the same URL during traffic spikes.
    async with CACHE_LOCK:
        cached = INFO_CACHE.get(key)
        if cached and cached["expires"] > now:
            return cached["data"]

        data = await asyncio.to_thread(fetch_video_info, url)
        INFO_CACHE[key] = {"data": data, "expires": time.time() + INFO_TTL}
        return data


def extract_audio_formats(info: dict) -> List[Dict]:
    fmts = []
    for f in info.get("formats", []) or []:
        if f.get("vcodec") not in (None, "none"):
            continue
        if not f.get("acodec") or f.get("acodec") == "none":
            continue
        fmts.append({
            "format_id": str(f.get("format_id")),
            "ext": f.get("ext") or "webm",
            "acodec": f.get("acodec"),
            "abr": f.get("abr"),
            "asr": f.get("asr"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "quality_label": _detect_quality_label(f.get("abr")),
        })

    seen = set()
    out = []
    for f in sorted(fmts, key=lambda x: (x["abr"] or 0), reverse=True):
        key = (f["ext"], round(f["abr"] or 0))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)
    return out


def find_audio_format(info: dict, format_id: str) -> Optional[dict]:
    for f in info.get("formats", []) or []:
        if str(f.get("format_id")) == str(format_id):
            if f.get("vcodec") in (None, "none") and f.get("acodec") not in (None, "none"):
                return f
    return None


# ============================================
# Routes
# ============================================
@app.get("/")
async def root():
    return {
        "name": "Koom Music API",
        "version": "2.0.0",
        "platform": "Railway",
        "endpoints": {
            "GET /health": "Fast health check",
            "GET /info?url=...": "Metadata + audio formats",
            "GET /formats?url=...": "Audio formats only",
            "POST /stream": "Create temporary stream token",
            "GET /stream/{token}": "Audio stream with Range support",
            "POST /download": "Alias of /stream",
        },
        "docs": "/docs",
    }


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/info", response_model=InfoResponse, dependencies=[Depends(verify_key)])
async def info(url: str = Query(...)):
    validate_youtube_url(url)
    try:
        data = await get_video_info(url)
    except yt_dlp.utils.DownloadError as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:250]}")
    except Exception as e:
        logger.exception("info error")
        raise HTTPException(500, f"Server error: {str(e)[:200]}")

    return InfoResponse(
        id=data.get("id", ""),
        title=data.get("title", ""),
        uploader=data.get("uploader") or data.get("channel"),
        duration=data.get("duration"),
        duration_string=data.get("duration_string"),
        view_count=data.get("view_count"),
        upload_date=data.get("upload_date"),
        thumbnail=data.get("thumbnail"),
        webpage_url=data.get("webpage_url", url),
        is_playlist=data.get("_type") == "playlist",
        formats=[FormatInfo(**f) for f in extract_audio_formats(data)],
    )


@app.get("/formats", dependencies=[Depends(verify_key)])
async def formats(url: str = Query(...)):
    validate_youtube_url(url)
    try:
        data = await get_video_info(url)
    except Exception as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:250]}")
    return {
        "id": data.get("id"),
        "title": data.get("title"),
        "formats": extract_audio_formats(data),
    }


@app.post("/stream", response_model=StreamResponse, dependencies=[Depends(verify_key)])
async def create_stream(req: StreamRequest):
    url = str(req.url)
    validate_youtube_url(url)

    allowed_conv = {None, "mp3", "m4a", "opus", "flac", "wav", "ogg", "aac"}
    if req.convert_to not in allowed_conv:
        raise HTTPException(400, "Invalid convert_to")

    try:
        # This is the important speed optimization: extraction happens once.
        data = await get_video_info(url)
    except Exception as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:250]}")

    fmt = find_audio_format(data, req.format_id)
    if not fmt:
        raise HTTPException(400, f"Audio format '{req.format_id}' not available")

    direct_url = fmt.get("url")
    if not direct_url:
        raise HTTPException(502, "Could not resolve direct media URL")

    token = secrets.token_urlsafe(24)
    STREAM_CACHE[token] = {
        "direct_url": direct_url,
        "ext": fmt.get("ext") or "webm",
        "format_id": str(req.format_id),
        "convert_to": req.convert_to,
        "expires": time.time() + STREAM_TTL,
    }

    base = PUBLIC_BASE_URL
    stream_url = f"{base}/stream/{token}" if base else f"/stream/{token}"

    return StreamResponse(
        stream_url=stream_url,
        expires_in=STREAM_TTL,
        format_id=str(req.format_id),
        ext=fmt.get("ext") or "webm",
        convert_to=req.convert_to,
        token=token,
    )

# ---------- Helpers: byte proxy ----------
async def _proxy_bytes(direct_url: str, ext: str, request: Request):
    """Simple byte-range proxy"""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*",
    }
    range_header = request.headers.get("range")
    if range_header:
        headers["Range"] = range_header

    mime = {
        "m4a": "audio/mp4",
        "mp3": "audio/mpeg",
        "opus": "audio/ogg",
        "webm": "audio/webm",
        "ogg": "audio/ogg",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "aac": "audio/aac",
    }.get(ext, "application/octet-stream")

    client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
    req = client.build_request("GET", direct_url, headers=headers)
    resp = await client.send(req, stream=True)

    if resp.status_code not in (200, 206):
        await resp.aclose()
        await client.aclose()
        raise HTTPException(502, f"Upstream error {resp.status_code}")

    async def streamer():
        try:
            async for chunk in resp.aiter_bytes(chunk_size=64 * 1024):
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    resp_headers = {
        "Content-Type": mime,
        "Accept-Ranges": "bytes",
        "Cache-Control": "no-store",
    }
    if "content-length" in resp.headers:
        resp_headers["Content-Length"] = resp.headers["content-length"]
    if "content-range" in resp.headers:
        resp_headers["Content-Range"] = resp.headers["content-range"]

    return StreamingResponse(
        streamer(),
        status_code=resp.status_code,
        headers=resp_headers,
        media_type=mime,
    )


# ---------- Helpers: ffmpeg convert + proxy ----------# ---------- Helpers: ffmpeg convert + proxy ----------
async def _proxy_ffmpeg(direct_url: str, target_fmt: str, request: Request):
    """Pipe through ffmpeg -> response. No range support (live transcode)."""
    mime_map = {
        "mp3": "audio/mpeg",
        "m4a": "audio/mp4",
        "opus": "audio/ogg",
        "ogg": "audio/ogg",
        "flac": "audio/flac",
        "wav": "audio/wav",
        "aac": "audio/aac",
    }
    mime = mime_map.get(target_fmt, "application/octet-stream")

    # ffmpeg command
    cmd = [
        "ffmpeg",
        "-hide_banner",
        "-loglevel", "error",
        "-i", "pipe:0",
        "-vn",
        "-f", target_fmt,
    ]

    # format-specific quality
    if target_fmt == "mp3":
        cmd += ["-codec:a", "libmp3lame", "-b:a", "192k"]
    elif target_fmt == "m4a":
        cmd += ["-codec:a", "aac", "-b:a", "192k"]
    elif target_fmt == "opus":
        cmd += ["-codec:a", "libopus", "-b:a", "160k"]
    elif target_fmt == "ogg":
        cmd += ["-codec:a", "libvorbis", "-q:a", "5"]
    elif target_fmt == "flac":
        cmd += ["-codec:a", "flac"]
    elif target_fmt == "wav":
        cmd += ["-codec:a", "pcm_s16le"]
    elif target_fmt == "aac":
        cmd += ["-codec:a", "aac", "-b:a", "192k"]

    cmd += ["pipe:1"]

    # Download upstream (whole thing in memory - simple; fine for songs)
    async with httpx.AsyncClient(timeout=120.0, follow_redirects=True) as client:
        async with client.stream("GET", direct_url, headers={
            "User-Agent": "Mozilla/5.0",
        }) as resp:
            if resp.status_code not in (200, 206):
                raise HTTPException(502, f"Upstream {resp.status_code}")

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )

            async def feed_stdin():
                try:
                    async for chunk in resp.aiter_bytes(chunk_size=64 * 1024):
                        proc.stdin.write(chunk)
                        await proc.stdin.drain()
                except Exception:
                    pass
                finally:
                    try:
                        proc.stdin.close()
                    except Exception:
                        pass

            asyncio.create_task(feed_stdin())

            async def reader():
                try:
                    while True:
                        chunk = await proc.stdout.read(64 * 1024)
                        if not chunk:
                            break
                        yield chunk
                finally:
                    try:
                        proc.kill()
                    except Exception:
                        pass

            return StreamingResponse(
                reader(),
                media_type=mime,
                headers={
                    "Content-Disposition": f'attachment; filename="audio.{target_fmt}"',
                    "Cache-Control": "no-store",
                },
            )



# ---------- STREAM PROXY ----------
@app.get("/stream/{token}")
async def proxy_stream(token: str, request: Request):
    item = STREAM_CACHE.get(token)
    if not item:
        raise HTTPException(404, "Stream not found or expired")
    if item["expires"] < time.time():
        STREAM_CACHE.pop(token, None)
        raise HTTPException(410, "Stream expired")

    direct_url = item["direct_url"]
    ext = item["ext"]
    convert_to = item["convert_to"]

    if not convert_to:
        return await _proxy_bytes(direct_url, ext, request)

    return await _proxy_ffmpeg(direct_url, convert_to, request)


@app.post("/download", response_model=StreamResponse, dependencies=[Depends(verify_key)])
async def download_info(req: StreamRequest):
    return await create_stream(req)


# ============================================
# Error handlers
# ============================================
@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.detail, "status": exc.status_code},
    )
