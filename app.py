"""KOOM Downloader API v2 - FastAPI + yt-dlp + FFmpeg.
Audio-focused YouTube URL resolver/streamer with metadata-aware downloads.
"""
import os
import re
import time
import uuid
import asyncio
import secrets
import logging
import tempfile
import shutil
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, Header, Depends, Request
from fastapi.responses import StreamingResponse, JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, HttpUrl, Field
import httpx
import yt_dlp

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("koom-downloader")

API_KEY = os.getenv("API_KEY", "")
STREAM_TTL = int(os.getenv("STREAM_TTL", "300"))
DOWNLOAD_TTL = int(os.getenv("DOWNLOAD_TTL", "900"))
COOKIES_FILE = os.getenv("COOKIES_FILE", "/app/cookies.txt")
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "/tmp/koom-downloads")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

TOKENS: Dict[str, Dict[str, Any]] = {}

app = FastAPI(
    title="KOOM Downloader API",
    description="Audio extraction, streaming and metadata-aware downloads.",
    version="2.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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
    is_best: bool = False

class InfoResponse(BaseModel):
    id: str
    title: str
    uploader: Optional[str] = None
    artist: Optional[str] = None
    album: Optional[str] = None
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
    format_id: str = Field(..., description="Audio format ID from /info; use best for automatic best-audio selection")
    convert_to: Optional[str] = Field(None, description="Deprecated; conversion is disabled and source format is preserved")

class StreamResponse(BaseModel):
    stream_url: str
    expires_in: int
    format_id: str
    ext: str
    convert_to: Optional[str] = None
    token: str
    download_url: Optional[str] = None
    filename: Optional[str] = None


def _get_ydl_opts(extra: Optional[dict] = None) -> dict:
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 30,
    }
    if os.path.isfile(COOKIES_FILE):
        opts["cookiefile"] = COOKIES_FILE
    if extra:
        opts.update(extra)
    return opts


def _clean_filename(title: str, ext: str) -> str:
    title = re.sub(r'[\\/:*?"<>|\r\n]+', " ", title or "audio")
    title = re.sub(r"\s+", " ", title).strip(" .")[:180] or "audio"
    return f"{title}.{ext}"


def _artist(info: dict) -> str:
    return info.get("artist") or info.get("uploader") or info.get("channel") or "Unknown Artist"


def _album(info: dict) -> str:
    return info.get("album") or info.get("playlist_title") or "Unknown Album"


def _quality_label(abr: Optional[float]) -> str:
    if abr is None:
        return "Unknown"
    if abr >= 256:
        return f"{int(abr)} kbps (Very High)"
    if abr >= 192:
        return f"{int(abr)} kbps (High)"
    if abr >= 128:
        return f"{int(abr)} kbps (Medium)"
    if abr >= 96:
        return f"{int(abr)} kbps (Low)"
    return f"{int(abr)} kbps (Very Low)"


def fetch_video_info(url: str) -> dict:
    with yt_dlp.YoutubeDL(_get_ydl_opts()) as ydl:
        return ydl.extract_info(url, download=False)


def extract_audio_formats(info: dict) -> List[Dict[str, Any]]:
    """Return real audio-only source formats. Never invent bitrate/quality."""
    fmts = []
    for f in info.get("formats", []) or []:
        if f.get("vcodec") not in (None, "none"):
            continue
        if not f.get("acodec") or f.get("acodec") == "none":
            continue
        abr = f.get("abr")
        fmts.append({
            "format_id": str(f.get("format_id")),
            "ext": f.get("ext") or "webm",
            "acodec": f.get("acodec"),
            "abr": abr,
            "asr": f.get("asr"),
            "filesize": f.get("filesize") or f.get("filesize_approx"),
            "quality_label": _quality_label(abr),
            "is_best": False,
        })

    # Prefer highest bitrate, then codec/container tie-breakers.
    fmts.sort(key=lambda x: (x["abr"] or 0, 1 if str(x["acodec"]).startswith("opus") else 0), reverse=True)

    # Keep one entry per extension+bitrate to avoid a noisy list.
    seen = set()
    out = []
    for f in fmts:
        key = (f["ext"], round(f["abr"] or 0))
        if key in seen:
            continue
        seen.add(key)
        out.append(f)

    if out:
        out[0]["is_best"] = True
    return out


def find_format(info: dict, format_id: str) -> Optional[dict]:
    formats = extract_audio_formats(info)
    if format_id.lower() in {"best", "auto", "highest"}:
        return formats[0] if formats else None
    return next((f for f in formats if f["format_id"] == format_id), None)


def base_url(request: Request) -> str:
    return PUBLIC_BASE_URL or str(request.base_url).rstrip("/")

async def _extract_direct(url: str, format_id: str) -> tuple[dict, dict]:
    """Resolve a fresh source URL and retain yt-dlp's request headers for proxying."""
    data = await asyncio.to_thread(fetch_video_info, url)
    selected = find_format(data, format_id)
    if not selected:
        raise HTTPException(400, f"Audio format '{format_id}' is not available")
    opts = _get_ydl_opts({"format": selected["format_id"], "noplaylist": True})
    resolved = await asyncio.to_thread(lambda: yt_dlp.YoutubeDL(opts).extract_info(url, download=False))
    # With a format selector, yt-dlp may return the selected format at top level.
    chosen = next((f for f in (resolved.get("formats", []) or []) if str(f.get("format_id")) == selected["format_id"]), None)
    direct = (chosen or {}).get("url") or resolved.get("url")
    request_headers = (chosen or {}).get("http_headers") or resolved.get("http_headers") or {}
    if not direct:
        raise HTTPException(502, "Could not resolve direct audio URL")
    return data, {**selected, "url": direct, "http_headers": request_headers}


@app.get("/")
async def root():
    return {"name": "KOOM Downloader API", "version": "2.0.0", "docs": "/docs", "features": ["best audio", "real source quality", "stream", "metadata downloads", "cover art", "ID3/M4A tags"]}

@app.get("/health")
async def health():
    return {"status": "ok", "version": "2.0.0", "time": int(time.time())}

@app.get("/info", response_model=InfoResponse, dependencies=[Depends(verify_key)])
async def info(url: str = Query(...)):
    if "youtube.com" not in url and "youtu.be" not in url:
        raise HTTPException(400, "Only YouTube URLs allowed")
    try:
        data = await asyncio.to_thread(fetch_video_info, url)
    except Exception as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:300]}")
    return InfoResponse(
        id=data.get("id", ""), title=data.get("title", ""), uploader=data.get("uploader") or data.get("channel"),
        artist=_artist(data), album=_album(data), duration=data.get("duration"), duration_string=data.get("duration_string"),
        view_count=data.get("view_count"), upload_date=data.get("upload_date"), thumbnail=data.get("thumbnail"),
        webpage_url=data.get("webpage_url", url), is_playlist=data.get("_type") == "playlist",
        formats=[FormatInfo(**f) for f in extract_audio_formats(data)],
    )

@app.get("/formats", dependencies=[Depends(verify_key)])
async def formats(url: str = Query(...)):
    if "youtube.com" not in url and "youtu.be" not in url:
        raise HTTPException(400, "Only YouTube URLs allowed")
    try:
        data = await asyncio.to_thread(fetch_video_info, url)
    except Exception as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:300]}")
    audio = extract_audio_formats(data)
    return {
        "title": data.get("title"), "id": data.get("id"), "thumbnail": data.get("thumbnail"),
        "artist": _artist(data), "album": _album(data), "best_audio": audio[0] if audio else None,
        "formats": audio,
    }

@app.post("/stream", response_model=StreamResponse, dependencies=[Depends(verify_key)])
async def create_stream(req: StreamRequest, request: Request):
    url = str(req.url)
    if "youtube.com" not in url and "youtu.be" not in url:
        raise HTTPException(400, "Only YouTube URLs allowed")
    # Conversion intentionally disabled: always preserve yt-dlp's actual source format.
    try:
        data = await asyncio.to_thread(fetch_video_info, url)
    except Exception as e:
        raise HTTPException(422, f"Extraction failed: {str(e)[:300]}")
    selected = find_format(data, req.format_id)
    if not selected:
        raise HTTPException(400, f"Audio format '{req.format_id}' is not available. Use format_id='best' for highest available audio.")
    token = secrets.token_urlsafe(24)
    title = data.get("title") or "audio"
    ext = selected["ext"]
    filename = _clean_filename(title, ext)
    TOKENS[token] = {
        "type": "stream", "url": url, "format_id": selected["format_id"], "ext": selected["ext"],
        "convert_to": None, "expires": time.time() + STREAM_TTL, "title": title,
        "artist": _artist(data), "album": _album(data), "thumbnail": data.get("thumbnail"),
        "duration": data.get("duration"), "track": data.get("track") or title,
    }
    b = base_url(request)
    return StreamResponse(
        stream_url=f"{b}/stream/{token}", expires_in=STREAM_TTL, format_id=selected["format_id"], ext=selected["ext"],
        convert_to=None, token=token, filename=filename,
    )

@app.get("/stream/{token}")
async def proxy_stream(token: str, request: Request):
    item = TOKENS.get(token)
    if not item or item.get("expires", 0) < time.time():
        TOKENS.pop(token, None)
        raise HTTPException(410, "Stream expired or not found")
    data, selected = await _extract_direct(item["url"], item["format_id"])
    return await _proxy_bytes(selected["url"], selected["ext"], request, item["title"], download=False,
                              upstream_headers=selected.get("http_headers") or {})

async def _proxy_bytes(direct_url: str, ext: str, request: Request, title: str, download: bool = False,
                       upstream_headers: Optional[dict] = None):
    # Use the headers yt-dlp associated with this exact media URL; keep Range for seeking.
    headers = dict(upstream_headers or {})
    headers.setdefault("User-Agent", "Mozilla/5.0")
    headers.setdefault("Accept", "*/*")
    if request.headers.get("range"):
        headers["Range"] = request.headers["range"]
    mime = {"m4a":"audio/mp4","mp3":"audio/mpeg","opus":"audio/ogg","webm":"audio/webm","ogg":"audio/ogg","flac":"audio/flac","wav":"audio/wav","aac":"audio/aac"}.get(ext, "application/octet-stream")
    client = httpx.AsyncClient(timeout=60.0, follow_redirects=True)
    resp = await client.send(client.build_request("GET", direct_url, headers=headers), stream=True)
    if resp.status_code not in (200, 206):
        await resp.aclose(); await client.aclose(); raise HTTPException(502, f"Upstream error {resp.status_code}")
    async def streamer():
        try:
            async for chunk in resp.aiter_bytes(64 * 1024): yield chunk
        finally:
            await resp.aclose(); await client.aclose()
    h = {"Content-Type": mime, "Accept-Ranges":"bytes", "Cache-Control":"no-store", "Content-Disposition": f'{"attachment" if download else "inline"}; filename="{_clean_filename(title, ext)}"'}
    for k in ("content-length", "content-range"):
        if k in resp.headers: h["Content-Length" if k == "content-length" else "Content-Range"] = resp.headers[k]
    return StreamingResponse(streamer(), status_code=resp.status_code, headers=h, media_type=mime)

async def _download_tagged(item: dict, output_fmt: str) -> str:
    """Download the chosen yt-dlp format as-is, then embed metadata/cover when supported."""
    work = tempfile.mkdtemp(prefix="koom-")
    try:
        outtmpl = os.path.join(work, "source.%(ext)s")
        opts = _get_ydl_opts({
            "skip_download": False,
            "format": item["format_id"],
            "outtmpl": outtmpl,
            "noplaylist": True,
            "writethumbnail": True,
            "postprocessors": [
                {"key": "FFmpegMetadata", "add_metadata": True},
                {"key": "EmbedThumbnail"},
            ],
        })
        # Run yt-dlp's own downloader rather than fetching googlevideo URLs separately.
        await asyncio.to_thread(lambda: _download_with_ydl(item["url"], opts))
        candidates = [p for p in Path(work).glob("source.*") if p.is_file() and not p.name.endswith((".jpg", ".jpeg", ".png", ".webp", ".json"))]
        if not candidates:
            raise HTTPException(502, "yt-dlp did not produce an audio file")
        source = max(candidates, key=lambda p: p.stat().st_size)
        final_name = _clean_filename(item.get("title") or "audio", source.suffix.lstrip(".") or "webm")
        final = os.path.join(DOWNLOAD_DIR, secrets.token_hex(12) + "-" + final_name)
        shutil.move(str(source), final)
        return final
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Native yt-dlp download failed")
        raise HTTPException(502, f"yt-dlp download failed: {str(e)[:250]}")
    finally:
        shutil.rmtree(work, ignore_errors=True)


def _download_with_ydl(url: str, opts: dict):
    with yt_dlp.YoutubeDL(opts) as ydl:
        ydl.download([url])


@app.post("/download", dependencies=[Depends(verify_key)])
async def create_download(req: StreamRequest, request: Request):
    url = str(req.url)
    if "youtube.com" not in url and "youtu.be" not in url: raise HTTPException(400, "Only YouTube URLs allowed")
    try: data = await asyncio.to_thread(fetch_video_info, url)
    except Exception as e: raise HTTPException(422, f"Extraction failed: {str(e)[:300]}")
    selected = find_format(data, req.format_id)
    if not selected: raise HTTPException(400, f"Audio format '{req.format_id}' is not available. Use 'best'.")
    # Source-format downloads only. convert_to is accepted for old clients but ignored.
    output_fmt = selected["ext"]
    token = secrets.token_urlsafe(24)
    title = data.get("title") or "audio"
    TOKENS[token] = {"type":"download","url":url,"format_id":selected["format_id"],"expires":time.time()+DOWNLOAD_TTL,"title":title,"artist":_artist(data),"album":_album(data),"thumbnail":data.get("thumbnail"),"track":data.get("track") or title,"output_fmt":output_fmt,"file":None}
    b = base_url(request)
    return {"download_url":f"{b}/download/{token}","stream_url":f"{b}/download/{token}","expires_in":DOWNLOAD_TTL,"format_id":selected["format_id"],"source_quality":selected["quality_label"],"output_format":output_fmt,"filename":_clean_filename(title,output_fmt),"title":title,"artist":_artist(data),"album":_album(data),"thumbnail":data.get("thumbnail")}

@app.get("/download/{token}")
async def download_file(token: str):
    item = TOKENS.get(token)
    if not item or item.get("expires",0) < time.time():
        TOKENS.pop(token,None); raise HTTPException(410,"Download expired or not found")
    if item.get("file") and os.path.isfile(item["file"]):
        return FileResponse(item["file"], filename=_clean_filename(item["title"],item["output_fmt"]), media_type={"mp3":"audio/mpeg","m4a":"audio/mp4","webm":"audio/webm","opus":"audio/ogg","ogg":"audio/ogg","flac":"audio/flac","wav":"audio/wav","aac":"audio/aac"}.get(item["output_fmt"], "application/octet-stream"))
    path = await _download_tagged(item,item["output_fmt"])
    item["file"] = path
    return FileResponse(path, filename=_clean_filename(item["title"],item["output_fmt"]), media_type={"mp3":"audio/mpeg","m4a":"audio/mp4","webm":"audio/webm","opus":"audio/ogg","ogg":"audio/ogg","flac":"audio/flac","wav":"audio/wav","aac":"audio/aac"}.get(item["output_fmt"], "application/octet-stream"))

@app.on_event("startup")
async def startup_cleanup():
    logger.info("KOOM Downloader API v2 started")

@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail, "status": exc.status_code})
