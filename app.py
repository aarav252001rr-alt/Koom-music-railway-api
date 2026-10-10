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
import threading
from pathlib import Path   # FIX: was missing -> /download/{token} crashed with NameError
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
def _norm_pot_url(u: str) -> str:
    """Railway public domains are served on https:443 only - ':4416' on *.up.railway.app just times out."""
    u = u.strip().rstrip("/")
    if not u:
        return ""
    from urllib.parse import urlsplit
    p = urlsplit(u if "://" in u else "http://" + u)
    if (p.hostname or "").endswith(".up.railway.app"):
        return f"https://{p.hostname}"
    return u


POT_BASE_URL = _norm_pot_url(os.getenv("POT_BASE_URL", ""))      # optional bgutil PO-token provider, e.g. http://pot.railway.internal:4416
# googlevideo URLs are bound to the IP that created them; Railway's outbound IP rotates within a small pool, so a
# 403 is often just "different egress IP". A fresh connection may leave from the right one - retry a few times.
EGRESS_RETRIES = max(1, int(os.getenv("EGRESS_RETRIES", "3")))
PROXY = os.getenv("YTDLP_PROXY", "").strip()          # optional: http(s)/socks5 proxy for YouTube (residential works best)
EXTRA_CLIENTS = [c.strip() for c in os.getenv("YT_PLAYER_CLIENTS", "").split(",") if c.strip()]
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


# ---- cookies: GitHub repo (preferred) -> local file fallback --------------------------------------
# Railway variables:
#   COOKIES_GITHUB_REPO   owner/repo of a PRIVATE repo holding the cookies file
#   COOKIES_GITHUB_PATH   path inside the repo            (default: cookies.txt)
#   COOKIES_GITHUB_BRANCH branch                           (default: main)
#   GITHUB_TOKEN          fine-grained token, ONLY this repo, permission "Contents: Read-only"
#   COOKIES_REFRESH_SECONDS  re-download interval          (default: 1800) -> update cookies without redeploying
GH_REPO = os.getenv("COOKIES_GITHUB_REPO", "").strip().strip("/")
GH_PATH = os.getenv("COOKIES_GITHUB_PATH", "cookies.txt").strip().lstrip("/")
GH_BRANCH = os.getenv("COOKIES_GITHUB_BRANCH", "main").strip()
GH_TOKEN = os.getenv("GITHUB_TOKEN", "").strip()
GH_REFRESH = int(os.getenv("COOKIES_REFRESH_SECONDS", "1800"))
GH_ALLOW_PUBLIC = os.getenv("COOKIES_ALLOW_PUBLIC", "") == "1"

JS_RUNTIME = shutil.which("deno") or shutil.which("node") or ""
_COOKIES_RUNTIME: Optional[str] = None   # None = not loaded yet, "" = none available
_COOKIES_T = 0.0
_COOKIES_SRC = ""
_COOKIES_LOCK = threading.Lock()


def _write_private(data: bytes) -> str:
    """Write cookies to a brand-new private dir (yt-dlp re-saves its jar, so it must be writable)."""
    dst = os.path.join(tempfile.mkdtemp(prefix="koom-ck-"), "cookies.txt")
    fd = os.open(dst, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "wb") as out:
        out.write(data)
    return dst


def _fetch_github_cookies() -> Optional[bytes]:
    if not GH_REPO:
        return None
    if not GH_TOKEN and not GH_ALLOW_PUBLIC:
        logger.error("COOKIES_GITHUB_REPO is set but GITHUB_TOKEN is missing - refusing to fetch cookies "
                     "anonymously (that would mean a PUBLIC repo with your login cookies). Use a private repo + token.")
        return None
    headers = {"Accept": "application/vnd.github.raw+json", "X-GitHub-Api-Version": "2022-11-28", "User-Agent": "koom-downloader"}
    if GH_TOKEN:
        headers["Authorization"] = f"Bearer {GH_TOKEN}"
    r = httpx.get(f"https://api.github.com/repos/{GH_REPO}/contents/{GH_PATH}", params={"ref": GH_BRANCH},
                  headers=headers, timeout=20, follow_redirects=True)
    if r.status_code != 200:
        raise RuntimeError(f"GitHub returned HTTP {r.status_code} (check repo name, path, branch and token access)")
    return r.content


def _cookie_path() -> Optional[str]:
    """Path of a private, writable cookies copy, or None. Never raises: cookie problems must not break playback."""
    global _COOKIES_RUNTIME, _COOKIES_T, _COOKIES_SRC
    if os.getenv("COOKIES_DISABLED", "") == "1":   # run fully cookie-less (also never contacts GitHub)
        return None
    with _COOKIES_LOCK:
        if _COOKIES_RUNTIME is not None and not (GH_REPO and time.time() - _COOKIES_T > GH_REFRESH):
            return _COOKIES_RUNTIME or None
        _COOKIES_T = time.time()
        data, src = None, ""
        if GH_REPO:
            try:
                data, src = _fetch_github_cookies(), f"github:{GH_REPO}/{GH_PATH}@{GH_BRANCH}"
            except Exception as e:  # noqa: BLE001
                logger.warning("GitHub cookies fetch failed: %s", e)
        if data is None and _COOKIES_RUNTIME:
            return _COOKIES_RUNTIME                     # refresh failed: keep the last good copy
        if data is None:
            try:
                if os.path.isfile(COOKIES_FILE):
                    with open(COOKIES_FILE, "rb") as f:
                        data, src = f.read(), COOKIES_FILE
            except Exception as e:  # noqa: BLE001
                logger.warning("Cannot read cookies file %s (%s). Fix: chmod 644 / readable by uid 1000.", COOKIES_FILE, e)
        if data and b"\t" in data and not data.lstrip().lower().startswith((b"<!doctype", b"<html", b"{")):
            try:
                _COOKIES_RUNTIME, _COOKIES_SRC = _write_private(data), src
                logger.info("Cookies loaded from %s (%d bytes)", src, len(data))
            except Exception as e:  # noqa: BLE001
                logger.warning("Cannot write private cookies copy: %s", e)
        elif data:
            logger.warning("Cookies from %s do not look like a Netscape cookies.txt - ignored", src)
        if not _COOKIES_RUNTIME:
            _COOKIES_RUNTIME = ""
        return _COOKIES_RUNTIME or None


# YouTube often answers "Sign in to confirm you're not a bot" to datacenter IPs. Different yt-dlp
# player clients are treated differently, so we try several and remember the one that works.
_VARIANTS = ([{"name": "env:" + ",".join(EXTRA_CLIENTS), "args": {"youtube": {"player_client": EXTRA_CLIENTS}}, "cookies": True}] if EXTRA_CLIENTS else []) + [
    # Verified on Railway via /diag: logged-in cookies put the account in YouTube's SABR-only experiment (direct URLs
    # 403), while these two cookie-less clients return playable URLs (HTTP 206). They go first.
    {"name": "tv_simply", "args": {"youtube": {"player_client": ["tv_simply"]}}, "cookies": False},
    {"name": "default-nocookie", "args": None, "cookies": False},
    {"name": "mweb-nocookie", "args": {"youtube": {"player_client": ["mweb"]}}, "cookies": False},
    # these tracks come from YouTube Music, so its own web client is worth a try (needs the PO-token provider)
    {"name": "web_music-nocookie", "args": {"youtube": {"player_client": ["web_music"]}}, "cookies": False},
    {"name": "web_embedded-nocookie", "args": {"youtube": {"player_client": ["web_embedded"]}}, "cookies": False},
    {"name": "tv-nocookie", "args": {"youtube": {"player_client": ["tv"]}}, "cookies": False},
    # cookie-based clients stay as a last-resort fallback (e.g. if YouTube starts demanding sign-in again)
    {"name": "default", "args": None, "cookies": True},
    {"name": "mweb", "args": {"youtube": {"player_client": ["mweb"]}}, "cookies": True},
    {"name": "web_creator", "args": {"youtube": {"player_client": ["web_creator"]}}, "cookies": True},
    {"name": "tv", "args": {"youtube": {"player_client": ["tv"]}}, "cookies": True},
    {"name": "android_vr", "args": {"youtube": {"player_client": ["android_vr"]}}, "cookies": False},
]
_VARIANT = 0
_RETRY_HINTS = ("not playable", "forbidden", "sign in", "not a bot", "confirm you", "http error 403", "po token", "no audio formats",
                "requested format", "unavailable", "player response")


def _pot_plugin_version() -> str:
    try:
        from importlib.metadata import version
        return version("bgutil-ytdlp-pot-provider")
    except Exception:  # noqa: BLE001
        return "NOT INSTALLED (add bgutil-ytdlp-pot-provider to requirements.txt)"


def _ping_pot() -> str:
    """Is the PO-token provider reachable? (bgutil exposes GET /ping)"""
    if not POT_BASE_URL:
        return "not configured (set POT_BASE_URL)"
    try:
        r = httpx.get(POT_BASE_URL.rstrip("/") + "/ping", timeout=8)
        return f"ok: {r.text[:120]}" if r.status_code == 200 else f"HTTP {r.status_code}"
    except Exception as e:  # noqa: BLE001
        return f"unreachable: {str(e)[:100]}"


def _probe_playable(info: dict) -> "tuple[bool, str]":
    """Request 2 bytes of the best direct audio URL (googlevideo answers 403 for PO-token / wrong-IP problems)."""
    from urllib.parse import urlsplit, parse_qs
    raws = [f for f in info.get("formats") or [] if f.get("url") and f.get("vcodec") in (None, "none")
            and f.get("acodec") not in (None, "none") and "m3u8" not in str(f.get("protocol", ""))]
    if not raws:
        return False, "no direct audio URLs"
    f = max(raws, key=lambda x: x.get("abr") or 0)
    h = dict(f.get("http_headers") or info.get("http_headers") or {})
    h.setdefault("User-Agent", "Mozilla/5.0")
    h["Range"] = "bytes=0-1"
    uip = (parse_qs(urlsplit(f["url"]).query).get("ip") or ["?"])[0]
    code = "?"
    for n in range(1, EGRESS_RETRIES + 1):
        try:
            r = httpx.get(f["url"], headers=h, timeout=12, follow_redirects=True, proxy=PROXY or None)  # new connection each time
            code = r.status_code
            if code in (200, 206):
                return True, f"HTTP {code} (try {n}, url ip {uip})"
            if code != 403:
                break
        except Exception as e:  # noqa: BLE001
            return False, str(e)[:100]
    return False, f"HTTP {code} after {n} tries (url ip {uip})"


def _egress_ips(k: int = 6) -> list:
    out = []
    for _ in range(k):
        try:
            out.append(httpx.get("https://api.ipify.org", timeout=6).text.strip())
        except Exception:  # noqa: BLE001
            out.append("?")
    return out


def _get_ydl_opts(extra: Optional[dict] = None, variant: Optional[int] = None) -> dict:
    v = _VARIANTS[_VARIANT if variant is None else variant]
    opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "noplaylist": True,
        "extract_flat": False,
        "socket_timeout": 30,
    }
    cookies = _cookie_path()
    if cookies and v["cookies"]:
        opts["cookiefile"] = cookies
    ea = dict(v["args"] or {})
    if POT_BASE_URL:
        ea["youtubepot-bgutilhttp"] = {"base_url": [POT_BASE_URL]}
    if ea:
        opts["extractor_args"] = ea
    if PROXY:
        opts["proxy"] = PROXY
    if extra:
        opts.update(extra)
    return opts


def _extract(url: str, extra: Optional[dict] = None) -> dict:
    """extract_info with automatic player-client fallback; remembers the winning client."""
    global _VARIANT
    have_cookies = bool(_cookie_path())
    order = [_VARIANT] + [n for n in range(len(_VARIANTS)) if n != _VARIANT]
    order = [n for n in order if have_cookies or not _VARIANTS[n]["cookies"]] or order   # no cookies loaded -> skip cookie variants
    last: Optional[Exception] = None
    for n in order:
        notes: list = []

        class Cap:                      # keep yt-dlp's own explanation (it is our best clue why formats are missing)
            def debug(self, m): pass
            def info(self, m): pass
            def warning(self, m): notes.append(str(m)[:170])
            def error(self, m): notes.append(str(m)[:170])
        try:
            opts = _get_ydl_opts(extra if extra is not None else {"ignore_no_formats_error": True}, n)
            opts["logger"] = Cap()
            with yt_dlp.YoutubeDL(opts) as ydl:
                info = ydl.extract_info(url, download=False)
            if extra is None and not extract_audio_formats(info):
                reason = f"availability={info.get('availability')}, age_limit={info.get('age_limit')}, live={info.get('live_status')}"
                hint = " | ".join(dict.fromkeys(n[:150] for n in notes))[:520]   # all distinct yt-dlp warnings, in order
                raise RuntimeError(f"no audio formats returned by this client ({reason})" + (f": {hint}" if hint else "") +
                                   ("" if JS_RUNTIME else " - NO JavaScript runtime installed (deno/node)"))
            if extra is None:
                ok, why = _probe_playable(info)
                if not ok:
                    raise RuntimeError(f"media URL not playable ({why}) - YouTube wants a PO token for this client")
            if n != _VARIANT:
                logger.info("yt-dlp client switched to %s", _VARIANTS[n]["name"])
            _VARIANT = n
            return info
        except Exception as e:  # noqa: BLE001
            last = e
            logger.warning("yt-dlp client %s failed for %s: %s", _VARIANTS[n]["name"], url, str(e)[:260])
            if not any(h in str(e).lower() for h in _RETRY_HINTS):
                raise
    raise last  # type: ignore[misc]


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
    return _extract(url)


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
    fid = format_id.lower()
    if fid in {"best", "auto", "highest", "high"}:
        return formats[0] if formats else None
    if formats and fid in {"standard", "medium"}:
        return formats[len(formats) // 2]
    if formats and fid in {"low", "lowest"}:
        return formats[-1]
    return next((f for f in formats if f["format_id"] == format_id), None)


def base_url(request: Request) -> str:
    return PUBLIC_BASE_URL or str(request.base_url).rstrip("/")

async def _extract_direct(url: str, format_id: str) -> tuple[dict, dict]:
    """Resolve a fresh source URL and retain yt-dlp's request headers for proxying."""
    data = await asyncio.to_thread(fetch_video_info, url)
    selected = find_format(data, format_id)
    if not selected:
        raise HTTPException(400, f"Audio format '{format_id}' is not available")
    resolved = await asyncio.to_thread(_extract, url, {"format": selected["format_id"], "noplaylist": True})
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

@app.get("/diag", dependencies=[Depends(verify_key)])
async def diag(url: str = Query("https://www.youtube.com/watch?v=dQw4w9WgXcQ")):
    """Why does extraction fail? Tries every yt-dlp client separately and reports cookie health (names only)."""
    cp = await asyncio.to_thread(_cookie_path)
    names = set()
    if cp:
        try:
            for line in open(cp, encoding="utf-8", errors="ignore"):
                p = line.rstrip("\n").split("\t")
                if len(p) >= 7 and not line.startswith("#") and "youtube" in p[0] + "google":
                    names.add(p[5])
        except Exception:
            pass
    key_cookies = ["LOGIN_INFO", "SAPISID", "__Secure-3PSID", "__Secure-1PSID", "SID", "HSID"]
    out = {
        "yt_dlp_version": getattr(yt_dlp.version, "__version__", "?"),
        "cookies_source": _COOKIES_SRC or None, "cookies_loaded": bool(cp), "cookie_count": len(names),
        "login_cookies_present": {k: (k in names) for k in key_cookies},
        "proxy_configured": bool(PROXY), "pot_provider": POT_BASE_URL or None, "pot_plugin": _pot_plugin_version(), "pot_provider_ping": await asyncio.to_thread(_ping_pot), "egress_ip_samples": await asyncio.to_thread(_egress_ips), "js_runtime": JS_RUNTIME or "MISSING - install deno (see Dockerfile)", "attempts": [],
    }
    def run(n):
        import re
        from urllib.parse import urlsplit, parse_qs
        lines: list = []

        class Cap:                       # capture only PO-token / plugin related yt-dlp debug output
            def debug(self, m):
                if re.search(r"pot|bgutil|plugin|po token|sabr|n challenge|nsig|skipp", str(m), re.I) and len(lines) < 14:
                    lines.append(str(m)[:200])
            def info(self, m): pass
            def warning(self, m):
                if len(lines) < 14: lines.append("W " + str(m)[:200])
            def error(self, m):
                if len(lines) < 14: lines.append("E " + str(m)[:200])
        res = {"client": _VARIANTS[n]["name"], "cookies": _VARIANTS[n]["cookies"]}
        try:
            with yt_dlp.YoutubeDL(_get_ydl_opts({"ignore_no_formats_error": True, "logger": Cap(), "verbose": True}, n)) as ydl:
                info = ydl.extract_info(url, download=False)
                ok, why = _probe_playable(info)
                res.update(ok=ok, audio_formats=len(extract_audio_formats(info)), media_url=why)
                raws = [f for f in info.get("formats") or [] if f.get("url") and f.get("vcodec") in (None, "none")
                        and f.get("acodec") not in (None, "none") and "m3u8" not in str(f.get("protocol", ""))]
                if raws:
                    f = max(raws, key=lambda x: x.get("abr") or 0)
                    q = parse_qs(urlsplit(f["url"]).query)
                    res["url_has_pot"] = "pot" in q
                    res["url_client"] = (q.get("c") or ["?"])[0]
                    h = dict(f.get("http_headers") or {}); h["Range"] = "bytes=0-1"
                    try:   # same URL through yt-dlp's OWN HTTP stack (different TLS/headers than httpx)
                        from yt_dlp.networking import Request as YReq
                        r = ydl.urlopen(YReq(f["url"], headers=h)); res["yt_dlp_own_http"] = f"HTTP {r.status}"; r.close()
                    except Exception as e:  # noqa: BLE001
                        res["yt_dlp_own_http"] = f"{type(e).__name__}: {str(e)[:90]}"
        except Exception as e:  # noqa: BLE001
            res.update(ok=False, error=str(e)[:200])
        res["yt_dlp_log"] = lines
        return res
    for n in range(len(_VARIANTS)):
        out["attempts"].append(await asyncio.to_thread(run, n))
    ping = out["pot_provider_ping"]
    if any(x["ok"] for x in out["attempts"]):
        out["verdict"] = "works"
    elif not POT_BASE_URL or not ping.startswith("ok"):
        out["verdict"] = ("PO token provider is not reachable - fix POT_BASE_URL first "
                          "(Railway public domain: https://<name>.up.railway.app with NO port; or http://<service>.railway.internal:4416)")
    else:
        out["verdict"] = "provider reachable but every client is still blocked: YouTube is rejecting this server's IP/cookies (try a proxy or a home/VPS IP)"
    return out


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
    # Reuse the resolved googlevideo URL for a few minutes: browsers send several Range requests per
    # track (seeking), and re-running yt-dlp for each one made playback slow to start.
    hit = item.get("direct")
    if not hit or hit["t"] < time.time() - 240:
        _, selected = await _extract_direct(item["url"], item["format_id"])
        hit = item["direct"] = {"t": time.time(), "url": selected["url"], "ext": selected["ext"], "h": selected.get("http_headers") or {}}
    return await _proxy_bytes(hit["url"], hit["ext"], request, item["title"], download=False, upstream_headers=hit["h"])

async def _proxy_bytes(direct_url: str, ext: str, request: Request, title: str, download: bool = False,
                       upstream_headers: Optional[dict] = None):
    # Use the headers yt-dlp associated with this exact media URL; keep Range for seeking.
    headers = dict(upstream_headers or {})
    headers.setdefault("User-Agent", "Mozilla/5.0")
    headers.setdefault("Accept", "*/*")
    if request.headers.get("range"):
        headers["Range"] = request.headers["range"]
    mime = {"m4a":"audio/mp4","mp3":"audio/mpeg","opus":"audio/ogg","webm":"audio/webm","ogg":"audio/ogg","flac":"audio/flac","wav":"audio/wav","aac":"audio/aac"}.get(ext, "application/octet-stream")
    for attempt in range(EGRESS_RETRIES):
        client = httpx.AsyncClient(timeout=60.0, follow_redirects=True, proxy=PROXY or None)
        resp = await client.send(client.build_request("GET", direct_url, headers=headers), stream=True)
        if resp.status_code in (200, 206):
            break
        code = resp.status_code
        await resp.aclose(); await client.aclose()
        if code != 403 or attempt == EGRESS_RETRIES - 1:
            raise HTTPException(502, f"Upstream error {code}")
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
            "retries": EGRESS_RETRIES + 5,
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
    if not JS_RUNTIME:
        logger.error("No JavaScript runtime (deno/node) found - YouTube extraction will fail with \"Requested format is not available\". Install deno.")
    else:
        logger.info("JS runtime: %s", JS_RUNTIME)

@app.exception_handler(HTTPException)
async def http_exc_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"error": exc.detail, "status": exc.status_code})
