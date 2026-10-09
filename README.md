# KOOM source-format audio API

Railway deployment for a FastAPI + yt-dlp audio API.

## Changes
- `/stream/{token}` proxies the exact selected yt-dlp audio format; no Opus-to-MP3 or other live transcoding.
- `/download/{token}` uses yt-dlp's native downloader for the selected format and attempts to embed metadata and cover art. No forced MP3 conversion.
- `/info` and `/formats` report source audio formats.
- Stream proxy forwards yt-dlp's per-format HTTP headers and Range requests for seeking.
- Cookies are not included in this repository; do not commit browser cookies or credentials.

## Deploy
Push these files to a GitHub repository and deploy that repository on Railway. Railway should build with the included Dockerfile.

## Endpoints
- `GET /health`
- `GET /info?url=<YouTube URL>`
- `GET /formats?url=<YouTube URL>`
- `POST /stream` with `{ "url": "https://www.youtube.com/watch?v=...", "format_id": "251" }`
- `GET /stream/{token}`
- `POST /download` with the same request shape
- `GET /download/{token}`

`convert_to` remains accepted for compatibility with old clients but is ignored. The output always uses the selected source format. Metadata/thumbnail embedding depends on source container support and the available FFmpeg/yt-dlp postprocessors.
