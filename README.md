# KOOM Downloader API v2

Railway/Render-ready FastAPI + yt-dlp + FFmpeg backend.

## New features
- `format_id=best` automatically selects the highest real audio-only source available.
- `/info` and `/formats` expose real audio source quality; no fake 192/256 kbps labels.
- `/stream` supports audio-only playback and optional conversion.
- `/download` creates a metadata-aware downloadable file (default MP3).
- Downloaded MP3/M4A files include title, artist, album and cover art when thumbnail retrieval succeeds.
- Download filename uses the real title instead of a random token.
- `download_url` is separate from playback `stream_url` while preserving simple integration.

## Environment variables
- `API_KEY` optional
- `PUBLIC_BASE_URL` recommended on Railway, e.g. `https://your-service.up.railway.app`
- `STREAM_TTL=300`
- `DOWNLOAD_TTL=900`
- `COOKIES_FILE=/app/cookies.txt` optional; never commit private cookies

## Example
POST `/stream`
```json
{"url":"https://www.youtube.com/watch?v=VIDEO_ID","format_id":"best"}
```

POST `/download`
```json
{"url":"https://www.youtube.com/watch?v=VIDEO_ID","format_id":"best","convert_to":"mp3"}
```

The download response contains `download_url`, `filename`, `title`, `artist`, `album`, `thumbnail`, and `source_quality`.
