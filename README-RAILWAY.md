# Koom Music API — Railway

FastAPI + yt-dlp backend optimized for a small Railway service.

## Deploy

1. Put these files in a GitHub repository.
2. Railway → New Project → Deploy from GitHub Repo.
3. Railway detects the root `Dockerfile` automatically.
4. Generate a public domain in the service Networking settings.
5. Add variables in Railway → Variables:
   - `API_KEY` = your private API key
   - optionally `INFO_TTL=300`
   - optionally `STREAM_TTL=300`
   - optionally `PUBLIC_BASE_URL=https://your-domain`
6. Redeploy.

Railway provides `PORT`; the container listens on `0.0.0.0:$PORT`.

## Endpoints

### Health
`GET /health`

### Metadata
`GET /info?url=YOUTUBE_URL`

Header:
`X-API-Key: YOUR_API_KEY`

### Formats
`GET /formats?url=YOUTUBE_URL`

### Create stream
`POST /stream`

JSON:
```json
{
  "url": "https://www.youtube.com/watch?v=VIDEO_ID",
  "format_id": "251",
  "convert_to": null
}
```

Then play the returned `stream_url`.

## Speed design

- Metadata is cached in RAM for `INFO_TTL` seconds.
- Concurrent requests for the same uncached URL are collapsed to one yt-dlp extraction.
- `/stream` uses the direct media URL already returned by that extraction.
- `/stream/{token}` does **not** run yt-dlp again.
- Range requests are passed through for seeking.
- `ffmpeg` conversion is only used when `convert_to` is requested.
- One worker is intentional for a small/free service; it avoids duplicating caches and wasting RAM.

## Important

This project does not include session cookies. Do not commit browser cookies or secrets to GitHub.

Railway changing the hosting environment may help if the previous Render IP was being rate-limited, but it does not guarantee YouTube extraction. If YouTube returns 403/429 from Railway too, changing FastAPI settings cannot reliably remove that restriction.
