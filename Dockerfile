FROM python:3.11-slim
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
# `deno` (PyPI wheel) puts a Deno binary in /usr/local/bin: yt-dlp needs a JS runtime to unlock YouTube formats.
RUN pip install --no-cache-dir -r requirements.txt && deno --version
COPY app.py .
RUN useradd -m -u 1000 apiuser && mkdir -p /tmp/koom-downloads && chown -R apiuser:apiuser /app /tmp/koom-downloads
USER apiuser
EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
