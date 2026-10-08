FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    DENO_INSTALL=/usr/local

RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg curl ca-certificates unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Deno directly from the official release archive.
# This avoids deno_install.sh and its unzip/7z prerequisite check.
RUN set -eux; \
    DENO_VERSION="v2.9.7"; \
    curl -fsSL "https://github.com/denoland/deno/releases/download/${DENO_VERSION}/deno-x86_64-unknown-linux-gnu.zip" -o /tmp/deno.zip; \
    unzip -q /tmp/deno.zip -d /tmp/deno; \
    install -m 0755 /tmp/deno/deno /usr/local/bin/deno; \
    rm -rf /tmp/deno /tmp/deno.zip; \
    deno --version

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py .

CMD ["sh", "-c", "exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8080} --workers 1"]
