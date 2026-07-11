# syntax=docker/dockerfile:1
# Stream processor image: PySpark in local[*] mode (see docs/DECISIONS.md ADR 0010).
# Heavier than the ingester/sink images because Spark needs a JVM.

# Pinned to Bookworm (Debian 12): Spark 3.5 needs Java 17, which Debian 13
# (Trixie) dropped in favour of Java 21. Bookworm still ships openjdk-17.
ARG PYTHON_VERSION=3.11
ARG DEBIAN_RELEASE=bookworm

# ---------------------------------------------------------------------------
# Stage 1: install dependencies (including the optional `spark` extra) into a venv
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE} AS builder

WORKDIR /build

RUN pip install --no-cache-dir uv==0.11.17

COPY pyproject.toml uv.lock ./

# --extra spark pulls PySpark; --no-dev/--no-install-project keep it lean.
RUN uv sync --frozen --no-dev --no-install-project --extra spark

# ---------------------------------------------------------------------------
# Stage 2: runtime image with a JRE
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim-${DEBIAN_RELEASE}

WORKDIR /app

# Spark 3.5 runs on Java 17. headless JRE keeps the image smaller.
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/.venv /app/.venv
COPY services/ ./services/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    HOME=/home/sparkuser

# Non-root user with a home dir so Spark's Ivy cache (~/.ivy2) is writable.
RUN useradd --create-home --home-dir /home/sparkuser --shell /bin/false sparkuser
USER sparkuser

# Spark UI (handy for debugging); not published by default in compose.
EXPOSE 4040

ENTRYPOINT ["python", "-m", "services.stream_processor.app"]
