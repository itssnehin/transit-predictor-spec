# syntax=docker/dockerfile:1
# Sink consumer image — same build pattern as the ingester Dockerfile.

ARG PYTHON_VERSION=3.11

FROM python:${PYTHON_VERSION}-slim AS builder

WORKDIR /build

RUN pip install --no-cache-dir uv==0.11.17

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

# ---------------------------------------------------------------------------

FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

COPY --from=builder /build/.venv /app/.venv
COPY services/ ./services/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN useradd --no-create-home --shell /bin/false appuser
USER appuser

ENTRYPOINT ["python", "-m", "services.sink.consumer"]
