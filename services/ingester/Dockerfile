# syntax=docker/dockerfile:1
# Multi-stage build: build stage installs dependencies, runtime stage is lean.
# The same base image is used in both stages so the Python ABI matches.

ARG PYTHON_VERSION=3.11

# ---------------------------------------------------------------------------
# Stage 1: install dependencies into a venv
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim AS builder

WORKDIR /build

# uv is used for fast, reproducible installs (matches local dev tooling)
RUN pip install --no-cache-dir uv==0.11.17

# Copy only the lock files first — Docker can cache this layer if they don't change
COPY pyproject.toml uv.lock ./

# Install runtime deps only (no dev/test extras) into the project venv
RUN uv sync --frozen --no-dev --no-install-project

# ---------------------------------------------------------------------------
# Stage 2: lean runtime image
# ---------------------------------------------------------------------------
FROM python:${PYTHON_VERSION}-slim

WORKDIR /app

# Copy the populated venv from the builder stage
COPY --from=builder /build/.venv /app/.venv

# Copy application source
COPY services/ ./services/

# Put the venv on PATH so `python` resolves to the venv interpreter
ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Non-root user for security best practice
RUN useradd --no-create-home --shell /bin/false appuser
USER appuser

# Health/metrics HTTP server
EXPOSE 9100

ENTRYPOINT ["python", "-m", "services.ingester.main"]
