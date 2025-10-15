# syntax=docker/dockerfile:1
FROM python:3.13-slim AS build

# Install uv
RUN apt-get update && apt-get install -y curl && \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    rm -rf /var/lib/apt/lists/*

# Add uv to PATH
ENV PATH="/root/.local/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
RUN uv sync --no-install-project

# ---- Runtime stage ----
FROM python:3.13-slim AS runtime

WORKDIR /app

# Copy uv binary từ build stage
COPY --from=build /root/.local/bin/uv /usr/local/bin/uv

# Copy code + dependencies
COPY --from=build /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY . .

# Expose cổng
EXPOSE 8000

CMD ["uv", "run", "celery", "-A", "app.worker", "worker", "--loglevel=info"]
