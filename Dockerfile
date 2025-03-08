FROM python:3.12-slim

ENV TZ Asia/Shanghai

WORKDIR /app

COPY pyproject.toml uv.lock .

RUN --mount=from=ghcr.io/astral-sh/uv:0.6.0,source=/uv,target=/bin/uv \
    uv sync --frozen --no-dev --no-cache

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "main.py"]
