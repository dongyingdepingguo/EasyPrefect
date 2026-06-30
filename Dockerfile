ARG PYTHON_IMAGE=192.168.50.224:8088/library/python:3.14-slim

FROM ${PYTHON_IMAGE} AS builder

WORKDIR /app

ENV UV_LINK_MODE=copy \
    PIP_NO_CACHE_DIR=1 \
    PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple \
    UV_DEFAULT_INDEX=https://mirrors.aliyun.com/pypi/simple \
    UV_PROJECT_ENVIRONMENT=/opt/venv

RUN python -m pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

FROM ${PYTHON_IMAGE} AS runtime

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=Asia/Shanghai \
    PYTHONPATH=/app \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:$PATH" \
    DO_NOT_TRACK=1

COPY --from=builder /opt/venv /opt/venv
