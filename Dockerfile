FROM python:3.10-slim

ENV TZ Asia/Shanghai

WORKDIR /app

COPY requirements.txt .
RUN pip install uv --no-cache-dir --disable-pip-version-check && \
    uv pip install --system --no-cache -r requirements.txt && \
    cd /usr/local/bin && \
    rm uv uvx

COPY . .

CMD ["python", "main.py"]
