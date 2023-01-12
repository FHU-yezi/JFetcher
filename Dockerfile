FROM python:3.10.8-slim

WORKDIR /app

ENV TZ Asia/Shanghai

COPY requirements.txt .

RUN pip install \
    -r requirements.txt \
    --no-cache-dir \
    --no-compile \
    --disable-pip-version-check \
    --quiet \
    -i https://mirrors.aliyun.com/pypi/simple

COPY . .

CMD ["python", "main.py"]