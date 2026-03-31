FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/app \
    TZ=Asia/Jakarta

WORKDIR ${APP_HOME}

RUN apt-get update \
    && apt-get install -y --no-install-recommends bash cron curl tzdata \
    && ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime \
    && echo "${TZ}" > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt

RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

RUN chmod +x /app/scripts/run_scheduled_etl.sh \
    && mkdir -p /app/logs /app/run /app/app/db /app/backups/sqlite

EXPOSE 8000 5504

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
