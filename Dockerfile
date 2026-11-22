FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements-runtime.lock ./
RUN python -m pip install --no-cache-dir -r requirements-runtime.lock

COPY app/ ./app/
COPY migrations/ ./migrations/

CMD ["python", "-m", "app.worker_cli"]
