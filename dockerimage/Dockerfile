FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    build-essential \
    && apt-get clean

RUN pip install --upgrade pip && \
    pip install dask pandas numpy natsort

WORKDIR /app

CMD ["python"]