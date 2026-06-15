FROM python:3.13-slim

RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . /app

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "test.py"]
