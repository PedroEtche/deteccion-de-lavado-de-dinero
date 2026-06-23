FROM python:3.13-slim

# CLI de docker: el lider de la deteccion de fallas revive peers con `docker start`.
RUN apt-get update && apt-get install -y docker.io \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.group.main"]
