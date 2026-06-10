FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

COPY src /app/src

ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "src.historical_filter.main"]
