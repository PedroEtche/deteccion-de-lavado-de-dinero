FROM python:3.11-slim

COPY ./requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY join .

ENTRYPOINT ["python3", "main.py"]
