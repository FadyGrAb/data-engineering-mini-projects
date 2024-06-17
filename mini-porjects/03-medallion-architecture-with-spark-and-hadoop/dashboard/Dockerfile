FROM python:3.12-slim-bullseye AS builder

WORKDIR /app

COPY requirements.txt app.py ./
RUN pip install --no-cache-dir -r requirements.txt

CMD [ "flask", "run", "--host", "0.0.0.0", "--port", "5000" ]