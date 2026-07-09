# syntax=docker/dockerfile:1.4
FROM python:3.12-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY ./requirements.txt /app/

RUN pip3 install -r /app/requirements.txt

FROM python:3.12-alpine AS run

WORKDIR /app

RUN apk add --no-cache tzdata

COPY --from=builder /usr/local /usr/local
COPY . /app

CMD ["python3", "app.py"]
