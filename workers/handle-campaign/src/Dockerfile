# syntax=docker/dockerfile:1.4
FROM python:3.12-alpine3.19 as builder

WORKDIR /app

RUN apk add --no-cache gcc musl-dev python3-dev build-base git

RUN apk add --no-cache ca-certificates

RUN pip3 install --upgrade pip

COPY . /app

RUN pip3 install -r requirements.txt

FROM python:3.12-alpine3.19 AS run

RUN apk add --no-cache tzdata

WORKDIR /app

COPY --from=builder /usr/local /usr/local
COPY . /app

CMD ["python3", "app.py"]
