FROM alpine:3.12 AS downloader

WORKDIR /download

RUN apk --no-cache add wget
RUN wget https://raw.githubusercontent.com/google/styleguide/gh-pages/cpplint/cpplint.py

FROM python:2.7-slim

WORKDIR /app
COPY --from=downloader /download/cpplint.py /usr/bin
RUN chmod +x /usr/bin/cpplint.py