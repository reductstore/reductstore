# only for GitHub Actions
FROM reductstore/reductstore AS builder
FROM ubuntu:22.04

RUN apt-get update && apt-get install -y curl

COPY --from=builder /build/bin/reductstore /usr/local/bin/reductstore
RUN mkdir /data

EXPOSE 8383

CMD ["reductstore"]
