FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/microdb ./cmd/microdb && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/microctl ./cmd/microctl

FROM alpine:3.20
# /data must belong to the unprivileged user the server runs as. Docker
# initialises a FRESH named volume from the image's /data and carries the
# ownership across, so a root-owned /data here yields a root-owned volume —
# and the process below could not create data.jsonl inside it
# ("open /data/data.jsonl: permission denied"). Owning it here is what
# makes `docker compose up` work on a clean checkout.
RUN adduser -D -H microdb \
    && mkdir -p /data \
    && chown microdb:microdb /data
USER microdb
COPY --from=build /out/microdb /usr/local/bin/microdb
COPY --from=build /out/microctl /usr/local/bin/microctl
EXPOSE 8001
VOLUME /data
ENTRYPOINT ["microdb"]
CMD ["--addr", ":8001", "--dir", "/data"]
