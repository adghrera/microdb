FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/microdb ./cmd/microdb && \
    CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/microctl ./cmd/microctl

FROM alpine:3.20
RUN adduser -D -H microdb
USER microdb
COPY --from=build /out/microdb /usr/local/bin/microdb
COPY --from=build /out/microctl /usr/local/bin/microctl
EXPOSE 8001
VOLUME /data
ENTRYPOINT ["microdb"]
CMD ["--addr", ":8001", "--dir", "/data"]
