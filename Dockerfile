FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/server ./cmd/server

FROM alpine:3.20
RUN adduser -D -H app && apk add --no-cache ca-certificates wget
USER app
COPY --from=build /out/server /usr/local/bin/server
EXPOSE 8080 9090
ENTRYPOINT ["server"]
