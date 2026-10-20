FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

ARG APP

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /app/bin ./cmd/${APP}

FROM alpine:3.22

WORKDIR /app

COPY --from=builder /app/bin ./bin

CMD ["./bin"]
