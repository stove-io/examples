FROM golang:1.27.3@sha256:2920bb124f1a2e12a1d39f0cab756ae0068441ee1da7655dec7f5e374944095b AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY internal ./internal

ARG GO_BUILD_FLAGS=""
RUN CGO_ENABLED=0 GOOS=linux go build ${GO_BUILD_FLAGS} -o /out/ecomm .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/ecomm /app/ecomm

EXPOSE 8080
ENTRYPOINT ["/app/ecomm"]
