# build stage
FROM golang:alpine as builder

ENV GO111MODULE=on
WORKDIR /app

COPY go.mod go.sum /app/
RUN go mod download
COPY *.go /app/
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build

# final stage
FROM scratch
WORKDIR /app
COPY --from=builder /app/cfsd /app/cfsd
COPY mtab.yaml /app/

ENTRYPOINT ["/app/cfsd"]
