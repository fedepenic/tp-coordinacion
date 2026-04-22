FROM golang:1.24-alpine3.23 AS builder
# join uses docker multistage builds feature https://docs.docker.com/develop/develop-images/multistage-build/
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY go.mod .
COPY go.sum .
COPY common ./common
RUN go mod download
COPY join .
# CGO_ENABLED must be disabled to run go binary in Alpine
RUN CGO_ENABLED=0 GOOS=linux go build -o bin/join

FROM busybox:latest
COPY --from=builder /build/bin/join /join
ENTRYPOINT ["./join"]
