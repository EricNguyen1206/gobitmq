FROM golang:1.25 AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ cmd/
COPY internal/ internal/

RUN CGO_ENABLED=0 go build -o /out/erionn-mq ./cmd

FROM gcr.io/distroless/base-debian12

WORKDIR /app
COPY --from=builder /out/erionn-mq /app/erionn-mq

ENV ERIONN_AMQP_ADDR=":5672"
ENV ERIONN_MGMT_ADDR=":15672"
ENV ERIONN_DATA_DIR="/data/broker"

EXPOSE 5672 15672
VOLUME ["/data"]

ENTRYPOINT ["/app/erionn-mq"]
