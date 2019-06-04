FROM golang:1 as builder

WORKDIR /go-workspace/sample-compute-apache-logs-ref

COPY . .

RUN go test ./... && \
    CGO_ENABLED=0 GOOS=linux go build -a -o /go/bin/compute-cost .

FROM alpine:3.8

COPY cost_per_click.yml /apache-log-generator/
COPY --from=builder /go/bin/compute-cost /apache-log-generator/

WORKDIR /apache-log-generator
CMD /apache-log-generator/compute-cost -c /apache-log-generator/cost_per_click.yml