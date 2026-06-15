# Build stage
FROM golang:1.25-alpine AS build

WORKDIR /app
COPY go.mod go.sum ./
# Fetch dependencies
RUN go mod download

COPY . .

# Build binaries
RUN go build -o /bin/broker ./cmd/broker && \
    go build -o /bin/processor ./cmd/processor && \
    go build -o /bin/loadgen ./cmd/loadgen

# Broker runtime
FROM alpine:latest AS broker
COPY --from=build /bin/broker /broker
EXPOSE 9092 9093 2112
CMD ["/broker"]

# Processor runtime
FROM alpine:latest AS processor
COPY --from=build /bin/processor /processor
EXPOSE 2113
CMD ["/processor"]

# Loadgen runtime
FROM alpine:latest AS loadgen
COPY --from=build /bin/loadgen /loadgen
EXPOSE 2114
CMD ["/loadgen"]
