# Pinned code generator: protoc 3.21.12 (Debian bookworm) + Go plugins.
FROM golang:1.24-bookworm
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
