# Pinned code generator: protoc 3.21.12 (Debian bookworm) + Go plugins.
FROM golang:1.24-bookworm@sha256:1a6d4452c65dea36aac2e2d606b01b4a029ec90cc1ae53890540ce6173ea77ac
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1
