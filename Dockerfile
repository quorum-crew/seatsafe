FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM alpine:3.20
# iproute2 provides tc, which cmd/chaos uses to add network delay (netem).
RUN apk add --no-cache iproute2
COPY --from=build /out/ /usr/local/bin/
EXPOSE 8080
CMD ["router"]
