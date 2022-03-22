FROM golang:alpine AS builder

RUN apk update && apk add --no-cache git

WORKDIR $GOPATH/src/metaspy/js/

COPY . .

ENV GO111MODULE=on \
    CGO_ENABLED=0 \
    GOOS=linux \
    GOARCH=amd64

# COPY go.mod .
# COPY go.sum .
RUN go mod download

# RUN go build -o main .
RUN go build -o /go/bin/metaspy-js

FROM chromedp/headless-shell:latest

WORKDIR /home/

COPY --from=builder /go/bin/metaspy-js /home/metaspy-js

#COPY config.yml /home/config.yml

RUN apt-get update && apt-get -qq -y install tini

# # ENTRYPOINT ["dumb-init", "--"]
ENTRYPOINT ["tini", "--"]

CMD ["/home/metaspy-js"]