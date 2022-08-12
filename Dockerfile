FROM golang:1.18-alpine AS build-env
ARG version

ADD . /src
RUN cd /src/cmd && go mod vendor && go build -v -i -ldflags "-s -w" -o accountant

FROM alpine
WORKDIR /app
COPY --from=build-env /src/cmd/accountant /app/
ENTRYPOINT ["./accountant"]