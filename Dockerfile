FROM alpine:latest AS build

RUN set -xe; \
  apk --update add build-base protoc curl; \
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs > /bin/rustup; \
  sh /bin/rustup -y

COPY . /src

RUN set -xe; \
  cd /src; \
  /root/.cargo/bin/cargo build --release

FROM alpine:latest

COPY --from=build /src/target/release/versionstore-operator /bin/versionstore-operator
