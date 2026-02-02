FROM rust:1.89.0-slim-bookworm

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    musl-tools \
    perl \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*

RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /work
