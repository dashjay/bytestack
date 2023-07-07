FROM rust:1.70 as builder

WORKDIR /usr/src/bytestack

COPY bin bin
COPY core core
COPY services services
COPY Cargo.toml Cargo.lock ./
RUN cargo build --release --manifest-path ./services/controller/Cargo.toml

FROM rust:1.70
COPY --from=builder /usr/src/bytestack/target/release/controller /controller
USER 1000