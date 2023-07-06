FROM rust:1.70 as builder

WORKDIR /usr/src/bytestack

COPY . .
COPY Cargo.toml ./
RUN cargo build --release --manifest-path ./services/controller/Cargo.toml

FROM gcr.io/distroless/cc-debian10
COPY --from=builder $WORKDIR/target/release/controller /controller
USER 1000
ENTRYPOINT ["/controller"]