FROM rust:1.70 as builder

WORKDIR /usr/src
ARG APP=controller

WORKDIR /usr/src/$APP

COPY ./src src
COPY Cargo.toml ./
RUN cargo build

# Copy the app to an base Docker image, here we use distroless
FROM gcr.io/distroless/cc-debian10
COPY --from=builder /usr/local/cargo/bin/$APP /$APP
USER 1000
ENTRYPOINT ["/$APP"]