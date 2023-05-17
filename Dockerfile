FROM rust:latest AS builder
WORKDIR /usr/src/app

COPY . .
RUN cargo install --path .

FROM debian:latest

COPY --from=builder /usr/local/cargo/bin/pg_tps_optimizer /usr/local/bin/pg_tps_optimizer

COPY README.md LICENSE .
ENTRYPOINT [ "/usr/local/bin/pg_tps_optimizer" ]
CMD ["--max-wait", "10s", "--min-samples", "10", "--range", "100", "--spread", "10"]
