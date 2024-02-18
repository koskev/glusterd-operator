FROM rust:1.76 as builder
WORKDIR /usr/src/glusterd-operator
COPY . .
RUN cargo install --path .

FROM debian:bullseye-slim
COPY --from=builder /usr/local/cargo/bin/glusterd-operator /usr/local/bin/glusterd-operator
CMD ["glusterd-operator"]
