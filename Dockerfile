FROM rust:1.91-slim-bookworm as builder

RUN apt-get update && \
    apt-get install -y curl gcc g++ libhdf5-dev perl make libsasl2-dev pkg-config libcfitsio-dev && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

# First we build an empty rust project to cache dependencies
# this way we skip dependencies build when only the source code changes
RUN cargo init app
COPY Cargo.toml Cargo.lock /app/
RUN cd app && cargo build --release && \
    rm -rf app/src

# Now we copy the source code and build the actual application
WORKDIR /app
COPY ./src ./src

# Build the application
RUN cargo build --release

## Create a minimal runtime image for binaries
FROM debian:bookworm-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends libcfitsio-dev tar

# Copy the built executables from the builder stage
COPY --from=builder /app/target/release/add_ascii_catalog /app/add_ascii_catalog
COPY --from=builder /app/target/release/add_parquet_catalog /app/add_parquet_catalog
COPY --from=builder /app/target/release/add_csv_catalog /app/add_csv_catalog
COPY --from=builder /app/target/release/add_fits_catalog /app/add_fits_catalog

# The entrypoint should just keep the container running forever
ENTRYPOINT ["tail", "-f", "/dev/null"]
