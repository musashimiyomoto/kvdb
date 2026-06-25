# ---- Build stage ------------------------------------------------------------
# Compile a fully static-ish release binary against the matching Rust toolchain.
FROM rust:1.96-slim AS builder

WORKDIR /app

# Cache dependencies first: copy only the manifest, build a stub, then the
# real sources. This way `cargo` only re-downloads/re-builds deps when
# Cargo.toml changes, not on every source edit.
COPY Cargo.toml ./
RUN mkdir -p src/bin \
    && echo "fn main() {}" > src/bin/server.rs \
    && echo "fn main() {}" > src/bin/client.rs \
    && echo "" > src/lib.rs \
    && cargo build --release --bin kvdb-server \
    && rm -rf src

# Now copy the real source and build for real. `touch` bumps the mtimes so
# cargo's fingerprinting can't mistake the real sources for the stub it just
# compiled and skip the rebuild.
COPY src ./src
RUN touch src/lib.rs src/bin/server.rs src/bin/client.rs \
    && cargo build --release --bin kvdb-server

# ---- Runtime stage ----------------------------------------------------------
# Minimal image: just the binary and a place to keep the WAL.
FROM debian:bookworm-slim AS runtime

# Run as a non-root user.
RUN useradd --system --create-home --uid 10001 kvdb

# Persisted data lives here; mount a volume at /data to keep it across restarts.
RUN mkdir -p /data && chown kvdb:kvdb /data
VOLUME ["/data"]

COPY --from=builder /app/target/release/kvdb-server /usr/local/bin/kvdb-server

USER kvdb
EXPOSE 6380

# Credentials are required and are NOT baked into the image — pass them at run
# time, e.g. `docker run -e KVDB_USER=admin -e KVDB_PASSWORD=secret ...`.
# The server refuses to start if KVDB_USER / KVDB_PASSWORD are unset.
#
# Bind to 0.0.0.0 so the HTTP server is reachable from outside the container,
# and store the WAL on the /data volume.
CMD ["kvdb-server", "0.0.0.0:6380", "/data/kvdb.wal"]
