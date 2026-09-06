# InfluxDB 3 Core image for the OSRS GE price database.
#
# Built on the official InfluxDB 3 Core image. We add a container HEALTHCHECK
# (the base image ships `curl`) and bake in sensible default `serve` arguments
# so the container can be run directly (`docker run`) or via docker-compose.
#
# The actual runtime values (host bind, data dir, node id, auth mode) are
# provided as build/run args and the CMD below, so they can be overridden
# without editing this file.
FROM quay.io/influxdb/influxdb3-core:latest

# --- Configurable defaults (override at build time with --build-arg) ---
# Port the HTTP API + Flight/gRPC endpoint listens on (same port for both).
ARG INFLUXDB3_PORT=8181
# Logical node id for this single-node Core instance.
ARG INFLUXDB3_NODE_ID=ge-node
# Where the object-store data is persisted INSIDE the container. Mount a
# volume here (compose does this) so data survives container removal.
ARG INFLUXDB3_DATA_DIR=/var/lib/influxdb3

# Persist the chosen values into the image environment so the CMD and
# healthcheck can reference them and operators can inspect them.
ENV INFLUXDB3_PORT=${INFLUXDB3_PORT} \
    INFLUXDB3_NODE_ID=${INFLUXDB3_NODE_ID} \
    INFLUXDB3_DATA_DIR=${INFLUXDB3_DATA_DIR}

# Ensure the data directory exists (the volume mount will overlay it at run).
# The base image runs as the non-root `influxdb3` user; create the dir as root
# then hand ownership back. Fall back gracefully if the user differs.
USER root
RUN mkdir -p "${INFLUXDB3_DATA_DIR}" \
    && (chown -R influxdb3:influxdb3 "${INFLUXDB3_DATA_DIR}" 2>/dev/null || true)
USER influxdb3

EXPOSE ${INFLUXDB3_PORT}

# Container-level health: the server answers 200 OK on /health once ready.
# Uses curl from the base image; --without-auth means no token is needed here.
HEALTHCHECK --interval=15s --timeout=5s --start-period=40s --retries=10 \
  CMD curl -fsS "http://localhost:${INFLUXDB3_PORT}/health" || exit 1

# Default command: run the server bound to all interfaces on the chosen port,
# persisting to the mounted data dir, with local auth disabled. Override the
# CMD (e.g. to enable auth) without changing the image by passing a new command
# in docker-compose or `docker run`.
#
# NOTE: the base image's ENTRYPOINT is /usr/bin/entrypoint.sh which execs
# `influxdb3`, so CMD supplies the `serve ...` subcommand + flags.
CMD ["serve", \
     "--node-id", "ge-node", \
     "--object-store", "file", \
     "--data-dir", "/var/lib/influxdb3", \
     "--http-bind", "0.0.0.0:8181", \
     "--without-auth"]
