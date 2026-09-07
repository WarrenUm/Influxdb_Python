# osrsGECurrentProcess: keeps the InfluxDB 3 store current with the latest OSRS
# GE prices by running ge-pipeline catch-up ingestion on a 5-minute timer.
#
# This image bundles the `ge_pipeline` package and runs its APScheduler daemon
# (`python -m ge_pipeline.scheduler`), which fires `run_catch_up` every 5 minutes.
# Each run detects the latest stored timestamp and ingests every missing
# 5-minute window up to now, so a run after a failure naturally backfills the gap.
#
# IMPORTANT: the build context is the REPOSITORY ROOT (see the `build.context`
# in docker-compose.yml), not this folder, so the whole `ge_pipeline` package and
# `pyproject.toml` are available to COPY/install.

FROM python:3.12-slim

# Keep Python output unbuffered so container logs stream in real time, and skip
# writing .pyc files into the image.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Install the package first with only the metadata needed to resolve deps, then
# the source. Copying pyproject.toml alone would still need the package dir for a
# build, so we copy both and install in one layer. Deps are pinned in
# pyproject.toml, so this is reproducible.
COPY pyproject.toml README.md ./
COPY ge_pipeline ./ge_pipeline

RUN pip install .

# Run as a non-root user for safety.
RUN useradd --create-home --uid 10001 appuser
USER appuser

# The scheduler is a long-running, signal-aware daemon: it runs catch-up
# ingestion every 5 minutes until it receives SIGINT/SIGTERM (which Docker sends
# on `docker stop`), then shuts down gracefully.
CMD ["python", "-m", "ge_pipeline.scheduler"]
