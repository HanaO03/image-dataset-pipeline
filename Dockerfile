# =============================================================================
#  Image dataset ingestion pipeline
# =============================================================================
#  Multi-stage build: compile wheels in a builder that has a toolchain, then
#  copy only the installed packages into a slim runtime. Keeps the shipped
#  image free of gcc and headers — smaller, and a smaller attack surface.
# =============================================================================

# --- builder -----------------------------------------------------------------
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Build-time only: some wheels (lxml, Pillow) may need a compiler on
# architectures without prebuilt wheels — notably arm64 Macs.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libjpeg-dev \
        zlib1g-dev \
        libxml2-dev \
        libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements alone first: this layer is cached and only invalidated when
# dependencies change, so editing source code does not trigger a reinstall.
COPY requirements.txt /tmp/requirements.txt
RUN pip install --prefix=/install -r /tmp/requirements.txt


# --- runtime -----------------------------------------------------------------
FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Runtime shared libraries only — no compilers, no headers.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libjpeg62-turbo \
        zlib1g \
        libxml2 \
        libxslt1.1 \
        libwebp7 \
        tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

# Run as a non-root user. The pipeline fetches and decodes untrusted bytes from
# the public internet; giving that process root inside the container is an
# unnecessary risk, and image decoders have a long history of CVEs.
RUN groupadd --gid 1000 appuser \
    && useradd --uid 1000 --gid 1000 --create-home appuser

WORKDIR /app
COPY --chown=appuser:appuser sql/ /app/sql/
COPY --chown=appuser:appuser src/ /app/src/
COPY --chown=appuser:appuser docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Created here so they exist with the right owner even when no volume is
# mounted over them.
RUN mkdir -p /data/images /data/output && chown -R appuser:appuser /data

VOLUME ["/data"]

# Note there is deliberately no `USER appuser` here. The entrypoint starts as
# root purely to fix ownership of a bind-mounted ./data (whose host UID we
# cannot know at build time), then drops to appuser via setpriv before exec'ing
# the command. This is the same pattern the official Postgres image uses, and
# it is what makes `docker compose up` work on Linux, macOS and WSL alike
# without the user having to chown anything by hand.
#
# tini reaps zombies and forwards signals, so Ctrl-C on `docker compose up`
# actually stops the pipeline instead of detaching from it.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/entrypoint.sh"]
CMD ["python", "-m", "src.cli", "run"]


# --- test --------------------------------------------------------------------
# A separate stage so the shipped image carries no test code and no pytest,
# while `make test` still needs nothing on the reviewer's machine but Docker.
# Requiring a local Python 3.12 to run the suite would mean, in practice, that
# most reviewers never run it.
FROM runtime AS test

USER root
# Installed from the same pinned file the project uses, rather than a version
# repeated here — two pins drift, one does not.
# Both files: requirements-dev.txt begins with `-r requirements.txt`, which pip
# resolves relative to the file it is reading, not to the working directory.
COPY requirements.txt requirements-dev.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements-dev.txt
COPY --chown=appuser:appuser tests/ /app/tests/
COPY --chown=appuser:appuser scripts/ /app/scripts/

# cache_dir under /tmp: /app is not writable by appuser, and a permissions
# warning on every test run trains people to ignore warnings.
CMD ["python", "-m", "pytest", "tests", "-q", "-o", "cache_dir=/tmp/.pytest_cache"]
