#!/bin/sh
# =============================================================================
#  Container entrypoint.
#
#  Exists to solve one specific, extremely common docker-compose failure: the
#  bind-mounted ./data directory is owned by the *host* user, whose UID rarely
#  matches the container's appuser (1000). The container then cannot write, and
#  the pipeline dies with a PermissionError that looks like a code bug but is
#  not.
#
#  The fix is the standard pattern used by the official Postgres and Redis
#  images: start as root, take ownership of the data directory, then drop
#  privileges before exec'ing the real command. The pipeline itself never runs
#  as root.
# =============================================================================
set -e

DATA_DIR="${PATH_DATA_DIR:-/data}"
mkdir -p "$DATA_DIR/raw" "$DATA_DIR/images" "$DATA_DIR/output"

if [ "$(id -u)" = "0" ]; then
    chown -R appuser:appuser "$DATA_DIR" 2>/dev/null || true

    if command -v setpriv >/dev/null 2>&1; then
        exec setpriv --reuid=appuser --regid=appuser --clear-groups "$@"
    elif command -v gosu >/dev/null 2>&1; then
        exec gosu appuser "$@"
    else
        # Neither helper available: warn loudly rather than silently running
        # the download-and-decode-untrusted-bytes process as root.
        echo "WARNING: cannot drop privileges (setpriv/gosu missing); running as root" >&2
        exec "$@"
    fi
fi

# Already non-root (e.g. `docker run --user`). Verify writability now and fail
# with an actionable message instead of a stack trace 30 seconds in.
if [ ! -w "$DATA_DIR" ]; then
    echo "ERROR: $DATA_DIR is not writable by uid $(id -u)." >&2
    echo "       Fix on the host with:  sudo chown -R $(id -u):$(id -g) ./data" >&2
    exit 1
fi

exec "$@"
