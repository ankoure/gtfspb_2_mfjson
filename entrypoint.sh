#!/bin/sh
set -e

# Fix ownership of mounted volumes
chown -R nonroot:nonroot /app/data /app/logs

# Drop to nonroot user and execute the command
exec gosu nonroot "$@"
