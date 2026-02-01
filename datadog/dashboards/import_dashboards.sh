#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load environment variables from .env file
if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
else
    echo "Error: .env file not found at $PROJECT_ROOT/.env"
    exit 1
fi

# Check for required environment variables
if [ -z "$DD_API_KEY" ]; then
    echo "Error: DD_API_KEY not set in .env file"
    exit 1
fi

if [ -z "$DD_APP_KEY" ]; then
    echo "Error: DD_APP_KEY not set in .env file"
    echo "You need an Application Key with 'dashboards_write' scope."
    echo "Create one at: https://app.datadoghq.com/organization-settings/application-keys"
    exit 1
fi

DD_SITE="${DD_SITE:-datadoghq.com}"
API_URL="https://api.${DD_SITE}/api/v1/dashboard"

echo "Importing dashboards to Datadog (${DD_SITE})..."

for dashboard in "$SCRIPT_DIR"/*.json; do
    if [ -f "$dashboard" ]; then
        name=$(basename "$dashboard")
        echo -n "  Importing $name... "

        response=$(curl -s -w "\n%{http_code}" -X POST "$API_URL" \
            -H "DD-API-KEY: ${DD_API_KEY}" \
            -H "DD-APPLICATION-KEY: ${DD_APP_KEY}" \
            -H "Content-Type: application/json" \
            -d @"$dashboard")

        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')

        if [ "$http_code" -eq 200 ]; then
            dashboard_url=$(echo "$body" | grep -o '"url":"[^"]*"' | cut -d'"' -f4)
            echo "OK"
            if [ -n "$dashboard_url" ]; then
                echo "    -> https://app.${DD_SITE}${dashboard_url}"
            fi
        else
            echo "FAILED (HTTP $http_code)"
            echo "    $body"
        fi
    fi
done

echo "Done!"
