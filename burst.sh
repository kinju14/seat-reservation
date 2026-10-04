#!/usr/bin/env bash
# Usage: ADMIN_TOKEN=... ./burst.sh https://your-app.onrender.com [extra burst.py flags]
set -euo pipefail
BASE_URL="${1:?usage: ADMIN_TOKEN=... ./burst.sh <BASE_URL> [flags]}"
cd "$(dirname "$0")"
[ -d .venv ] || python3 -m venv .venv
.venv/bin/pip install -q aiohttp
exec .venv/bin/python burst.py "$BASE_URL" --admin-token "${ADMIN_TOKEN:?set ADMIN_TOKEN}" "${@:2}"
