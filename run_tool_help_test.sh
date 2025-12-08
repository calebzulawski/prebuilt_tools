#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
    echo "usage: $0 <binary>" >&2
    exit 1
fi
"$1" --version >/dev/null
