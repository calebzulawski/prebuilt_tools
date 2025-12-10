#!/usr/bin/env bash
set -euo pipefail

binary="$1"
output="$("$binary")"

if [[ "$output" != "Hello, world!" ]]; then
    echo "unexpected output: $output" >&2
    exit 1
fi
