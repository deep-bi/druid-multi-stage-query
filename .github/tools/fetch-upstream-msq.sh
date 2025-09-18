#!/usr/bin/env bash
set -euo pipefail
REF="${1:?ref}"; DST="${2:?dst}"
UPSTREAM="https://github.com/apache/druid.git"
MSQ_PATH="extensions-core/multi-stage-query"

# normalize tags like 33.0.0 -> druid-33.0.0
[[ "$REF" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc[0-9]+)?$ ]] && REF="druid-$REF"

rm -rf "$DST"
git init "$DST"
git -C "$DST" remote add origin "$UPSTREAM"
git -C "$DST" config core.sparseCheckout true
echo "$MSQ_PATH" > "$DST/.git/info/sparse-checkout"
git -C "$DST" fetch --depth 1 origin "$REF" || git -C "$DST" fetch --depth 1 origin "refs/tags/$REF"
git -C "$DST" checkout --detach FETCH_HEAD
