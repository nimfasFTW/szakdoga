#!/usr/bin/env bash
set -euo pipefail
kubectl -n chaos port-forward deploy/uc8-server 5000:5000 >/dev/null &
PF=$!; trap "kill $PF" EXIT; sleep 2
curl -s -XPOST localhost:5000/hang; echo