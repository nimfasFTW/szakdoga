#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

kubectl delete -f "$SCRIPT_DIR/../llm_resolver/" 
kubectl delete -f "$SCRIPT_DIR/../deter_resolver/" 
kubectl delete -f "$SCRIPT_DIR/../dispatcher/" 
kubectl delete -f "$SCRIPT_DIR/../chaos/k8s_yamls/" 
