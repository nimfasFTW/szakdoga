#!/usr/bin/env bash
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
docker build -t nimfasftw/szakdoga_dispatcher:latest "$SCRIPT_DIR/../dispatcher/."
docker push nimfasftw/szakdoga_dispatcher:latest
kubectl apply -f "$SCRIPT_DIR/../dispatcher/rbac.yaml"
kubectl rollout restart deployment -n chaos dispatcher

