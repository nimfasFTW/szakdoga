#!/usr/bin/env bash
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
docker build -t nimfasftw/szakdoga_deter_resolver:latest "$SCRIPT_DIR/../deter_resolver/."
docker push nimfasftw/szakdoga_deter_resolver:latest
# kubectl apply -f "../deter_resolver/rbac.yaml"
kubectl rollout restart deployment -n chaos deter-resolver

