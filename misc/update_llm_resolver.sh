#!/usr/bin/env bash
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
docker build -t nimfasftw/szakdoga_llm_resolver:latest "$SCRIPT_DIR/../llm_resolver/."
docker push nimfasftw/szakdoga_llm_resolver:latest
# first time, or after changing rbac.yaml / deploy.yaml / secret.yaml:
kubectl apply -f "$SCRIPT_DIR/../llm_resolver/"
kubectl rollout restart deployment -n chaos llm-resolver
