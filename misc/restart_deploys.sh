#!/usr/bin/env bash
# https://gist.github.com/akrasic/380bda362e0420be08709152c91ca1f9
set -euo pipefail
kubectl rollout restart deployment -n chaos deter-resolver
kubectl rollout restart deployment -n chaos dispatcher
