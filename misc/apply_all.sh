#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

wait_all() {
    local phase="$1"
    shift

    local status=0
    local pid

    for pid in "$@"; do
        if wait "$pid"; then
            :
        else
            status=1
        fi
    done

    if (( status == 0 )); then
        echo "Phase $phase ended successfully"
    else
        echo "Phase $phase failed" >&2
    fi

    return "$status"
}

# BUILD
docker build -t nimfasftw/szakdoga_dispatcher:latest "$SCRIPT_DIR/../dispatcher/." &
p1=$!
docker build -t nimfasftw/szakdoga_deter_resolver:latest "$SCRIPT_DIR/../deter_resolver/." &
p2=$!
docker build -t nimfasftw/szakdoga_llm_resolver:latest "$SCRIPT_DIR/../llm_resolver/." &
p3=$!

wait_all "BUILD" "$p1" "$p2" "$p3"

# PUSH
docker push nimfasftw/szakdoga_dispatcher:latest &
p1=$!
docker push nimfasftw/szakdoga_deter_resolver:latest &
p2=$!
docker push nimfasftw/szakdoga_llm_resolver:latest &
p3=$!

wait_all "PUSH" "$p1" "$p2" "$p3"

# APPLY
kubectl apply -f "$SCRIPT_DIR/../namespace_chaos.yaml"
kubectl apply -f "$SCRIPT_DIR/../namespace_control.yaml"
kubectl apply -f "$SCRIPT_DIR/../prom/promrules.yaml"
kubectl apply -f "$SCRIPT_DIR/../llm_resolver/" &
p1=$!
kubectl apply -f "$SCRIPT_DIR/../deter_resolver/" &
p2=$!
kubectl apply -f "$SCRIPT_DIR/../dispatcher/" &
p3=$!

wait_all "APPLY" "$p1" "$p2" "$p3"

# RESTART
kubectl rollout restart deployment -n control llm-resolver &
p1=$!
kubectl rollout restart deployment -n control deter-resolver &
p2=$!
kubectl rollout restart deployment -n control dispatcher &
p3=$!

wait_all "RESTART" "$p1" "$p2" "$p3"