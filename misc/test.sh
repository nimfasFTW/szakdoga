#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)

NAMESPACE="chaos"
MODE="llm"
UC_DIR="$SCRIPT_DIR/../chaos/k8s_yamls"

source "$SCRIPT_DIR/apply_all.sh"

# max time allowed for each uc
declare -A WAIT_TIME=(
  [uc1]=60
  [uc2]=120
  [uc3]=150
  [uc4]=180
  [uc6]=150
  [uc7]=90
  [uc8]=120
#   [uc8]=320
  [uc9]=320
)

UCS=(uc1 uc2 uc3 uc4 uc6 uc7 uc8 uc9)

cleanup() {
    echo "Cleaning UC deployments from $NAMESPACE..."

    kubectl delete -f "$UC_DIR" --ignore-not-found=true

    echo "Cleanup complete."
}

run_uc() {
    local uc="$1"
    local wait_time="${WAIT_TIME[$uc]}"

    echo
    echo "=================================="
    echo "Starting $uc"
    echo "=================================="

    cleanup

    echo "Applying $uc..."

    if [[ "$uc" == "uc8" ]]; then
        kubectl apply -f "$UC_DIR/$uc.yaml" \
            --namespace="$NAMESPACE"
        
        sleep 20

        source "$SCRIPT_DIR/trigger_uc8_server_error.sh"
    fi


    if [[ "$uc" == "uc9" ]]; then
        kubectl apply -f "$UC_DIR/$uc.yaml" \
            --namespace="$NAMESPACE"

        sleep 20

        kubectl apply -f "$UC_DIR/${uc}_load.yaml" \
            --namespace="$NAMESPACE"
    else
        kubectl apply -f "$UC_DIR/$uc.yaml" \
            --namespace="$NAMESPACE"
    fi

    echo "Giving resolver up to ${wait_time}s..."
    sleep "$wait_time"

    echo "$uc observation window finished."

    cleanup

    echo "Finished $uc"
}


trap cleanup EXIT

for uc in "${UCS[@]}"; do
    run_uc "$uc"
done

echo
echo "All UC tests completed."


EVPATH="$SCRIPT_DIR/../evidence/$(date +%Y%m%d_%H%M)_$MODE"

echo "Saving logs under $EVPATH"

mkdir -p "$EVPATH"

kubectl logs -n control deploy/dispatcher > "$EVPATH/dispatcher.log"

kubectl logs -n control deploy/deter-resolver > "$EVPATH/deter_resolver.log"

kubectl logs -n control deploy/llm-resolver > "$EVPATH/llm_resolver.log"
