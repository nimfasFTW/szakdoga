from __future__ import annotations

import logging
from typing import Any, Optional

from kubernetes import client, config
from kubernetes.client.exceptions import ApiException

from signature import Facts, normalize_waiting_reason

logger = logging.getLogger("dispatcher.enrich")


def _pick_container_status(statuses: list[dict], container_hint: Optional[str]) -> Optional[dict]:
    if not statuses:
        return None
    if container_hint:
        for cs in statuses:
            if cs.get("name") == container_hint:
                return cs
    not_ready = [cs for cs in statuses if not cs.get("ready")]
    return (not_ready or statuses)[0]


def facts_from_pod(pod: dict, alertname: str, container_hint: Optional[str],
                   workload_kind: Optional[str], workload_name: Optional[str]) -> Facts:
    meta = pod.get("metadata", {})
    status = pod.get("status", {})
    cs = _pick_container_status(status.get("containerStatuses") or [], container_hint)

    waiting_reason = termination_reason = exit_code = None
    restart_count, ready = 0, None
    if cs is not None:
        state = cs.get("state") or {}
        last_state = cs.get("lastState") or {}
        waiting_reason = normalize_waiting_reason((state.get("waiting") or {}).get("reason"))
        # The most recent termination: the current state if the container has just died,
        # otherwise the previous run (the usual case while a pod sits in CrashLoopBackOff).
        terminated = state.get("terminated") or last_state.get("terminated") or {}
        termination_reason = terminated.get("reason")
        exit_code = terminated.get("exitCode")
        restart_count = cs.get("restartCount", 0)
        ready = cs.get("ready")

    return Facts(
        alertname=alertname,
        namespace=meta.get("namespace", ""),
        pod=meta.get("name", ""),
        container=cs.get("name") if cs else container_hint,
        pod_found=True,
        phase=status.get("phase"),
        workload_kind=workload_kind,
        workload_name=workload_name,
        waiting_reason=waiting_reason,
        termination_reason=termination_reason,
        exit_code=exit_code,
        restart_count=restart_count,
        ready=ready,
    )


class PodInspector:
    def __init__(self, core_v1: Any, apps_v1: Any):
        self._core = core_v1
        self._apps = apps_v1

    @classmethod
    def from_cluster(cls) -> "PodInspector":
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()   # local development
        return cls(client.CoreV1Api(), client.AppsV1Api())

    def inspect(self, alertname: str, namespace: str, pod_name: str,
                container_hint: Optional[str] = None) -> Facts:
        try:
            pod_obj = self._core.read_namespaced_pod(pod_name, namespace)
        except ApiException as e:
            if e.status == 404: 
                return Facts(alertname=alertname, namespace=namespace, pod=pod_name,
                             container=container_hint, pod_found=False)
            raise
        pod = self._core.api_client.sanitize_for_serialization(pod_obj)
        kind, name = self._resolve_workload(namespace, pod)
        return facts_from_pod(pod, alertname, container_hint, kind, name)

    def _resolve_workload(self, namespace: str, pod: dict) -> tuple[str, str]:
        """Pod -> ReplicaSet -> Deployment."""
        pod_name = pod["metadata"]["name"]
        owners = pod["metadata"].get("ownerReferences") or []
        ctrl = next((o for o in owners if o.get("controller")), None)
        if ctrl is None:
            return "Pod", pod_name
        if ctrl["kind"] != "ReplicaSet":
            return ctrl["kind"], ctrl["name"]
        try:
            rs = self._apps.read_namespaced_replica_set(ctrl["name"], namespace)
        except ApiException as e:
            if e.status == 404:
                return "ReplicaSet", ctrl["name"]
            raise
        dep = next((o for o in (rs.metadata.owner_references or []) if o.kind == "Deployment"), None)
        return ("Deployment", dep.name) if dep else ("ReplicaSet", ctrl["name"])
