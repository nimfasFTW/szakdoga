"""The only module that talks to the Kubernetes API. Everything else works on plain dicts,
so the tools and the verification can be exercised against a fake cluster.

Superset of deter_resolver/cluster.py: the deterministic resolver only ever patches a Deployment,
the agent also has to read logs / events / ConfigMaps / Services and fix the last two."""
from __future__ import annotations

from typing import Any, Optional, Protocol

from kubernetes import client, config

STRATEGIC_MERGE = "application/strategic-merge-patch+json"


class Cluster(Protocol):
    # --- read ---
    def get_deployment(self, namespace: str, name: str) -> dict: ...
    def list_deployments(self, namespace: str) -> list[dict]: ...
    def list_pods(self, namespace: str, label_selector: str) -> list[dict]: ...
    def read_pod_log(self, namespace: str, pod: str, container: Optional[str],
                     previous: bool, tail_lines: int) -> str: ...
    def list_events(self, namespace: str) -> list[dict]: ...
    def get_configmap(self, namespace: str, name: str) -> dict: ...
    def list_services(self, namespace: str) -> list[dict]: ...
    # --- write (strategic merge patches) ---
    def patch_deployment(self, namespace: str, name: str, body: dict) -> None: ...
    def patch_configmap(self, namespace: str, name: str, body: dict) -> None: ...
    def patch_service(self, namespace: str, name: str, body: dict) -> None: ...


class KubeCluster:
    def __init__(self, apps_v1: Any, core_v1: Any):
        self._apps = apps_v1
        self._core = core_v1

    @classmethod
    def from_cluster(cls) -> "KubeCluster":
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()   # local development
        return cls(client.AppsV1Api(), client.CoreV1Api())

    def _plain(self, obj: Any) -> dict:
        return self._apps.api_client.sanitize_for_serialization(obj)

    # ---------- read ----------
    def get_deployment(self, namespace: str, name: str) -> dict:
        return self._plain(self._apps.read_namespaced_deployment(name, namespace))

    def list_deployments(self, namespace: str) -> list[dict]:
        return [self._plain(d) for d in self._apps.list_namespaced_deployment(namespace).items]

    def list_pods(self, namespace: str, label_selector: str) -> list[dict]:
        pods = self._core.list_namespaced_pod(namespace, label_selector=label_selector)
        return [self._plain(p) for p in pods.items]

    def read_pod_log(self, namespace: str, pod: str, container: Optional[str],
                     previous: bool, tail_lines: int) -> str:
        return self._core.read_namespaced_pod_log(
            pod, namespace, container=container, previous=previous, tail_lines=tail_lines)

    def list_events(self, namespace: str) -> list[dict]:
        return [self._plain(e) for e in self._core.list_namespaced_event(namespace).items]

    def get_configmap(self, namespace: str, name: str) -> dict:
        return self._plain(self._core.read_namespaced_config_map(name, namespace))

    def list_services(self, namespace: str) -> list[dict]:
        return [self._plain(s) for s in self._core.list_namespaced_service(namespace).items]

    # ---------- write ----------
    # strategic merge: lists with a merge key (containers, env, service ports) are merged by that
    # key instead of replaced, and a key set to null is removed
    def patch_deployment(self, namespace: str, name: str, body: dict) -> None:
        self._apps.patch_namespaced_deployment(name, namespace, body, _content_type=STRATEGIC_MERGE)

    def patch_configmap(self, namespace: str, name: str, body: dict) -> None:
        self._core.patch_namespaced_config_map(name, namespace, body, _content_type=STRATEGIC_MERGE)

    def patch_service(self, namespace: str, name: str, body: dict) -> None:
        self._core.patch_namespaced_service(name, namespace, body, _content_type=STRATEGIC_MERGE)
