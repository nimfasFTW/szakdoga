from __future__ import annotations

from typing import Any, Protocol

from kubernetes import client, config

STRATEGIC_MERGE = "application/strategic-merge-patch+json"

class Cluster(Protocol):
    def get_deployment(self, namespace: str, name: str) -> dict: ...
    def patch_deployment(self, namespace: str, name: str, body: dict) -> None: ...
    def list_pods(self, namespace: str, label_selector: str) -> list[dict]: ...


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

    def get_deployment(self, namespace: str, name: str) -> dict:
        return self._plain(self._apps.read_namespaced_deployment(name, namespace))

    def patch_deployment(self, namespace: str, name: str, body: dict) -> None:
        self._apps.patch_namespaced_deployment(name, namespace, body, _content_type=STRATEGIC_MERGE)

    def list_pods(self, namespace: str, label_selector: str) -> list[dict]:
        pods = self._core.list_namespaced_pod(namespace, label_selector=label_selector)
        return [self._plain(p) for p in pods.items]
