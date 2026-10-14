"""The tools the agent can call.

Design:
  * Every tool is bound to ONE namespace (the one of the incident) when the toolkit is built. The
    model never passes a namespace, so it cannot wander off and has fewer arguments to get wrong.
  * Tools never raise. A failure (404, the API rejecting a patch, bad arguments) comes back as an
    'ERROR: ...' string, because that is information the agent can act on.
  * Every call is logged and recorded. The recording is what ends up in the verdict, and
    `changes` / `last_change_at` tell the resolver whether the agent touched the cluster at all.
  * Health checking reuses verify.wait_until_healthy: the agent is told 'healthy' by exactly the
    same code that later decides the verdict.
"""
from __future__ import annotations

import functools
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from kubernetes.client.exceptions import ApiException
from langchain_core.tools import tool

from clock import Clock
from cluster import Cluster
from verify import wait_until_healthy

logger = logging.getLogger("llm_resolver.tools")

MAX_TOOL_OUTPUT_CHARS = 12_000
MAX_LOG_LINES = 300
MAX_EVENTS = 40

# fields the API server fills in with defaults; pure noise for the model
_NOISY_CONTAINER_KEYS = ("terminationMessagePath", "terminationMessagePolicy")
_NOISY_POD_SPEC_KEYS = ("dnsPolicy", "schedulerName", "terminationGracePeriodSeconds")


# ---------------------------------------------------------------------------------------------
# pure helpers (dict in, dict out)
# ---------------------------------------------------------------------------------------------
def describe_error(e: Exception) -> str:
    if isinstance(e, ApiException):
        message = e.reason
        try:
            message = json.loads(e.body).get("message") or message
        except Exception:
            pass
        return f"Kubernetes API {e.status}: {message}"
    return f"{type(e).__name__}: {e}"


def clip(text: str, limit: int = MAX_TOOL_OUTPUT_CHARS, keep: str = "start") -> str:
    if len(text) <= limit:
        return text
    cut = len(text) - limit
    if keep == "end":   # logs: the interesting part is the tail
        return f"[... {cut} chars cut ...]\n" + text[-limit:]
    return text[:limit] + f"\n[... {cut} chars cut ...]"


def _compact(d: dict) -> dict:
    return {k: v for k, v in d.items() if v not in (None, [], {})}


def selector_string(match_labels: dict) -> str:
    return ",".join(f"{k}={v}" for k, v in sorted(match_labels.items()))


def _pod_spec(dep: dict) -> dict:
    return dep["spec"]["template"]["spec"]


def pick_container(dep: dict, name: Optional[str]) -> dict:
    containers = _pod_spec(dep)["containers"]
    if name:
        for c in containers:
            if c["name"] == name:
                return c
        raise ValueError(f"container {name!r} not found; the Deployment has: {[c['name'] for c in containers]}")
    if len(containers) == 1:
        return containers[0]
    raise ValueError(f"the Deployment has several containers, pass one of: {[c['name'] for c in containers]}")


def trim_deployment(dep: dict) -> dict:
    spec = dep["spec"]
    pod_spec = {k: v for k, v in _pod_spec(dep).items() if k not in _NOISY_POD_SPEC_KEYS}
    if not pod_spec.get("securityContext"):
        pod_spec.pop("securityContext", None)
    for key in ("containers", "initContainers"):
        if key in pod_spec:
            pod_spec[key] = [{k: v for k, v in c.items() if k not in _NOISY_CONTAINER_KEYS}
                             for c in pod_spec[key]]
    st = dep.get("status") or {}
    return {
        "name": dep["metadata"]["name"],
        "generation": dep["metadata"].get("generation"),
        "replicas": spec.get("replicas", 1),
        "selector": spec.get("selector", {}).get("matchLabels"),
        "pod_spec": pod_spec,
        "status": _compact({
            "observedGeneration": st.get("observedGeneration"),
            "replicas": st.get("replicas"), "updatedReplicas": st.get("updatedReplicas"),
            "readyReplicas": st.get("readyReplicas"), "availableReplicas": st.get("availableReplicas"),
            "unavailableReplicas": st.get("unavailableReplicas"),
            "conditions": [_compact({k: c.get(k) for k in ("type", "status", "reason", "message")})
                           for c in st.get("conditions") or []],
        }),
    }


def referenced_configmaps(dep: dict) -> list[str]:
    ps = _pod_spec(dep)
    names: set[str] = set()
    for c in (ps.get("containers") or []) + (ps.get("initContainers") or []):
        for ef in c.get("envFrom") or []:
            if (ef.get("configMapRef") or {}).get("name"):
                names.add(ef["configMapRef"]["name"])
        for ev in c.get("env") or []:
            ref = (ev.get("valueFrom") or {}).get("configMapKeyRef") or {}
            if ref.get("name"):
                names.add(ref["name"])
    for v in ps.get("volumes") or []:
        if (v.get("configMap") or {}).get("name"):
            names.add(v["configMap"]["name"])
    return sorted(names)


def summarize_deployment_line(dep: dict) -> dict:
    st = dep.get("status") or {}
    return {
        "name": dep["metadata"]["name"],
        "replicas_desired": dep["spec"].get("replicas", 1),
        "replicas_ready": st.get("readyReplicas", 0),
        "images": {c["name"]: c["image"] for c in _pod_spec(dep)["containers"]},
        "configmaps": referenced_configmaps(dep),
    }


def _terminated(t: dict) -> dict:
    return _compact({"reason": t.get("reason"), "exitCode": t.get("exitCode"),
                     "message": t.get("message"), "finishedAt": t.get("finishedAt")})


def summarize_pod(pod: dict) -> dict:
    meta, st = pod["metadata"], pod.get("status") or {}
    containers = []
    for cs in st.get("containerStatuses") or []:
        state = cs.get("state") or {}
        entry = {"name": cs["name"], "image": cs.get("image"), "ready": cs.get("ready"),
                 "restarts": cs.get("restartCount", 0)}
        if state.get("waiting"):
            entry["waiting"] = _compact({"reason": state["waiting"].get("reason"),
                                         "message": state["waiting"].get("message")})
        elif state.get("terminated"):
            entry["terminated"] = _terminated(state["terminated"])
        elif state.get("running"):
            entry["running_since"] = state["running"].get("startedAt")
        last = (cs.get("lastState") or {}).get("terminated")
        if last:
            entry["last_terminated"] = _terminated(last)
        containers.append(_compact(entry))
    unmet = [_compact({k: c.get(k) for k in ("type", "status", "reason", "message")})
             for c in st.get("conditions") or [] if c.get("status") != "True"]
    return _compact({"name": meta["name"], "phase": st.get("phase"), "started": st.get("startTime"),
                     "terminating": bool(meta.get("deletionTimestamp")) or None,
                     "containers": containers, "unmet_conditions": unmet})


def _event_time(e: dict) -> str:
    return e.get("lastTimestamp") or e.get("eventTime") or (e.get("metadata") or {}).get("creationTimestamp") or ""


def summarize_event(e: dict) -> dict:
    obj = e.get("involvedObject") or {}
    return _compact({"time": _event_time(e), "type": e.get("type"), "reason": e.get("reason"),
                     "object": f"{obj.get('kind', '?')}/{obj.get('name', '?')}",
                     "count": e.get("count"), "message": e.get("message")})


def summarize_service(svc: dict, matching_pods: list[dict]) -> dict:
    spec = svc["spec"]
    ready = sum(1 for p in matching_pods
                if any(c.get("type") == "Ready" and c.get("status") == "True"
                       for c in (p.get("status") or {}).get("conditions") or []))
    return _compact({
        "name": svc["metadata"]["name"], "type": spec.get("type"), "clusterIP": spec.get("clusterIP"),
        "selector": spec.get("selector"),
        "ports": [_compact({k: p.get(k) for k in ("name", "port", "targetPort", "protocol")})
                  for p in spec.get("ports") or []],
        "pods_matching_selector": len(matching_pods), "pods_ready": ready,
    })


def _j(obj: Any) -> str:
    return json.dumps(obj, indent=1, sort_keys=False, default=str)


# ---------------------------------------------------------------------------------------------
# the toolkit
# ---------------------------------------------------------------------------------------------
class Toolkit:
    """One instance per incident. `tools` goes to the agent; the rest is the paper trail."""

    def __init__(self, cluster: Cluster, namespace: str, *, stable_s: float, poll_s: float,
                 clock: Clock, max_wait_s: float = 90.0) -> None:
        self.cluster = cluster
        self.namespace = namespace
        self.stable_s = stable_s
        self.poll_s = poll_s
        self.clock = clock
        self.max_wait_s = max_wait_s
        self.calls: list[dict] = []        # every tool invocation, in order
        self.changes: list[dict] = []      # successful modifications of the cluster
        self.last_change_at: Optional[float] = None
        self.tools = self._build()

    # ---------- bookkeeping ----------
    def _changed(self, tool_name: str, summary: str) -> None:
        self.changes.append({"tool": tool_name, "change": summary})
        self.last_change_at = self.clock.now()

    def _register(self, fn: Callable[..., Any]):
        """Turn a plain function into a LangChain tool that never raises and leaves a record."""
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                out, ok = fn(*args, **kwargs), True
            except Exception as e:
                out, ok = f"ERROR: {describe_error(e)}", False
            out = clip(out if isinstance(out, str) else _j(out),
                       keep="end" if fn.__name__ == "get_pod_logs" else "start")
            self.calls.append({"tool": fn.__name__, "args": kwargs, "ok": ok})
            logger.info("tool %s %s -> %s%s", fn.__name__, json.dumps(kwargs, default=str),
                        "ok" if ok else "FAILED", "" if ok else f" ({out[:300]})")
            return out
        return tool(parse_docstring=True)(wrapper)

    # ---------- the tools ----------
    def _build(self) -> list:
        c, ns = self.cluster, self.namespace
        reg = self._register

        # ============================ read ============================
        @reg
        def list_workloads() -> str:
            """List every Deployment in the namespace with desired/ready replicas, container images and the ConfigMaps it uses.

            Start here to see what else runs next to the affected workload; a dependency of the
            alerting workload is usually one of these.
            """
            return _j([summarize_deployment_line(d) for d in c.list_deployments(ns)])

        @reg
        def get_deployment(deployment: str) -> str:
            """Show a Deployment's full pod template (images, env, envFrom, resources, probes, ports) and its rollout status and conditions.

            Args:
                deployment: Name of the Deployment.
            """
            return _j(trim_deployment(c.get_deployment(ns, deployment)))

        @reg
        def get_pods(deployment: str) -> str:
            """Show the pods of a Deployment: phase, per-container state, restart count, waiting/termination reason and exit code, and unmet pod conditions.

            Args:
                deployment: Name of the Deployment whose pods to show.
            """
            dep = c.get_deployment(ns, deployment)
            pods = c.list_pods(ns, selector_string(dep["spec"]["selector"].get("matchLabels", {})))
            return _j([summarize_pod(p) for p in pods])

        @reg
        def get_pod_logs(pod: str, container: Optional[str] = None, previous: bool = False,
                         tail_lines: int = 80) -> str:
            """Read the last log lines of a container.

            For a container that keeps crashing, the current container often has no useful output
            yet: set previous=True to read the logs of the run that crashed.

            Args:
                pod: Exact pod name (get it from get_pods).
                container: Container name. Optional when the pod has only one container.
                previous: Read the logs of the previous (crashed) container instance instead of the current one.
                tail_lines: How many lines from the end to return (max 300).
            """
            lines = max(1, min(int(tail_lines), MAX_LOG_LINES))
            text = c.read_pod_log(ns, pod, container, bool(previous), lines)
            return text if text.strip() else "(no log output)"

        @reg
        def get_events(name_prefix: Optional[str] = None) -> str:
            """Show the most recent Kubernetes events in the namespace, newest last.

            Events explain scheduling, image pull, probe and OOM problems that the pod status
            alone does not.

            Args:
                name_prefix: Only events whose object name starts with this, e.g. a Deployment name (this also matches its ReplicaSets and pods). Omit for all events.
            """
            events = c.list_events(ns)
            if name_prefix:
                events = [e for e in events
                          if str((e.get("involvedObject") or {}).get("name", "")).startswith(name_prefix)]
            events.sort(key=_event_time)
            return _j([summarize_event(e) for e in events[-MAX_EVENTS:]]) if events else "(no events)"

        @reg
        def get_configmap(configmap: str) -> str:
            """Show the key/value data of a ConfigMap.

            Args:
                configmap: Name of the ConfigMap.
            """
            cm = c.get_configmap(ns, configmap)
            return _j({"name": cm["metadata"]["name"], "data": cm.get("data") or {}})

        @reg
        def list_services() -> str:
            """List the Services in the namespace: selector, ports (port -> targetPort), and how many pods currently match the selector and are Ready.

            Use it to check that a Service points at the right pods and the right port, and to
            find the port a dependency actually listens on.
            """
            out = []
            for svc in c.list_services(ns):
                selector = (svc["spec"].get("selector") or {})
                pods = c.list_pods(ns, selector_string(selector)) if selector else []
                out.append(summarize_service(svc, pods))
            return _j(out)

        # ============================ check ============================
        @reg
        def check_health(deployment: str, wait_s: int = 30) -> str:
            """Check whether a Deployment is healthy, waiting up to wait_s seconds for it to become so.

            Healthy means: rollout finished, every replica Ready, and no container restarted for a
            stable period. This is the same check that judges your work when you finish, so
            healthy=false means the problem is NOT fixed yet. Call it after every change.

            Args:
                deployment: Name of the Deployment to check.
                wait_s: Maximum seconds to wait (0 = just look now, max 90).
            """
            wait = max(0.0, min(float(wait_s), self.max_wait_s))
            return _j(wait_until_healthy(c, ns, deployment, timeout_s=wait, stable_s=self.stable_s,
                                         poll_s=self.poll_s, clock=self.clock))

        # ============================ write ============================
        @reg
        def set_configmap_value(configmap: str, key: str, value: str) -> str:
            """Set one key of a ConfigMap to a new value (the key is created if it does not exist).

            IMPORTANT: environment variables that come from a ConfigMap are read only when a
            container starts. Running pods keep the old value until their Deployment is restarted
            (restart_deployment). The result lists the Deployments that use this ConfigMap.

            Args:
                configmap: Name of the ConfigMap.
                key: Key to set.
                value: New value (always a string, e.g. "5000").
            """
            old = (c.get_configmap(ns, configmap).get("data") or {}).get(key)
            if old == value:
                return f"No change: {configmap}[{key}] is already {value!r}."
            c.patch_configmap(ns, configmap, {"data": {key: value}})
            self._changed("set_configmap_value", f"{configmap}[{key}]: {old!r} -> {value!r}")
            users = [d["metadata"]["name"] for d in c.list_deployments(ns)
                     if configmap in referenced_configmaps(d)]
            return _j({"updated": f"{configmap}[{key}]", "old": old, "new": value,
                       "used_by_deployments": users,
                       "note": "running pods still have the old value until the Deployment is restarted"})

        @reg
        def set_container_image(deployment: str, image: str, container: Optional[str] = None) -> str:
            """Change the image of a container in a Deployment. This starts a new rollout.

            You cannot see the registry, so only use a tag you are confident exists.

            Args:
                deployment: Name of the Deployment.
                image: Full image reference, e.g. "nginx:1.27-alpine".
                container: Container name. Optional when the Deployment has only one container.
            """
            target = pick_container(c.get_deployment(ns, deployment), container)
            if target["image"] == image:
                return f"No change: {target['name']} already uses {image}."
            c.patch_deployment(ns, deployment, {"spec": {"template": {"spec": {"containers": [
                {"name": target["name"], "image": image}]}}}})
            self._changed("set_container_image", f"{deployment}/{target['name']}: {target['image']} -> {image}")
            return f"Image of {deployment}/{target['name']} changed from {target['image']} to {image}; a rollout has started."

        @reg
        def set_container_resources(deployment: str, container: Optional[str] = None,
                                    memory_limit: Optional[str] = None, memory_request: Optional[str] = None,
                                    cpu_limit: Optional[str] = None, cpu_request: Optional[str] = None) -> str:
            """Change resource limits/requests of a container in a Deployment. This starts a new rollout. Only the values you pass are changed.

            Args:
                deployment: Name of the Deployment.
                container: Container name. Optional when the Deployment has only one container.
                memory_limit: New memory limit, e.g. "256Mi".
                memory_request: New memory request, e.g. "128Mi". Must not exceed the limit.
                cpu_limit: New CPU limit, e.g. "500m".
                cpu_request: New CPU request, e.g. "250m". Must not exceed the limit.
            """
            limits = {k: v for k, v in (("memory", memory_limit), ("cpu", cpu_limit)) if v}
            requests = {k: v for k, v in (("memory", memory_request), ("cpu", cpu_request)) if v}
            if not limits and not requests:
                raise ValueError("pass at least one of memory_limit, memory_request, cpu_limit, cpu_request")
            target = pick_container(c.get_deployment(ns, deployment), container)
            before = target.get("resources") or {}
            resources = _compact({"limits": limits, "requests": requests})
            c.patch_deployment(ns, deployment, {"spec": {"template": {"spec": {"containers": [
                {"name": target["name"], "resources": resources}]}}}})
            self._changed("set_container_resources", f"{deployment}/{target['name']}: {before} + {resources}")
            return _j({"updated": f"{deployment}/{target['name']}", "before": before, "applied": resources,
                       "note": "a rollout has started"})

        @reg
        def set_container_env(deployment: str, name: str, value: str, container: Optional[str] = None) -> str:
            """Set a literal environment variable directly on a container in a Deployment (added if missing). This starts a new rollout.

            For values that come from a ConfigMap, fix the ConfigMap instead.

            Args:
                deployment: Name of the Deployment.
                name: Environment variable name.
                value: New value (a string).
                container: Container name. Optional when the Deployment has only one container.
            """
            target = pick_container(c.get_deployment(ns, deployment), container)
            old = next((e.get("value") for e in target.get("env") or [] if e["name"] == name), None)
            if old == value:
                return f"No change: {name} is already {value!r} on {deployment}/{target['name']}."
            c.patch_deployment(ns, deployment, {"spec": {"template": {"spec": {"containers": [
                {"name": target["name"], "env": [{"name": name, "value": value}]}]}}}})
            self._changed("set_container_env", f"{deployment}/{target['name']} env {name}: {old!r} -> {value!r}")
            return f"Env {name} on {deployment}/{target['name']} changed from {old!r} to {value!r}; a rollout has started."

        @reg
        def patch_service(service: str, spec_patch_json: str) -> str:
            """Fix a Service by merging a JSON patch into its spec.

            Merge rules: 'selector' keys are merged (set a key to null to remove it); entries of
            'ports' are matched by their 'port' number, so
            {"ports": [{"port": 5000, "targetPort": 5001}]} changes only the targetPort of the
            existing port 5000.

            Args:
                service: Name of the Service.
                spec_patch_json: JSON object to merge into spec, as a string, e.g. '{"selector": {"app": "db"}}'.
            """
            patch = json.loads(spec_patch_json)
            if not isinstance(patch, dict) or not patch:
                raise ValueError("spec_patch_json must be a non-empty JSON object")
            c.patch_service(ns, service, {"spec": patch})
            self._changed("patch_service", f"{service}: merged {json.dumps(patch)}")
            return f"Service {service} patched with {json.dumps(patch)}. Use list_services to confirm the result."

        @reg
        def restart_deployment(deployment: str) -> str:
            """Restart all pods of a Deployment with a rolling restart (like 'kubectl rollout restart').

            Use it to make pods pick up a changed ConfigMap, or to recover a workload whose
            process is stuck while its configuration is fine.

            Args:
                deployment: Name of the Deployment.
            """
            stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
            c.patch_deployment(ns, deployment, {"spec": {"template": {"metadata": {"annotations": {
                "kubectl.kubernetes.io/restartedAt": stamp}}}}})
            self._changed("restart_deployment", deployment)
            return f"Rolling restart of {deployment} started."

        return [list_workloads, get_deployment, get_pods, get_pod_logs, get_events, get_configmap,
                list_services, check_health, set_configmap_value, set_container_image,
                set_container_resources, set_container_env, patch_service, restart_deployment]
