"""Offline tests for the UC9 changes (no cluster, no Prometheus, no LLM needed).

Run from the repository root:   python3 tests/test_uc9.py
Each resolver has modules with the same names (verify.py, cluster.py, ...), so each block runs in a
fresh interpreter state with its own src directory on the path.
"""
import importlib, os, sys, unittest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MODULES = ["actions", "resolver", "verify", "saturation", "tools", "cluster", "clock", "agent", "lookup", "signature"]


def use_src(subdir):
    for m in MODULES:
        sys.modules.pop(m, None)
    sys.path[:] = [p for p in sys.path if not p.startswith(ROOT)]
    sys.path.insert(0, os.path.join(ROOT, subdir, "src"))


class FakeClock:
    def __init__(self): self.t = 0.0
    def now(self): return self.t
    def sleep(self, s): self.t += s


class FakeCluster:
    """One Deployment. `scale()` makes new replicas Ready immediately (the pods start fast in the test)."""
    def __init__(self, replicas=1, ready=True):
        self.replicas, self.ready = replicas, ready
        self.patches = []
    def get_deployment(self, ns, name):
        n = self.replicas
        return {"metadata": {"name": name, "generation": 1},
                "spec": {"replicas": n, "selector": {"matchLabels": {"app": name}},
                         "template": {"spec": {"containers": [{"name": name, "image": "x",
                                                                 "resources": {"limits": {"cpu": "250m"}}}]}}},
                "status": {"observedGeneration": 1, "updatedReplicas": n, "replicas": n,
                           "readyReplicas": n if self.ready else 0}}
    def list_pods(self, ns, selector):
        return [{"metadata": {"name": f"uc9-abc-{i}"}, "status": {"containerStatuses": [{"restartCount": 0}]}}
                for i in range(self.replicas)]
    def patch_deployment(self, ns, name, body):
        self.patches.append(body)
        self.replicas = body["spec"]["replicas"]


class FakeSaturation:
    """CPU utilisation as a function of replicas: load / capacity, like the real scenario (10 rps, 4.5 rps per pod)."""
    def __init__(self, cluster, load=10.0, per_pod=4.5):
        self.cluster, self.load, self.per_pod = cluster, load, per_pod
        self.queries = 0
    def max_cpu_utilisation(self, ns, pods):
        self.queries += 1
        return min(1.0, self.load / (self.per_pod * max(1, len(pods))))


class DeterministicTests(unittest.TestCase):
    def setUp(self):
        use_src("deter_resolver")
        self.actions = importlib.import_module("actions")
        self.resolver = importlib.import_module("resolver")
        self.verify = importlib.import_module("verify")

    def req(self, **params):
        return self.actions.ActionRequest(incident="i", action="scale_deployment", params=params,
                                          namespace="chaos", kind="Deployment", name="uc9")

    def test_scale_adds_replicas(self):
        c = FakeCluster(1)
        out = self.actions.scale_deployment(c, self.req(add_replicas=2, max_replicas=5, timeout_s=90))
        self.assertTrue(out.applied); self.assertEqual(c.replicas, 3); self.assertEqual(out.verify_timeout_s, 90)

    def test_scale_is_capped(self):
        c = FakeCluster(4)
        out = self.actions.scale_deployment(c, self.req(add_replicas=2, max_replicas=5))
        self.assertEqual(c.replicas, 5); self.assertTrue(out.detail["capped"])

    def test_scale_at_ceiling_does_nothing(self):
        c = FakeCluster(5)
        out = self.actions.scale_deployment(c, self.req(add_replicas=2, max_replicas=5))
        self.assertFalse(out.applied); self.assertIsNone(out.verify_timeout_s); self.assertEqual(c.patches, [])

    def test_rejects_bad_params(self):
        with self.assertRaises(self.actions.ActionError):
            self.actions.scale_deployment(FakeCluster(1), self.req(add_replicas=0, max_replicas=5))

    def _wait(self, cluster, sat, timeout=60):
        return self.verify.wait_until_healthy(cluster, "chaos", "uc9", timeout_s=timeout, stable_s=10, poll_s=2,
                                              clock=FakeClock(), saturation=sat)

    def test_doing_nothing_is_NOT_healthy_when_saturated(self):
        c = FakeCluster(1); r = self._wait(c, FakeSaturation(c))
        self.assertFalse(r["healthy"]); self.assertEqual(r["last"]["max_cpu_utilisation"], 1.0)

    def test_two_replicas_still_saturated(self):
        c = FakeCluster(2); self.assertFalse(self._wait(c, FakeSaturation(c))["healthy"])

    def test_three_replicas_healthy(self):
        c = FakeCluster(3); r = self._wait(c, FakeSaturation(c))
        self.assertTrue(r["healthy"]); self.assertLess(r["max_cpu_utilisation"], 0.9)

    def test_without_prometheus_old_behaviour(self):
        c = FakeCluster(1); r = self._wait(c, None)
        self.assertTrue(r["healthy"]); self.assertNotIn("max_cpu_utilisation", r)

    def test_unknown_cpu_does_not_block_other_use_cases(self):
        class Unknown:
            def max_cpu_utilisation(self, ns, pods): return None
        c = FakeCluster(1); self.assertTrue(self._wait(c, Unknown())["healthy"])

    def test_end_to_end_resolve_with_playbook_params(self):
        c = FakeCluster(1)
        cfg = self.resolver.ResolverConfig(namespaces=frozenset({"chaos"}), stable_s=10, poll_s=2)   # no Prometheus
        raw = {"incident": "i", "action": "scale_deployment",
               "params": {"add_replicas": 2, "max_replicas": 5, "timeout_s": 30},
               "target": {"namespace": "chaos", "kind": "Deployment", "name": "uc9"}}
        v = self.resolver.resolve(raw, c, cfg, FakeClock())
        self.assertEqual(v["status"], "resolved"); self.assertTrue(v["applied"])
        self.assertEqual(v["detail"]["to_replicas"], 3)


class SaturationQueryTests(unittest.TestCase):
    def setUp(self):
        use_src("deter_resolver")
        self.sat = importlib.import_module("saturation")

    def _client(self, payload=None, boom=False):
        outer = self
        class R:
            def raise_for_status(s): pass
            def json(s): return payload
        class H:
            def get(s, url, params, timeout):
                outer.last = params["query"]
                if boom: raise RuntimeError("down")
                return R()
        return self.sat.PrometheusSaturation("http://prom:9090", http=H())

    def test_parses_value_and_builds_query(self):
        p = self._client({"data": {"result": [{"value": [0, "0.93"]}]}})
        self.assertAlmostEqual(p.max_cpu_utilisation("chaos", ["uc9-a-1", "uc9-a-2"]), 0.93)
        self.assertIn('pod=~"uc9-a-1|uc9-a-2"', self.last); self.assertIn("[30s]", self.last)

    def test_empty_result_is_unknown(self):
        self.assertIsNone(self._client({"data": {"result": []}}).max_cpu_utilisation("chaos", ["uc9-a-1"]))

    def test_failure_is_unknown_not_an_exception(self):
        self.assertIsNone(self._client(boom=True).max_cpu_utilisation("chaos", ["uc9-a-1"]))

    def test_unsafe_names_are_rejected(self):
        p = self._client({"data": {"result": [{"value": [0, "1"]}]}})
        self.assertIsNone(p.max_cpu_utilisation("chaos", ['x"}) or vector(1) #']))


class LlmToolTests(unittest.TestCase):
    def setUp(self):
        use_src("llm_resolver")
        self.tools = importlib.import_module("tools")
        self.cluster = FakeCluster(1)
        self.sat = FakeSaturation(self.cluster)
        self.tk = self.tools.Toolkit(self.cluster, "chaos", stable_s=10, poll_s=2, clock=FakeClock(),
                                     saturation=self.sat, max_replicas=5)
        self.by_name = {t.name: t for t in self.tk.tools}

    def call(self, name, **kw): return self.by_name[name].invoke(kw)

    def test_tool_list(self):
        self.assertEqual(len(self.tk.tools), 16)
        self.assertIn("scale_deployment", self.by_name); self.assertIn("get_cpu_usage", self.by_name)

    def test_get_cpu_usage(self):
        out = self.call("get_cpu_usage", deployment="uc9")
        self.assertIn('"max_pod_cpu_utilisation": 1.0', out); self.assertIn("250m", out)

    def test_scale_records_change(self):
        out = self.call("scale_deployment", deployment="uc9", replicas=3)
        self.assertEqual(self.cluster.replicas, 3); self.assertIn("1 to 3", out)
        self.assertEqual(self.tk.changes[0]["tool"], "scale_deployment")

    def test_scale_respects_ceiling_and_minimum(self):
        self.assertIn("ERROR", self.call("scale_deployment", deployment="uc9", replicas=6))
        self.assertIn("ERROR", self.call("scale_deployment", deployment="uc9", replicas=0))
        self.assertEqual(self.cluster.replicas, 1); self.assertEqual(self.tk.changes, [])

    def test_check_health_reports_saturation(self):
        self.assertIn('"healthy": false', self.call("check_health", deployment="uc9", wait_s=0))
        self.call("scale_deployment", deployment="uc9", replicas=3)
        self.assertIn('"healthy": true', self.call("check_health", deployment="uc9", wait_s=30))

    def test_without_prometheus_tool_says_so(self):
        tk = self.tools.Toolkit(FakeCluster(1), "chaos", stable_s=10, poll_s=2, clock=FakeClock())
        out = {t.name: t for t in tk.tools}["get_cpu_usage"].invoke({"deployment": "uc9"})
        self.assertIn("not available", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)
