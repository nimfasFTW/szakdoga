from functools import lru_cache
import json
import logging
import os
from pathlib import Path

from fastapi import BackgroundTasks, Body, FastAPI

from attempts import AttemptStore
from dispatch import ResolverClient, on_no_match, run_escalation, run_fast_loop
from enrich import PodInspector
from guard import IncidentGuard
from lookup import LookupTable
from pipeline import Config, handle_alert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dispatcher")
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S") )
logger.addHandler(ch)


CONFIG = Config(
    namespaces=frozenset(os.getenv("WATCHED_NAMESPACES", "chaos").split(",")),
    pod_alerts=frozenset(os.getenv(
        "POD_ALERTS",
        "ChaosPodCrashLooping,ChaosPodImagePullFailure,ChaosPodNotReady,ChaosPodCPUSaturated").split(",")),
)
# deterministic: table only, a miss gives up.  hybrid: table first, a miss goes to the LLM agent.
# llm: no table, every actionable alert goes to the LLM agent.
MODE = os.getenv("DISPATCH_MODE", "hybrid")
if MODE not in ("deterministic", "hybrid", "llm"):
    raise ValueError(f"DISPATCH_MODE must be deterministic|hybrid|llm, got {MODE!r}")

TABLE = (LookupTable([]) if MODE == "llm" else
         LookupTable.from_yaml(os.getenv("PLAYBOOKS_PATH", str(Path(__file__).with_name("playbooks.yaml")))))
ATTEMPTS = AttemptStore(ttl_s=float(os.getenv("ATTEMPT_TTL_S", "600")))
GUARD = IncidentGuard(lease_s=float(os.getenv("INCIDENT_LEASE_S", "300")))

DETER_TIMEOUT_S = float(os.getenv("RESOLVER_TIMEOUT_S", "180"))
LLM_TIMEOUT_S = float(os.getenv("LLM_RESOLVER_TIMEOUT_S", "300"))
LLM_MAX_ATTEMPTS = int(os.getenv("LLM_MAX_ATTEMPTS", "2"))   # LLM runs per incident before giving up

DETER_RESOLVER_SVC_NAME = os.getenv("DETER_RESOLVER_SVC_NAME", "deter-resolver")
LLM_RESOLVER_SVC_NAME = os.getenv("LLM_RESOLVER_SVC_NAME", "llm-resolver")
RESOLVER = ResolverClient(f"http://{DETER_RESOLVER_SVC_NAME}:8000", timeout_s=DETER_TIMEOUT_S)

if MODE == "deterministic":
    LLM_RESOLVER = LLM_ATTEMPTS = None
else:
    LLM_RESOLVER = ResolverClient(f"http://{LLM_RESOLVER_SVC_NAME}:8000", timeout_s=LLM_TIMEOUT_S,
                                  path=os.getenv("LLM_RESOLVER_PATH", "/resolve"))
    LLM_ATTEMPTS = AttemptStore(ttl_s=float(os.getenv("ATTEMPT_TTL_S", "600")))

_longest = max(DETER_TIMEOUT_S, LLM_TIMEOUT_S if LLM_RESOLVER else 0)
if GUARD._lease <= _longest:
    logger.warning("INCIDENT_LEASE_S (%s) should exceed the longest resolver timeout (%s), "
                   "or a lock can expire mid-incident", GUARD._lease, _longest)
logger.info("dispatch mode=%s", MODE)

@lru_cache(maxsize=1)
def get_inspector() -> PodInspector:   # lazy: importing this module must not need a cluster
    return PodInspector.from_cluster()


app = FastAPI()


@app.post("/alert")
def receive_alert(background: BackgroundTasks, payload: dict = Body(...)):
    """Decide, answer Alertmanager immediately, and let the resolver work in the background."""
    results = []
    for alert in payload.get("alerts", []):
        try:
            results.append(handle_alert(alert, CONFIG, get_inspector(), TABLE, ATTEMPTS, GUARD,
                                        llm_attempts=LLM_ATTEMPTS, llm_max_attempts=LLM_MAX_ATTEMPTS))
        except Exception as e:   # one bad alert must not fail the whole batch (Alertmanager would resend it)
            logger.exception("failed to handle alert")
            results.append({"outcome": "error", "why": repr(e)})
    for r in results:
        if r["outcome"] == "fast_loop":
            background.add_task(run_fast_loop, r, RESOLVER, ATTEMPTS, GUARD)
        elif r["outcome"] == "escalate":
            background.add_task(run_escalation, r, LLM_RESOLVER, ATTEMPTS, LLM_ATTEMPTS, GUARD)
        elif r["outcome"] == "no_match":
            on_no_match(r)
    return {"received": len(results), "results": results}


@app.post("/admin/reset")
def reset():
    """Experiment helper: forget all attempts and in-flight incidents between trials."""
    ATTEMPTS.reset_all()
    if LLM_ATTEMPTS is not None:
        LLM_ATTEMPTS.reset_all()
    GUARD.reset_all()
    return {"status": "reset"}


@app.get("/healthz")
async def health():
    return {"status": "ok"}
