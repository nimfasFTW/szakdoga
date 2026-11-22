import json
import logging
import os
from functools import lru_cache

from fastapi import Body, FastAPI

from clock import Clock
from cluster import KubeCluster
from resolver import ResolverConfig, resolve

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deter_resolver")
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S"))
logger.addHandler(ch)

CONFIG = ResolverConfig(
    namespaces=frozenset(os.getenv("WATCHED_NAMESPACES", "chaos").split(",")),
    stable_s=float(os.getenv("VERIFY_STABLE_S", "10")),
    poll_s=float(os.getenv("VERIFY_POLL_S", "2")),
    prometheus_url=os.getenv("PROMETHEUS_URL") or None,
    max_cpu_util=float(os.getenv("MAX_CPU_UTIL", "0.9")),
)
CLOCK = Clock()


@lru_cache(maxsize=1)
def get_cluster() -> KubeCluster:   # lazy: importing this module must not need a cluster
    return KubeCluster.from_cluster()


app = FastAPI()


@app.post("/resolve")
def resolve_endpoint(payload: dict = Body(...)):
    """Blocks until the action is executed AND verified (tens of seconds). The dispatcher calls this
    from a background task, never from the Alertmanager webhook thread, so that is fine."""
    logger.info("request %s", json.dumps(payload, sort_keys=True))
    result = resolve(payload, get_cluster(), CONFIG, CLOCK)
    logger.info("verdict %s", json.dumps(result, sort_keys=True))
    return result


@app.get("/healthz")
async def health():
    return {"status": "ok"}
