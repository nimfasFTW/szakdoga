import logging
import os
import requests
import json
from pathlib import Path

from fastapi import Body, FastAPI

from attempts import AttemptStore
from enrich import PodInspector
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
        "POD_ALERTS", "ChaosPodCrashLooping,ChaosPodImagePullFailure,ChaosPodNotReady").split(",")),
)
TABLE = LookupTable.from_yaml(os.getenv("PLAYBOOKS_PATH", str(Path(__file__).with_name("playbooks.yaml"))))
ATTEMPTS = AttemptStore()
DETER_RESOLVER_SVC_NAME = os.getenv("DETER_RESOLVER_SVC_NAME", "deter-resolver")

def get_inspector() -> PodInspector:   # lazy: importing this module must not need a cluster
    return PodInspector.from_cluster()


app = FastAPI()


@app.post("/alert")
def receive_alert(payload: dict = Body(...)):
    # plain `def`: FastAPI runs it in a worker thread, so the blocking k8s client calls are fine
    results = [handle_alert(a, CONFIG, get_inspector(), TABLE, ATTEMPTS)
               for a in payload.get("alerts", [])]
    requests.post(
        url=f"http://{DETER_RESOLVER_SVC_NAME}:8000/resolve",
        json={"results": results},
        )
    # my-svc.my-namespace.svc.cluster-domain.example
    return {"received": len(results), "results": results}


@app.get("/healthz")
async def health():
    return {"status": "ok"}
