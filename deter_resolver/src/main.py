import logging
from fastapi import Body, FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("deter_resolver")
logger.propagate = False
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%Y-%m-%d %H:%M:%S") )
logger.addHandler(ch)

app = FastAPI()

@app.post("/resolve")
def receive_alert(payload: dict = Body(...)):
    logger.info(f"Received request to resolve: {payload}")
    return {"received": len(payload), "results": payload}
    
@app.get("/healthz")
async def health():
    return {"status": "ok"}