import logging
from fastapi import FastAPI, Request

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dispatcher")

app = FastAPI()

@app.post("/alert")
async def receive_alert(request: Request):
    payload = await request.json()
    for alert in payload.get("alerts", []):
        if alert.get("labels", {}).get("namespace") == "chaos":
            logger.info(
                "status=%s alertname=%s namespace=%s pod=%s",
                alert.get("status"),
                alert.get("labels", {}).get("alertname"),
                alert.get("labels", {}).get("namespace"),
                alert.get("labels", {}).get("pod"),
            )
            logger.info("full payload: %s", alert)
    return {"received": len(payload.get("alerts", []))}

@app.get("/healthz")
async def health():
    return {"status": "ok"}