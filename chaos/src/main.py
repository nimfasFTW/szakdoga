import requests
import os
import logging
import threading
import time
from flask import Flask, jsonify

UC1_ENV_CODE = "uc1"
UC1_ALLOCATED_MEM_SIZE = 105  # in MiB

UC2_ENV_CODE = "uc2"
UC2_ALLOCATED_MEM_SIZE = 200

UC3_ENV_CODE_SERVER = "uc3_server"
UC3_ENV_CODE_CLIENT = "uc3_client"

UC4_ENV_CODE = "uc4"

UC5_ENV_CODE = "uc5"
UC6_ENV_CODE = "uc6"
UC7_ENV_CODE = "uc7"

UC8_ENV_CODE_SERVER = "uc8_server"
UC8_ENV_CODE_CLIENT = "uc8_client"
UC8_HEARTBEAT_STALE_AFTER = 5  # seconds without a heartbeat -> db worker is considered stuck
UC8_ADMIN_PORT = 9000

uc8_stuck = threading.Event()          # set -> the db worker is stuck
uc8_last_heartbeat = time.time()

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# choose use case
def main():
    uc_code = os.environ["UC_CODE"]
    logger.info(f"Starting chaos applicatoin with uc_code {uc_code}")

    if uc_code == UC1_ENV_CODE:
        uc1()
    elif uc_code == UC2_ENV_CODE:
        uc2()
    elif uc_code == UC3_ENV_CODE_CLIENT:
        uc3_client()
    elif uc_code == UC3_ENV_CODE_SERVER:
        uc3_server()
    elif uc_code == UC4_ENV_CODE:
        uc4()
    elif uc_code == UC8_ENV_CODE_SERVER:
        uc8_server()
    elif uc_code == UC8_ENV_CODE_CLIENT:
        uc8_client()
    elif uc_code == UC7_ENV_CODE or uc_code == UC5_ENV_CODE or uc_code == UC6_ENV_CODE:
        port = int(os.getenv("APP_PORT", "8080"))
        app.run(host="0.0.0.0", port=port)
    else:
        logger.error(f"Unknown error code: {uc_code}")

@app.route("/ready")
def readiness():

    uc_code = os.environ["UC_CODE"]
    
    if uc_code == UC8_ENV_CODE_SERVER:
        stale_for = time.time() - uc8_last_heartbeat
        if stale_for > UC8_HEARTBEAT_STALE_AFTER:
            logger.error(f"Readiness check failed: db worker has not responded for {stale_for:.0f}s")
            return jsonify(status="not ready"), 503
        return jsonify(status="ready"), 200
 
    if uc_code == UC8_ENV_CODE_CLIENT:
        url = f"http://{os.environ['UC8_DB_HOST']}:{os.environ['UC8_DB_PORT']}/getRecords"
        try:
            response = requests.get(url, timeout=2)
            if response.status_code == 200:
                logger.info("Database connection successful")
                return jsonify(status="ready"), 200
            logger.error(f"Database returned HTTP {response.status_code}")
        except Exception as e:
            logger.error(f"Could not connect to database at {url}: {e}")
        return jsonify(status="not ready"), 503
    
    if uc_code == UC7_ENV_CODE:
        external_db_url = os.environ["UC7_EXTERNAL_DB_URL"]
        external_db_port = os.environ["UC7_EXTERNAL_DB_PORT"]
        logger.error(f"Cannot connect to database at {external_db_url}:{external_db_port}")
        return jsonify(status="not ready"), 503

    db_host = os.environ["UC3_DB_HOST"]
    db_port = os.environ["UC3_DB_PORT"]

    url = f"http://{db_host}:{db_port}/getRecords"

    logger.info(f"Checking database at {url}")

    try:
        response = requests.get(url, timeout=2)

        if response.status_code == 200:
            logger.info("Database connection successful")
            return jsonify(status="ready"), 200

        logger.error(
            f"Database returned HTTP {response.status_code}"
        )
        return jsonify(status="not ready"), 503

    except Exception as e:
        logger.error(
            f"Could not connect to database at {url}: {e}"
        )
        return jsonify(status="not ready"), 503

@app.route("/getRecords")
def handle_dummy_db():

    if os.environ.get("UC_CODE") == UC8_ENV_CODE_SERVER:
        # simulate a hung query: the request is accepted, but never answered in time
        waited = 0
        while uc8_stuck.is_set() and waited < 120:
            if waited == 0:
                logger.warning("Query received, but the db worker is blocked, request is queued")
            time.sleep(1)
            waited += 1
        if uc8_stuck.is_set():
            return jsonify(error="query timed out"), 504
        return jsonify(status="ok"), 200

    db_mode_code = os.environ['UC3_DB_MODE_CODE']

    # simulate internal server error, like a db process being stuck
    if db_mode_code == "error":
        logger.error("DB process not running") 
        return jsonify(error="simulated database failure"), 500

    if db_mode_code == "correct":
        logger.info("No error, responding with 200")
        return jsonify(status="ok"), 200



# allocate UC1_ALLOCATED_MEM_SIZE MiB of memory
def uc1():
    data = bytearray(UC1_ALLOCATED_MEM_SIZE * 1024 * 1024) 
    time.sleep(3600)

def uc2():
    data = bytearray(UC2_ALLOCATED_MEM_SIZE * 1024 * 1024) 
    time.sleep(3600)

def uc3_client():
    port = int(os.getenv("UC3_CLIENT_PORT", "8080"))

    logger.info("Starting UC3 client")
    logger.info(
        f"Configured DB: "
        f"{os.environ['UC3_DB_HOST']}:{os.environ['UC3_DB_PORT']}"
    )

    app.run(host="0.0.0.0", port=port)

def uc3_server():
    port = int(os.getenv("UC3_DB_PORT", "5000"))
    logger.info(f"Starting DB on port {port}")
    app.run(host="0.0.0.0", port=port)

def uc4_worker(size_mib):
    data = bytearray(size_mib * 1024 * 1024)
    time.sleep(3600)

def uc4():
    n_of_workers = int(os.environ["UC4_NUMBER_OF_WORKER_THREADS"])
    memory_limit_mib = int(os.environ["UC4_MEMORY_LIMIT"])
    # intentionally over-allocate relative to the limit to simulate a leak
    overcommit_factor = 1.3
    worker_size = int((memory_limit_mib * overcommit_factor) // n_of_workers)

    threads = []
    for i in range(n_of_workers):
        logger.info(f"Starting thread with size {worker_size} MiB")
        t = threading.Thread(target=uc4_worker, args=(worker_size,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

def uc8_db_worker():
    """Fake DB worker: heartbeats every second, unless the stuck switch is on."""
    global uc8_last_heartbeat
    stalled_since = None
    while True:
        if uc8_stuck.is_set():
            stalled_since = stalled_since or time.time()
            stalled_for = time.time() - stalled_since
            if int(stalled_for) % 10 == 0:
                logger.error(f"DB worker stalled for {stalled_for:.0f}s, queries are blocked")
        else:
            stalled_since = None
            uc8_last_heartbeat = time.time()
        time.sleep(1)
 
@app.route("/hang", methods=["POST"])
def uc8_hang():
    uc8_stuck.set()
    logger.warning("--- switch ---")
    return jsonify(stuck=True)
 
def uc8_server():
    port = int(os.getenv("UC8_DB_PORT", "5000"))
    logger.info(f"Starting UC8 DB on port {port}")
    threading.Thread(target=uc8_db_worker, daemon=True).start()
    app.run(host="0.0.0.0", port=port, threaded=True)
 
def uc8_client():
    port = int(os.getenv("UC8_CLIENT_PORT", "8080"))
    logger.info(f"Starting UC8 client, configured DB: {os.environ['UC8_DB_HOST']}:{os.environ['UC8_DB_PORT']}")
    app.run(host="0.0.0.0", port=port)

if __name__ == "__main__":
    main()