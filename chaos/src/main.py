import os
import logging
import threading
import time
from flask import Flask, jsonify

UC1_ENV_CODE = "uc1"
UC1_ALLOCATED_MEM_SIZE = 105  # in MiB

UC2_ENV_CODE = "uc2"
UC2_ALLOCATED_MEM_SIZE = 200

UC4_ENV_CODE = "uc4"

UC_DUMMY_DB = "dummy_db"

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
    elif uc_code == UC4_ENV_CODE:
        uc4()
    elif uc_code == UC_DUMMY_DB:
        app.run(port=int(os.getenv("UC_DUMMY_DB_PORT", "5000")))
    else:
        logger.error(f"Unknown error code: {uc_code}")


@app.route("/getRecords")
def handle_dummy_db():
    # logger.info("reve")

    # if DELAY:
    #     time.sleep(DELAY)
    db_mode_code = os.environ['DB_MODE_CODE']

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
    time.sleep(60)

def uc2():
    data = bytearray(UC2_ALLOCATED_MEM_SIZE * 1024 * 1024) 
    time.sleep(3600)

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


if __name__ == "__main__":
    main()