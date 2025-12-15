#!/usr/bin/env python3
#
# Copyright (c) 2025 A Platform. All rights reserved.
#
# [Disclaimer]
# This script is provided for educational and demonstration purposes as part of 
# the A Platform Support Bulletin. 
#
# You are free to use, modify, and distribute this code for your own environments.
# However, this code is provided "as is" without warranty of any kind.
# A Platform assumes no responsibility for any errors or damages resulting from its use.

import http.server
import socketserver
import threading
from dotenv import load_dotenv
import singlestoredb as s2
import multiprocessing as mp
import os
import time
import logging

# ######################################################################
# User-defined settings
# ######################################################################
MA_CHECK_YN="Y"             # Check MA node (Y/N, default: Y)
HEALTH_CHECK_PORT="3309"    # HTTP port for health check (default: 3309)
CHECK_INTERVAL=30           # Check interval (sec, default: 20)
QUERY_TIMEOUT=10            # Query timeout (sec, default: 10)
# ######################################################################

load_dotenv(dotenv_path="/PATH/TO/.env")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_pass = os.getenv("DB_PASS")

is_healthy = False
health_last_error = ""
is_ma = False
ma_last_error = ""

MA_CHECK_YN = globals().get("MA_CHECK_YN", "Y")
HEALTH_CHECK_PORT = globals().get("HEALTH_CHECK_PORT", 3309)
CHECK_INTERVAL = globals().get("CHECK_INTERVAL", 20)
QUERY_TIMEOUT = globals().get("QUERY_TIMEOUT", 10)


def execute_query_worker(query, shared_dict):
    """Execute query"""
    
    conn = None
    try:
        conn = s2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            results_type='tuple'
        )
        conn.autocommit(True)
        with conn.cursor() as cur:
            pid = os.getpid()
            
            cur.execute("select connection_id();")
            conn_id = str(cur.fetchone()[0])
            shared_dict["conn_id"] = conn_id
            # print(f"pid: {pid}, conn_id: {conn_id}, query: {query}")

            cur.execute(query)
            row = cur.fetchone()
            result = str(row[0]) if row else None
            shared_dict["result"] = result
    except Exception as e:
        shared_dict["error"] = str(e)
    finally:
        if conn:
            # print(f"connection close.(pid={pid})")
            conn.close()


def kill_connection(conn_id):
    """Open a new connection and kill target connection"""
    try:
        conn = s2.connect(
            host=db_host,
            port=db_port,
            user=db_user,
            password=db_pass,
            results_type='tuple'
        )
        conn.autocommit(True)
        with conn.cursor() as cur:
            # print(f"pid: {os.getpid()}, conn_id: kill {conn_id}")
            cur.execute(f"KILL {conn_id};")
            print(f"[INFO] Killed connection {conn_id}")
    except Exception as e:
        print(f"[WARN] Failed to kill connection {conn_id}: {e}")
    finally:
        conn.close()


def run_query(query: str, step_msg: str):
    """Execute query with timeout — forcibly close connection if exceeded"""
    timeout_sec = QUERY_TIMEOUT

    with mp.Manager() as manager:
        shared_dict = manager.dict()
        proc = mp.Process(target=execute_query_worker, args=(query, shared_dict))
        proc.start()
        proc.join(timeout=timeout_sec)

        if proc.is_alive():
            # Timeout
            conn_id = shared_dict.get("conn_id")
            print(f"[WARN] Query timeout after {timeout_sec}s — forcibly terminating worker connection.")
            if conn_id:
                kill_connection(conn_id)
            else:
                print("[WARN] conn_id not found — cannot kill worker connection.")
            proc.terminate()
            proc.join()
            raise RuntimeError(f"{step_msg} Timeout after {timeout_sec} seconds")

        if "error" in shared_dict:
            raise RuntimeError(f"{step_msg} Failed: {shared_dict['error']}")
        return shared_dict.get("result")


    # with ProcessPoolExecutor(max_workers=1) as executor:
    #     future = executor.submit(execute_query_worker, query)
    #     worker_process = next(iter(executor._processes.values())) 
    #     try:
    #         result, conn_id = future.result(timeout=timeout_sec)
    #         return result
    #     except TimeoutError:
    #         print(f"[WARN] Query timeout after {timeout_sec}s — forcibly terminating worker connection. (pid:{worker_process.pid}, conn_id: worker_process.conn_id)")


def ma_check():
    """ Master Aggregator Check """
    global is_ma, ma_last_error

    try: 
        ma_id = run_query("select node_id from information_schema.aggregators where MASTER_AGGREGATOR = 1/*mntr_select*/;", "Step 1: Master Aggregator id check")
        agg_id = run_query("select aggregator_id()/*mntr_select*/;", "Step 2: Aggregator id check")

        if int(ma_id) != int(agg_id):
            is_ma = False
            return

        is_ma = True

    except RuntimeError as e:
        ma_last_error = str(e)
        is_ma = False
    except Exception as e:
        ma_last_error = f"unknown error : {e}"
        is_ma = False


def health_check():
    """ Aggregator Health Check """
    global is_healthy, health_last_error

    try:
        # 1. Check the Aggregator connection
        conn_check = run_query("select 1 /*mntr_select*/;", "Step 1: Aggregator connection check")
        
        if conn_check != "1":
            health_last_error = "Step 1: Aggregator connection failed"
            is_healthy = False
            return
        
        # 2. Check the node role
        agg_id = run_query("select aggregator_id()/*mntr_select*/;", "Step 2: Aggregator role check")
        
        if agg_id == "-1":
            health_last_error = "Step 2: This node is not aggregator node"
            is_healthy = False
            return

        # 3. Check the status of master partitions
        qr_cnt_unavail_mst_parts = run_query (
                "select count(*) from information_schema.DISTRIBUTED_PARTITIONS where role = 'Master' and is_offline = 1/*mntr_select*/;",
                "Step 3: Check the count of unavailable master partitions."
            )
        if int(qr_cnt_unavail_mst_parts) > 0:
            health_last_error = "Step 3: Cannot use the several master partitions."
            is_healthy = False
            return

        # 4. Verify demoted clusters when consensus_enabled = on
        qr_consensus = run_query ("select @@consensus_enabled/*mntr_select*/;", "Select the consensus_enabled values.")
        
        if qr_consensus == "1":
            qr_connecting_cnt = run_query (
                    "select count(*) from information_schema.lmv_consensus_nodes where ROLE != 'Non-voting Member' and CONNECTION = 'Connecting'/*mntr_select*/;",
                    "Step 4-1: Counting the conncting nodes."
                )
            qr_connected_cnt = run_query (
                    "select count(*) from information_schema.lmv_consensus_nodes where ROLE != 'Non-voting Member' and CONNECTION = 'Connected'/*mntr_select*/;",
                    "Step 4-2: Counting the conncted nodes."
                )
        
            if int(qr_connecting_cnt) > int(qr_connected_cnt):
                health_last_error = "Step 4: This cluster was demoted."
                is_healthy = False
                return

        # All checks passed
        is_healthy = True
        health_last_error = ""

    except RuntimeError as e:
        health_last_error = str(e)
        is_healthy = False
    except Exception as e:
        health_last_error = f"unknown error : {e}"
        is_healthy = False


RED = "\033[31m"
RESET = "\033[0m"

class ErrorOnlyFormatter(logging.Formatter):
    def format(self, record):
        # 에러 레벨이면 [ERROR]만 빨간색
        if record.levelno == logging.ERROR:
            record.levelname = f"{RED}{record.levelname}{RESET}"
        return super().format(record)

handler = logging.StreamHandler()
formatter = ErrorOnlyFormatter("[%(asctime)s] [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S")
handler.setFormatter(formatter)
logging.basicConfig(level=logging.INFO, handlers=[handler])


def update_health():
    while True:
        health_check()
        # HealthCheck
        if is_healthy:
            logging.info("HealthCheck: Healthy")
        else:
            logging.error(f"HealthCheck: Unhealthy, LastError: {health_last_error}")

        # MA Check
        if is_healthy and MA_CHECK_YN.upper() == "Y":
            ma_check()
            ma_status = "is" if is_ma else "is not"
            if is_ma:
                logging.info(f"MA Check: This node {ma_status} MA")
            else:
                if ma_last_error:
                    logging.error(f"MA Check: This node {ma_status} MA, LastError: {ma_last_error}")
                else:
                    logging.info(f"MA Check: This node {ma_status} MA")
        
        time.sleep(CHECK_INTERVAL)
        

class HealthCheckHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        # DML Endpoint
        if self.path == "/agg_check":
            if is_healthy:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK - Aggregator Healthy\n")
            else:
                self.send_response(503)
                self.end_headers()
                self.wfile.write(f"Unavailable - {health_last_error}\n".encode())
            return

        # DDL Endpoint (MA_CHECK_YN == "Y")
        if self.path == "/ma_check" and MA_CHECK_YN.upper() == "Y":
            if is_ma and is_healthy:
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK - Master Aggregator\n")
            else:
                self.send_response(503)
                self.end_headers()
                self.wfile.write(f"Unavailable - {ma_last_error}\n".encode())
            return

        # Other URL
        self.send_response(404)
        self.end_headers()
        self.wfile.write(b"Not Found\n")

    def log_message(self, format, *args):
        return


class ReusableTCPServer(socketserver.TCPServer):
    allow_reuse_address = True

def run_server():
    with ReusableTCPServer(("", int(HEALTH_CHECK_PORT)), HealthCheckHandler) as httpd:
        logging.info(f"Aggregator Health Check Server running on port {HEALTH_CHECK_PORT}")
        httpd.serve_forever()

if __name__ == "__main__":
    # Aggregator Health check
    threading.Thread(target=update_health, daemon=True).start()

    threading.Thread(target=run_server, daemon=True).start()

    threading.Event().wait()
