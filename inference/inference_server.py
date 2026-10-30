
import csv
import stat
import os
import joblib
import numpy as np
import pandas as pd

from pathlib import Path
from datetime import datetime, timezone

# ==========================================
# 1. CONFIGURATION
# ==========================================

PROJECT_DIR = Path(__file__).resolve().parent.parent

MODEL_PATH = PROJECT_DIR / "models" / "network_anomaly_model.pkl"
LOG_PATH = PROJECT_DIR / "logs" / "anomaly_log.csv"

FIFO_PATH = "/tmp/nwp-anomaly.pipe"

NUMERIC_FEATURES = [
    "id.orig_p", "id.resp_p", "duration",
    "orig_bytes", "resp_bytes", "missed_bytes",
    "orig_pkts", "orig_ip_bytes", "resp_pkts",
    "resp_ip_bytes"
]

CATEGORICAL_FEATURES = [
    "proto", "service", "conn_state",
    "local_orig", "local_resp", "history"
]

ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

COLUMNS = [
    "ts", "uid", "id.orig_h", "id.orig_p",
    "id.resp_h", "id.resp_p", "proto",
    "service", "duration", "orig_bytes",
    "resp_bytes", "conn_state", "local_orig",
    "local_resp", "missed_bytes", "history",
    "orig_pkts", "orig_ip_bytes", "resp_pkts",
    "resp_ip_bytes", "tunnel_parents",
    "label", "detailed-label"
]

# ==========================================
# 2. LOAD MODEL
# ==========================================

print("[INFO] Loading Random Forest model...")

model = joblib.load(MODEL_PATH)

print("[READY] Random Forest model loaded.")

# ==========================================
# 3. PREPARE LOG
# ==========================================

LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

if not LOG_PATH.exists() or LOG_PATH.stat().st_size == 0:
    with open(LOG_PATH, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([
            "timestamp",
            "prediction",
            "actual_label",
            "source_ip",
            "destination_ip",
            "source_port",
            "destination_port",
            "protocol",
            "traffic_record"
        ])

# ==========================================
# 4. CREATE FIFO
# ==========================================

if os.path.exists(FIFO_PATH):
    if not stat.S_ISFIFO(os.stat(FIFO_PATH).st_mode):
        os.remove(FIFO_PATH)
        os.mkfifo(FIFO_PATH)
else:
    os.mkfifo(FIFO_PATH)

# ==========================================
# 5. PARSE RECORD
# ==========================================

def parse_record(line):

    values = next(csv.reader([line]))

    # Split the final combined label fields.
    if len(values) == 21:
        last_parts = values[-1].strip().split(maxsplit=2)

        if len(last_parts) != 3:
            raise ValueError("Cannot split combined label columns")

        values = values[:-1] + last_parts

    if len(values) != 23:
        raise ValueError(
            f"Expected 23 fields, received {len(values)}"
        )

    record = dict(zip(COLUMNS, values))

    features = {
        column: record[column]
        for column in ALL_FEATURES
    }

    X = pd.DataFrame([features])

    # Convert numeric columns safely.
    for column in NUMERIC_FEATURES:
        X[column] = pd.to_numeric(
            X[column].mask(X[column].eq("-"), np.nan),
            errors="coerce"
        )

    # Clean categorical columns without pandas downcasting warnings.
    for column in CATEGORICAL_FEATURES:
        X[column] = X[column].mask(X[column].eq("-"), np.nan)

    return X, record

# ==========================================
# 6. PREDICT AND LOG
# ==========================================

def process_record(line):

    X, record = parse_record(line)

    prediction = model.predict(X)[0]

    timestamp = datetime.now(timezone.utc).isoformat()

    source_ip = record["id.orig_h"]
    destination_ip = record["id.resp_h"]

    if prediction == "Malicious":

        print(
            f"[MALICIOUS] {source_ip} -> {destination_ip}",
            flush=True
        )

        with open(LOG_PATH, "a", newline="") as file:
            writer = csv.writer(file)

            writer.writerow([
                timestamp,
                prediction,
                record["label"],
                source_ip,
                destination_ip,
                record["id.orig_p"],
                record["id.resp_p"],
                record["proto"],
                line
            ])

    else:

        print(
            f"[NORMAL] {source_ip} -> {destination_ip}",
            flush=True
        )

    return prediction

# ==========================================
# 7. STREAMING SERVER
# ==========================================

def start_server():

    processed = 0
    benign_count = 0
    malicious_count = 0
    errors = 0

    print("\n====================================")
    print(" NETWORK ANOMALY DETECTION SERVER")
    print("====================================")
    print("[INFO] Waiting for NS-3 connection...")

    with open(FIFO_PATH, "r", encoding="utf-8") as fifo:

        print("[READY] NS-3 connected. Monitoring traffic.\n")

        for line in fifo:

            line = line.strip()

            if not line:
                continue

            try:
                prediction = process_record(line)

                processed += 1

                if prediction == "Malicious":
                    malicious_count += 1
                else:
                    benign_count += 1

            except Exception as error:
                errors += 1
                print(f"[ERROR] {error}", flush=True)

    print("\n====================================")
    print(" ANALYSIS COMPLETED")
    print("====================================")
    print("Records processed:", processed)
    print("Normal:", benign_count)
    print("Malicious:", malicious_count)
    print("Errors:", errors)
    print("Anomaly log:", LOG_PATH)


if __name__ == "__main__":
    try:
        start_server()
    except KeyboardInterrupt:
        print("\n[STOPPED] Inference server stopped by user.")
