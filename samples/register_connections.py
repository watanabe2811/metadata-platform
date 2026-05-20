"""Bootstrap script — register sample connections in the collector.

Run after the collector is up:
    python samples/register_connections.py
"""
from __future__ import annotations

import json
import sys

import httpx

COLLECTOR_URL = "http://localhost:8080"
TOKEN = "dev-token-change-me"

CONNECTIONS = [
    {
        "logical_name": "t24-core-prod",
        "platform": "oracle",
        "host": "oracle-prod-01.bank.local",
        "port": 1521,
        "service_name": "T24P",
        "vault_path": "kv/bank/db/t24-core-prod",
        "classification": "confidential",
        "owner_team": "core-banking",
        "description": "T24 production core banking Oracle DB",
    },
    {
        "logical_name": "iceberg-warehouse",
        "platform": "iceberg",
        "host": "polaris.bank.local",
        "port": 8181,
        "service_name": "warehouse",
        "vault_path": "kv/bank/iceberg/warehouse",
        "classification": "internal",
        "owner_team": "data-platform",
        "description": "Main Iceberg lakehouse via Polaris catalog",
    },
    {
        "logical_name": "kafka-cdc-prod",
        "platform": "kafka",
        "host": "kafka.bank.local",
        "port": 9092,
        "service_name": "cdc-cluster",
        "vault_path": "kv/bank/kafka/cdc",
        "classification": "internal",
        "owner_team": "streaming",
        "description": "Kafka cluster for GoldenGate CDC topics",
    },
    {
        "logical_name": "doris-serving",
        "platform": "doris",
        "host": "doris-fe.bank.local",
        "port": 9030,
        "service_name": "serving",
        "vault_path": "kv/bank/doris/serving",
        "classification": "internal",
        "owner_team": "data-platform",
        "description": "Apache Doris serving cluster",
    },
]


def main() -> int:
    client = httpx.Client(
        base_url=COLLECTOR_URL,
        headers={"Authorization": f"Bearer {TOKEN}"},
        timeout=10.0,
    )
    for c in CONNECTIONS:
        resp = client.post("/api/v1/connections", json=c)
        if resp.status_code in (200, 201):
            print(f"✓ Created {c['logical_name']}")
        elif resp.status_code == 409:
            print(f"~ Exists  {c['logical_name']} — updating")
            update_payload = {k: v for k, v in c.items() if k != "logical_name"}
            resp = client.put(f"/api/v1/connections/{c['logical_name']}",
                              json=update_payload)
            if resp.status_code != 200:
                print(f"  ✗ Update failed: {resp.status_code} {resp.text}")
                return 1
        else:
            print(f"✗ Failed  {c['logical_name']}: {resp.status_code} {resp.text}")
            return 1
    print()
    print("All connections registered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
