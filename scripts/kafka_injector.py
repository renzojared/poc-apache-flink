#!/usr/bin/env python3
"""
Kafka Event & Dynamic Configuration Injector for Apache Flink PoC.

Supports:
1. HTTP REST Endpoint server for real-time injections via curl / Postman.
2. CLI commands for automated testing of out-of-order stream joins & rule updates.
3. Real-time consumer for Side Output topic (movements.alerts).
"""

import argparse
import json
import logging
import os
import sys
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict

try:
    from kafka import KafkaConsumer, KafkaProducer
except ImportError:
    print("Error: kafka-python-ng not found. Please activate .venv or run: pip install kafka-python-ng")
    sys.exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
    datefmt="%H:%M:%S"
)
LOG = logging.getLogger("KafkaInjector")

# Default Environment Variables
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_MOVEMENTS = os.getenv("KAFKA_TOPIC_MOVEMENTS", "movements.incoming")
TOPIC_CURRENCIES = os.getenv("KAFKA_TOPIC_CURRENCIES", "movements.currencies")
TOPIC_RULES = os.getenv("KAFKA_TOPIC_RULES", "config.rules")
TOPIC_ALERTS = os.getenv("KAFKA_TOPIC_ALERTS", "movements.alerts")
PORT = int(os.getenv("PORT", "8080"))


class KafkaEventPublisher:
    def __init__(self, bootstrap_servers: str = KAFKA_BOOTSTRAP):
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            retries=3,
        )
        LOG.info(f"Connected KafkaProducer to {self.bootstrap_servers}")

    def send(self, topic: str, key: str, payload: Dict[str, Any]) -> None:
        future = self.producer.send(topic, key=key, value=payload)
        record_metadata = future.get(timeout=10)
        LOG.info(
            f"Successfully produced to '{topic}' [partition {record_metadata.partition}, offset {record_metadata.offset}] key='{key}'"
        )
        self.producer.flush()

    def send_movement(self, payload: Dict[str, Any]) -> None:
        key = str(payload.get("movement") or payload.get("movementId") or "UNKNOWN")
        if "op" not in payload:
            payload["op"] = "u"
        if "timestamp" not in payload:
            payload["timestamp"] = int(time.time() * 1000)
        self.send(TOPIC_MOVEMENTS, key, payload)

    def send_currency(self, payload: Dict[str, Any]) -> None:
        key = str(payload.get("movement") or payload.get("movementId") or "UNKNOWN")
        if "timestamp" not in payload:
            payload["timestamp"] = int(time.time() * 1000)
        self.send(TOPIC_CURRENCIES, key, payload)

    def send_rule(self, payload: Dict[str, Any]) -> None:
        key = str(payload.get("ruleId") or "DEFAULT_RULE")
        self.send(TOPIC_RULES, key, payload)


# Singleton publisher for HTTP Handler
PUBLISHER: KafkaEventPublisher = None


class InjectorHttpHandler(BaseHTTPRequestHandler):
    def _set_headers(self, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(200)

    def do_GET(self):
        if self.path in ["/health", "/", ""]:
            self._set_headers(200)
            resp = {
                "status": "UP",
                "kafka": PUBLISHER.bootstrap_servers,
                "endpoints": {
                    "POST /config": "Broadcast dynamic business rules to config.rules",
                    "POST /movement": "Publish movement transaction to movements.incoming",
                    "POST /currency": "Publish currency event to movements.currencies"
                }
            }
            self.wfile.write(json.dumps(resp, indent=2).encode("utf-8"))
        else:
            self._set_headers(404)
            self.wfile.write(json.dumps({"error": "Not Found"}).encode("utf-8"))

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        if content_length == 0:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": "Empty body"}).encode("utf-8"))
            return

        body = self.rfile.read(content_length).decode("utf-8")
        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            self._set_headers(400)
            self.wfile.write(json.dumps({"error": f"Invalid JSON: {str(e)}"}).encode("utf-8"))
            return

        try:
            if self.path in ["/config", "/rule", "/rules"]:
                PUBLISHER.send_rule(data)
                self._set_headers(200)
                self.wfile.write(json.dumps({
                    "status": "RULE_BROADCAST_SENT",
                    "topic": TOPIC_RULES,
                    "rule": data
                }).encode("utf-8"))

            elif self.path in ["/movement", "/movements"]:
                PUBLISHER.send_movement(data)
                self._set_headers(200)
                self.wfile.write(json.dumps({
                    "status": "MOVEMENT_SENT",
                    "topic": TOPIC_MOVEMENTS,
                    "movement": data
                }).encode("utf-8"))

            elif self.path in ["/currency", "/currencies"]:
                PUBLISHER.send_currency(data)
                self._set_headers(200)
                self.wfile.write(json.dumps({
                    "status": "CURRENCY_SENT",
                    "topic": TOPIC_CURRENCIES,
                    "currency": data
                }).encode("utf-8"))

            else:
                self._set_headers(404)
                self.wfile.write(json.dumps({"error": f"Unknown endpoint '{self.path}'"}).encode("utf-8"))

        except Exception as e:
            LOG.exception("Error processing POST request")
            self._set_headers(500)
            self.wfile.write(json.dumps({"error": str(e)}).encode("utf-8"))


def start_server(port: int = PORT):
    global PUBLISHER
    PUBLISHER = KafkaEventPublisher()
    server_address = ("", port)
    httpd = HTTPServer(server_address, InjectorHttpHandler)
    LOG.info(f"⚡ Kafka Injector HTTP Server running at http://localhost:{port}")
    LOG.info(f"   Available endpoints:")
    LOG.info(f"     POST http://localhost:{port}/config")
    LOG.info(f"     POST http://localhost:{port}/movement")
    LOG.info(f"     POST http://localhost:{port}/currency")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        LOG.info("Stopping HTTP server...")
        httpd.server_close()


def run_scenario_currency_first():
    """Scenario 1: Currency arrives before Movement."""
    publisher = KafkaEventPublisher()
    movement_id = f"test-curr-first-{int(time.time())}"
    LOG.info(f"--- [Scenario 1: Currency arrives first for id={movement_id}] ---")

    # Step 1: Send currency
    curr_payload = {"movement": movement_id, "currency": "PEN"}
    LOG.info(f"Step 1: Sending currency event to '{TOPIC_CURRENCIES}'...")
    publisher.send_currency(curr_payload)

    LOG.info("Waiting 2 seconds...")
    time.sleep(2)

    # Step 2: Send movement
    mov_payload = {
        "movement": movement_id,
        "operation": "3747531388",
        "user": "586305",
        "account": "CASH",
        "amount": "-700000",
        "op": "u"
    }
    LOG.info(f"Step 2: Sending movement event to '{TOPIC_MOVEMENTS}'...")
    publisher.send_movement(mov_payload)
    LOG.info(f"✅ Finished Scenario 1. Flink should immediately join and emit enriched movement.")


def run_scenario_movement_first():
    """Scenario 2: Movement arrives before Currency (buffered in state, timer scheduled)."""
    publisher = KafkaEventPublisher()
    movement_id = f"test-mov-first-{int(time.time())}"
    LOG.info(f"--- [Scenario 2: Movement arrives first for id={movement_id}] ---")

    # Step 1: Send movement
    mov_payload = {
        "movement": movement_id,
        "operation": "3747531388",
        "user": "586305",
        "account": "CASH",
        "amount": "500000",
        "op": "u"
    }
    LOG.info(f"Step 1: Sending movement event to '{TOPIC_MOVEMENTS}' (buffered by Flink)...")
    publisher.send_movement(mov_payload)

    LOG.info("Waiting 3 seconds before sending currency...")
    time.sleep(3)

    # Step 2: Send currency before timeout
    curr_payload = {"movement": movement_id, "currency": "USD"}
    LOG.info(f"Step 2: Sending currency event to '{TOPIC_CURRENCIES}' (triggers drain and timer cancellation)...")
    publisher.send_currency(curr_payload)
    LOG.info(f"✅ Finished Scenario 2. Flink should drain buffered movement and cancel timer.")


def run_scenario_timeout():
    """Scenario 3: Movement arrives, currency NEVER arrives -> Side Output alert fired on timer."""
    publisher = KafkaEventPublisher()
    movement_id = f"test-timeout-{int(time.time())}"
    LOG.info(f"--- [Scenario 3: Timeout side output test for id={movement_id}] ---")

    mov_payload = {
        "movement": movement_id,
        "operation": "9999999999",
        "user": "user-orphan",
        "account": "CREDIT",
        "amount": "250000",
        "op": "u"
    }
    LOG.info(f"Sending movement event with NO corresponding currency to '{TOPIC_MOVEMENTS}'...")
    publisher.send_movement(mov_payload)
    LOG.info(f"✅ Movement sent. Once the timer expires, Flink will divert to side output '{TOPIC_ALERTS}'.")


def run_scenario_dynamic_rule():
    """Scenario 4: Inject dynamic rule to demonstrate Broadcast State update."""
    publisher = KafkaEventPublisher()
    rule_payload = {
        "ruleId": "RULE_STRICT_AML",
        "allowedCurrencies": ["PEN", "USD"],
        "minAmount": 100,
        "maxAmount": 1000000,
        "active": True,
        "description": "Strict AML limit: Only PEN and USD, max 1,000,000"
    }
    LOG.info(f"--- [Scenario 4: Broadcasting Dynamic Rule via '{TOPIC_RULES}'] ---")
    publisher.send_rule(rule_payload)
    LOG.info(f"✅ Dynamic rule broadcasted. TaskManagers will evaluate in-memory without Redis.")


def consume_alerts():
    """Consume and display events from Side Output alerts topic."""
    LOG.info(f"Starting consumer on '{TOPIC_ALERTS}' at {KAFKA_BOOTSTRAP}...")
    consumer = KafkaConsumer(
        TOPIC_ALERTS,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="python-alerts-watcher",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    LOG.info(f"Listening for Side Output alerts on '{TOPIC_ALERTS}'. Press Ctrl+C to stop.")
    for msg in consumer:
        LOG.warning(f"🚨 SIDE OUTPUT ALERT RECEIVED on [offset {msg.offset}]:")
        print(json.dumps(msg.value, indent=2))


def main():
    parser = argparse.ArgumentParser(description="Kafka Injector for Apache Flink PoC")
    parser.add_argument("--server", action="store_true", help="Start HTTP REST server")
    parser.add_argument("--port", type=int, default=PORT, help=f"HTTP Server port (default: {PORT})")
    parser.add_argument("--test-currency-first", action="store_true", help="Run Scenario 1 (Currency first)")
    parser.add_argument("--test-movement-first", action="store_true", help="Run Scenario 2 (Movement first)")
    parser.add_argument("--test-timeout", action="store_true", help="Run Scenario 3 (Timeout -> Side Output)")
    parser.add_argument("--test-rule", action="store_true", help="Run Scenario 4 (Dynamic Rule Broadcast)")
    parser.add_argument("--consume-alerts", action="store_true", help="Consume alerts from movements.alerts")

    args = parser.parse_args()

    if args.server:
        start_server(args.port)
    elif args.test_currency_first:
        run_scenario_currency_first()
    elif args.test_movement_first:
        run_scenario_movement_first()
    elif args.test_timeout:
        run_scenario_timeout()
    elif args.test_rule:
        run_scenario_dynamic_rule()
    elif args.consume_alerts:
        consume_alerts()
    else:
        # Default behavior if no flags: start HTTP server
        start_server(args.port)


if __name__ == "__main__":
    main()
