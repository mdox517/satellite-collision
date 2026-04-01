"""
ingester.py — Kafka producer that fetches TLEs from Celestrak and publishes them.

Runs on a configurable interval (default: every 30 minutes).
Each TLE is published as a JSON message to the 'tle-updates' Kafka topic.

Usage:
    pip install confluent-kafka requests
    docker-compose up -d  # start Kafka + Redis
    python src/ingester.py
"""

import json
import time
import logging
import os
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.environ.get('TLE_TOPIC', 'tle-updates')
FETCH_INTERVAL = int(os.environ.get('FETCH_INTERVAL_SEC', 1800))  # 30 min

CELESTRAK_URLS = {
    'active':   'https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=tle',
    'stations': 'https://celestrak.org/NORAD/elements/gp.php?GROUP=stations&FORMAT=tle',
    'debris':   'https://celestrak.org/NORAD/elements/gp.php?GROUP=cosmos-2251-debris&FORMAT=tle',
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [ingester] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# TLE Fetching
# ---------------------------------------------------------------------------
def fetch_tles(url, timeout=30):
    """Fetch TLE data from Celestrak. Returns list of (name, line1, line2)."""
    headers = {'User-Agent': 'satellite-collision-pipeline/0.1'}
    response = requests.get(url, timeout=timeout, headers=headers)
    response.raise_for_status()

    lines = response.text.strip().splitlines()
    satellites = []
    for i in range(0, len(lines) - 2, 3):
        name = lines[i].strip()
        line1 = lines[i + 1].strip()
        line2 = lines[i + 2].strip()
        if line1.startswith('1') and line2.startswith('2'):
            satellites.append((name, line1, line2))

    return satellites


def fetch_all_tles():
    """Fetch TLEs from all configured Celestrak endpoints."""
    all_tles = []
    seen_norad_ids = set()

    for group, url in CELESTRAK_URLS.items():
        try:
            tles = fetch_tles(url)
            # Deduplicate by NORAD catalog number (columns 3-7 of line 1)
            new = 0
            for name, l1, l2 in tles:
                norad_id = l1[2:7].strip()
                if norad_id not in seen_norad_ids:
                    seen_norad_ids.add(norad_id)
                    all_tles.append((name, l1, l2))
                    new += 1
            log.info(f'  {group}: {len(tles)} fetched, {new} new')
        except Exception as e:
            log.warning(f'  {group}: failed — {e}')

    return all_tles

# ---------------------------------------------------------------------------
# Kafka Producer
# ---------------------------------------------------------------------------
def create_producer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'tle-ingester',
        'linger.ms': 50,
        'batch.num.messages': 500,
    }
    return Producer(conf)


def delivery_callback(err, msg):
    if err:
        log.error(f'Delivery failed: {err}')


def publish_tles(producer, tles):
    """Publish each TLE as a JSON message to Kafka."""
    fetch_time = datetime.now(timezone.utc).isoformat()

    for name, line1, line2 in tles:
        norad_id = line1[2:7].strip()
        message = {
            'norad_id': norad_id,
            'name': name,
            'tle_line1': line1,
            'tle_line2': line2,
            'fetch_time': fetch_time,
        }
        producer.produce(
            TOPIC,
            key=norad_id,
            value=json.dumps(message),
            callback=delivery_callback,
        )

    producer.flush()

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run():
    log.info(f'Starting TLE ingester')
    log.info(f'  Kafka broker : {KAFKA_BROKER}')
    log.info(f'  Topic        : {TOPIC}')
    log.info(f'  Interval     : {FETCH_INTERVAL}s')

    producer = create_producer()

    while True:
        log.info('Fetching TLEs from Celestrak...')
        try:
            tles = fetch_all_tles()
            log.info(f'Total TLEs: {len(tles)}')

            publish_tles(producer, tles)
            log.info(f'Published {len(tles)} TLEs to Kafka topic "{TOPIC}"')
        except Exception as e:
            log.error(f'Fetch/publish cycle failed: {e}')

        log.info(f'Sleeping {FETCH_INTERVAL}s until next fetch...')
        time.sleep(FETCH_INTERVAL)


if __name__ == '__main__':
    run()
