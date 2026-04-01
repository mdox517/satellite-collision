"""
propagator.py — Kafka consumer that propagates orbits and writes positions to Redis.

Consumes TLE messages from the 'tle-updates' topic, propagates each satellite
to the current time using SGP4, and stores the resulting ECI position + velocity
in Redis. The detector service reads these snapshots to run conjunction search.

Usage:
    pip install confluent-kafka redis sgp4 numpy
    docker-compose up -d
    python src/propagator.py
"""

import json
import time
import logging
import os
from datetime import datetime, timezone

import numpy as np
import redis
from confluent_kafka import Consumer, KafkaError
from sgp4.api import Satrec, jday

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
TOPIC = os.environ.get('TLE_TOPIC', 'tle-updates')
GROUP_ID = os.environ.get('CONSUMER_GROUP', 'propagator-group')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

# How often to re-propagate all stored TLEs to current time
REPROPAGATE_INTERVAL = int(os.environ.get('REPROPAGATE_INTERVAL_SEC', 60))

REDIS_KEY_PREFIX = 'sat:'
REDIS_INDEX_KEY = 'sat:index'

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [propagator] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SGP4 Propagation
# ---------------------------------------------------------------------------
def propagate(tle_line1, tle_line2, dt=None):
    """
    Propagate a TLE to datetime dt (default: now).
    Returns (position_km, velocity_kms) or (None, None) on error.
    """
    if dt is None:
        dt = datetime.now(timezone.utc)

    sat = Satrec.twoline2rv(tle_line1, tle_line2)
    jd, fr = jday(dt.year, dt.month, dt.day,
                  dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
    err, pos, vel = sat.sgp4(jd, fr)

    if err != 0:
        return None, None

    return list(pos), list(vel)

# ---------------------------------------------------------------------------
# Redis
# ---------------------------------------------------------------------------
def create_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                       decode_responses=True)


def store_position(r, norad_id, name, position, velocity, tle_line1, tle_line2, fetch_time):
    """Store a satellite's current state in Redis."""
    key = f'{REDIS_KEY_PREFIX}{norad_id}'
    now = datetime.now(timezone.utc).isoformat()

    data = {
        'norad_id': norad_id,
        'name': name,
        'x': position[0],
        'y': position[1],
        'z': position[2],
        'vx': velocity[0],
        'vy': velocity[1],
        'vz': velocity[2],
        'tle_line1': tle_line1,
        'tle_line2': tle_line2,
        'fetch_time': fetch_time,
        'propagated_at': now,
    }

    pipe = r.pipeline()
    pipe.hset(key, mapping=data)
    pipe.sadd(REDIS_INDEX_KEY, norad_id)
    pipe.execute()

# ---------------------------------------------------------------------------
# Kafka Consumer
# ---------------------------------------------------------------------------
def create_consumer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    return consumer


def process_message(r, msg):
    """Parse a TLE message, propagate, and store in Redis."""
    try:
        data = json.loads(msg.value())
    except json.JSONDecodeError:
        log.warning('Invalid JSON message, skipping')
        return False

    norad_id = data['norad_id']
    name = data['name']
    line1 = data['tle_line1']
    line2 = data['tle_line2']
    fetch_time = data.get('fetch_time', '')

    pos, vel = propagate(line1, line2)
    if pos is None:
        log.debug(f'Propagation failed for {name} ({norad_id}), skipping')
        return False

    store_position(r, norad_id, name, pos, vel, line1, line2, fetch_time)
    return True

# ---------------------------------------------------------------------------
# Re-propagation
# ---------------------------------------------------------------------------
def repropagate_all(r):
    """
    Re-propagate all stored satellites to the current time.
    Positions drift as time passes — this keeps the Redis snapshot fresh.
    """
    norad_ids = r.smembers(REDIS_INDEX_KEY)
    if not norad_ids:
        return 0

    updated = 0
    for norad_id in norad_ids:
        key = f'{REDIS_KEY_PREFIX}{norad_id}'
        sat_data = r.hgetall(key)
        if not sat_data or 'tle_line1' not in sat_data:
            continue

        pos, vel = propagate(sat_data['tle_line1'], sat_data['tle_line2'])
        if pos is None:
            continue

        now = datetime.now(timezone.utc).isoformat()
        r.hset(key, mapping={
            'x': pos[0], 'y': pos[1], 'z': pos[2],
            'vx': vel[0], 'vy': vel[1], 'vz': vel[2],
            'propagated_at': now,
        })
        updated += 1

    return updated

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run():
    log.info('Starting orbit propagator')
    log.info(f'  Kafka broker       : {KAFKA_BROKER}')
    log.info(f'  Topic              : {TOPIC}')
    log.info(f'  Redis              : {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
    log.info(f'  Repropagate every  : {REPROPAGATE_INTERVAL}s')

    r = create_redis()
    consumer = create_consumer()

    last_repropagate = time.time()
    ingested = 0
    failed = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        log.error(f'Kafka error: {msg.error()}')
                else:
                    if process_message(r, msg):
                        ingested += 1
                    else:
                        failed += 1

                    if ingested % 1000 == 0 and ingested > 0:
                        total = r.scard(REDIS_INDEX_KEY)
                        log.info(f'Ingested {ingested} TLEs ({failed} failed), {total} satellites in Redis')

            # Periodically re-propagate all positions to current time
            now = time.time()
            if now - last_repropagate >= REPROPAGATE_INTERVAL:
                updated = repropagate_all(r)
                log.info(f'Re-propagated {updated} satellites to current time')
                last_repropagate = now

    except KeyboardInterrupt:
        log.info('Shutting down...')
    finally:
        consumer.close()


if __name__ == '__main__':
    run()
