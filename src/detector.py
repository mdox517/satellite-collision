"""
detector.py — Conjunction detection service.

Reads satellite positions from Redis, runs k-d tree screening to find
close approaches, scores candidates with the ML model, and publishes
alerts to the 'conjunction-alerts' Kafka topic.

Usage:
    pip install confluent-kafka redis scipy numpy scikit-learn pandas
    docker-compose up -d
    python src/detector.py
"""

import json
import time
import logging
import os
import pickle
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import redis
from scipy.spatial import KDTree
from confluent_kafka import Producer

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER = os.environ.get('KAFKA_BROKER', 'localhost:9092')
ALERT_TOPIC = os.environ.get('ALERT_TOPIC', 'conjunction-alerts')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))

REDIS_KEY_PREFIX = 'sat:'
REDIS_INDEX_KEY = 'sat:index'
REDIS_ENCOUNTER_KEY = 'encounters:latest'

# K-D tree screening threshold (km)
SCREEN_THRESHOLD_KM = float(os.environ.get('SCREEN_THRESHOLD_KM', 500.0))

# Close encounters: pairs within this distance get stored for the dashboard
CLOSE_ENCOUNTER_KM = float(os.environ.get('CLOSE_ENCOUNTER_KM', 20.0))

# Only alert on events with risk above this (log10 Pc)
ALERT_RISK_THRESHOLD = float(os.environ.get('ALERT_RISK_THRESHOLD', -6.0))

# How often to run the detection cycle
SCAN_INTERVAL = int(os.environ.get('SCAN_INTERVAL_SEC', 60))

MODEL_PATH = os.environ.get(
    'MODEL_PATH',
    str(Path(__file__).parent.parent / 'data' / 'collision_model.pkl'),
)

# Known station complexes — objects physically docked together
ISS_COMPLEX = {
    'ISS (ZARYA)', 'POISK', 'ISS (NAUKA)',
    'PROGRESS-MS 32', 'PROGRESS-MS 33',
    'SOYUZ-MS 28', 'CREW DRAGON 12',
}
CSS_COMPLEX = {
    'CSS (TIANHE)', 'CSS (WENTIAN)', 'CSS (MENGTIAN)',
    'TIANZHOU-9', 'SHENZHOU-22',
}
KNOWN_COMPLEXES = [ISS_COMPLEX, CSS_COMPLEX]

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [detector] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Load ML Model
# ---------------------------------------------------------------------------
def load_model(path):
    """Load the trained collision risk model."""
    try:
        with open(path, 'rb') as f:
            bundle = pickle.load(f)
        log.info(f'Loaded model v{bundle.get("version", "?")} '
                 f'({len(bundle["feature_cols"])} features, '
                 f'R²={bundle.get("r2", "?"):.4f})')
        return bundle
    except FileNotFoundError:
        log.warning(f'Model not found at {path} — scoring disabled')
        return None

# ---------------------------------------------------------------------------
# Redis Snapshot
# ---------------------------------------------------------------------------
def create_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                       decode_responses=True)


def read_snapshot(r):
    """
    Read all satellite positions from Redis.
    Returns: names, norad_ids, positions (n,3), velocities (n,3), metadata list
    """
    norad_ids_set = r.smembers(REDIS_INDEX_KEY)
    if not norad_ids_set:
        return [], [], np.empty((0, 3)), np.empty((0, 3)), []

    pipe = r.pipeline()
    norad_ids_list = sorted(norad_ids_set)
    for nid in norad_ids_list:
        pipe.hgetall(f'{REDIS_KEY_PREFIX}{nid}')
    results = pipe.execute()

    names = []
    norad_ids = []
    positions = []
    velocities = []
    metadata = []

    for nid, data in zip(norad_ids_list, results):
        if not data or 'x' not in data:
            continue
        try:
            pos = [float(data['x']), float(data['y']), float(data['z'])]
            vel = [float(data['vx']), float(data['vy']), float(data['vz'])]
        except (KeyError, ValueError):
            continue

        names.append(data.get('name', nid))
        norad_ids.append(nid)
        positions.append(pos)
        velocities.append(vel)
        metadata.append(data)

    return names, norad_ids, np.array(positions), np.array(velocities), metadata

# ---------------------------------------------------------------------------
# K-D Tree Screening
# ---------------------------------------------------------------------------
def find_candidates(positions, threshold_km):
    """Use k-d tree to find all pairs within threshold_km."""
    if len(positions) < 2:
        return []

    tree = KDTree(positions)
    results = tree.query_ball_tree(tree, r=threshold_km)

    pairs = set()
    for i, neighbors in enumerate(results):
        for j in neighbors:
            if j > i:
                pairs.add((i, j))

    return list(pairs)


def is_same_complex(name_a, name_b):
    """Check if two objects are part of the same docked station complex."""
    for complex_set in KNOWN_COMPLEXES:
        if name_a in complex_set and name_b in complex_set:
            return True
    return False


def filter_false_positives(pairs, names, positions, velocities, colocation_km=1.0):
    """Remove docked objects, co-located pairs, and co-orbiting objects."""
    filtered = []
    for i, j in pairs:
        if is_same_complex(names[i], names[j]):
            continue
        dist = np.linalg.norm(positions[i] - positions[j])
        if dist < colocation_km:
            continue
        # Filter co-orbiting objects (same launch, near-zero relative speed)
        rel_speed = np.linalg.norm(velocities[i] - velocities[j])
        if rel_speed < 0.5:  # less than 500 m/s (0.5 km/s) relative
            continue
        filtered.append((i, j))
    return filtered

# ---------------------------------------------------------------------------
# Conjunction Analysis & Scoring
# ---------------------------------------------------------------------------
def analyze_conjunction(i, j, names, norad_ids, positions, velocities, metadata):
    """Compute conjunction features for a candidate pair."""
    pos_a, pos_b = positions[i], positions[j]
    vel_a, vel_b = velocities[i], velocities[j]

    rel_pos = pos_b - pos_a          # km
    rel_vel = vel_b - vel_a          # km/s

    miss_distance_km = np.linalg.norm(rel_pos)
    miss_distance_m = miss_distance_km * 1000.0
    relative_speed = np.linalg.norm(rel_vel) * 1000.0  # m/s

    # Approximate RTN (radial, along-track, cross-track) decomposition
    r_hat = pos_a / np.linalg.norm(pos_a)               # radial
    n_hat = np.cross(pos_a, vel_a)                        # cross-track
    n_norm = np.linalg.norm(n_hat)
    if n_norm > 0:
        n_hat = n_hat / n_norm
    t_hat = np.cross(n_hat, r_hat)                        # along-track

    rel_pos_m = rel_pos * 1000.0
    rel_position_r = float(np.dot(rel_pos_m, r_hat))
    rel_position_t = float(np.dot(rel_pos_m, t_hat))
    rel_position_n = float(np.dot(rel_pos_m, n_hat))

    # Altitude (average of both objects)
    alt_a = np.linalg.norm(pos_a) - 6371.0
    alt_b = np.linalg.norm(pos_b) - 6371.0

    return {
        'sat_a': names[i],
        'sat_b': names[j],
        'norad_a': norad_ids[i],
        'norad_b': norad_ids[j],
        'miss_distance': miss_distance_m,
        'miss_distance_km': miss_distance_km,
        'relative_speed': relative_speed,
        'relative_position_r': rel_position_r,
        'relative_position_t': rel_position_t,
        'relative_position_n': rel_position_n,
        'c_h_apo': alt_a,
        'c_h_per': alt_a,
        't_h_apo': alt_b,
        't_h_per': alt_b,
        'time_to_tca': 0.0,  # we're looking at current positions
    }


def score_event(event, model_bundle):
    """Score a conjunction event with the ML model."""
    if model_bundle is None:
        return None

    features = pd.DataFrame([event])
    feature_cols = model_bundle['feature_cols']

    # Fill missing features with 0
    for col in feature_cols:
        if col not in features.columns:
            features[col] = 0.0

    risk = model_bundle['model'].predict(features[feature_cols])[0]
    return float(risk)

# ---------------------------------------------------------------------------
# Kafka Alerts
# ---------------------------------------------------------------------------
def create_producer():
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'conjunction-detector',
    }
    return Producer(conf)


def publish_alert(producer, event):
    """Publish a conjunction alert to Kafka."""
    producer.produce(
        ALERT_TOPIC,
        key=f'{event["norad_a"]}-{event["norad_b"]}',
        value=json.dumps(event, default=str),
    )

# ---------------------------------------------------------------------------
# Detection Cycle
# ---------------------------------------------------------------------------
def run_detection_cycle(r, model_bundle, producer):
    """One full detection cycle: snapshot → screen → score → alert."""
    t0 = time.perf_counter()

    # Step 1: Read positions from Redis
    names, norad_ids, positions, velocities, metadata = read_snapshot(r)
    if len(names) < 2:
        log.info(f'Only {len(names)} satellites in Redis, skipping cycle')
        return

    # Step 2: K-D tree screening for close encounters
    # Use the close encounter threshold directly — no need to scan 500km
    # and then filter down, which creates 100k+ pairs to score
    candidates = find_candidates(positions, CLOSE_ENCOUNTER_KM)

    # Step 3: Filter false positives
    candidates = filter_false_positives(candidates, names, positions, velocities)

    # Step 4: Analyze and score each candidate
    alerts = []
    encounters = []
    now_str = datetime.now(timezone.utc).isoformat()
    for i, j in candidates:
        event = analyze_conjunction(i, j, names, norad_ids, positions, velocities, metadata)
        event['detected_at'] = now_str

        risk = score_event(event, model_bundle)
        if risk is not None:
            event['risk_log10'] = risk
            event['collision_probability'] = 10 ** risk

        encounters.append(event)

        # High-risk alert
        if risk is not None and risk >= ALERT_RISK_THRESHOLD:
            alerts.append(event)
            publish_alert(producer, event)

    producer.flush()

    # Store close encounters in Redis (replace previous set)
    pipe = r.pipeline()
    pipe.delete(REDIS_ENCOUNTER_KEY)
    for enc in sorted(encounters, key=lambda e: e['miss_distance_km'])[:100]:
        pipe.rpush(REDIS_ENCOUNTER_KEY, json.dumps(enc, default=str))
    pipe.execute()

    elapsed = time.perf_counter() - t0

    log.info(
        f'Cycle complete: {len(names)} sats, '
        f'{len(candidates)} candidates, '
        f'{len(encounters)} close encounters (<{CLOSE_ENCOUNTER_KM} km), '
        f'{len(alerts)} alerts, '
        f'{elapsed:.2f}s'
    )

    # Log top alerts
    if alerts:
        alerts.sort(key=lambda e: e.get('risk_log10', -999), reverse=True)
        for event in alerts[:5]:
            risk_str = f'10^{event["risk_log10"]:.1f}' if 'risk_log10' in event else '?'
            log.info(
                f'  ALERT: {event["sat_a"]} — {event["sat_b"]}  '
                f'dist={event["miss_distance_km"]:.1f} km  '
                f'Pc={risk_str}'
            )

# ---------------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------------
def run():
    log.info('Starting conjunction detector')
    log.info(f'  Kafka broker     : {KAFKA_BROKER}')
    log.info(f'  Alert topic      : {ALERT_TOPIC}')
    log.info(f'  Redis            : {REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}')
    log.info(f'  Screen threshold : {SCREEN_THRESHOLD_KM} km')
    log.info(f'  Alert threshold  : 10^{ALERT_RISK_THRESHOLD} Pc')
    log.info(f'  Scan interval    : {SCAN_INTERVAL}s')

    r = create_redis()
    model_bundle = load_model(MODEL_PATH)
    producer = create_producer()

    while True:
        try:
            run_detection_cycle(r, model_bundle, producer)
        except Exception as e:
            log.error(f'Detection cycle failed: {e}', exc_info=True)

        time.sleep(SCAN_INTERVAL)


if __name__ == '__main__':
    run()
