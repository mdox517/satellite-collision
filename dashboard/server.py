"""
dashboard/server.py — Lightweight web server for the CesiumJS dashboard.

Serves the frontend and provides REST API endpoints that read
satellite positions and conjunction alerts from Redis.

Usage:
    pip install flask redis sgp4 numpy
    python dashboard/server.py
"""

import json
import math
import os
import logging
from datetime import datetime, timezone, timedelta

import numpy as np
import redis
from flask import Flask, jsonify, send_from_directory, request
from sgp4.api import Satrec, jday

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_DB = int(os.environ.get('REDIS_DB', 0))
PORT = int(os.environ.get('DASHBOARD_PORT', 8080))

REDIS_KEY_PREFIX = 'sat:'
REDIS_INDEX_KEY = 'sat:index'
REDIS_ALERT_KEY = 'alerts:latest'
REDIS_ENCOUNTER_KEY = 'encounters:latest'

EARTH_RADIUS_KM = 6371.0

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [dashboard] %(levelname)s %(message)s',
    datefmt='%H:%M:%S',
)

app = Flask(__name__, static_folder='static')

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                       decode_responses=True)

# ---------------------------------------------------------------------------
# Coordinate Conversion
# ---------------------------------------------------------------------------
def eci_to_geodetic(x, y, z):
    """Approximate ECI to geodetic (lat, lon, alt)."""
    r = math.sqrt(x**2 + y**2 + z**2)
    alt = r - EARTH_RADIUS_KM
    lat = math.degrees(math.asin(z / r)) if r > 0 else 0
    lon = math.degrees(math.atan2(y, x))
    return lat, lon, alt

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------
@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/satellites')
def api_satellites():
    """Return all satellite positions."""
    r = get_redis()
    norad_ids = r.smembers(REDIS_INDEX_KEY)
    if not norad_ids:
        return jsonify({'satellites': [], 'count': 0})

    pipe = r.pipeline()
    norad_ids = sorted(norad_ids)
    for nid in norad_ids:
        pipe.hgetall(f'{REDIS_KEY_PREFIX}{nid}')
    results = pipe.execute()

    satellites = []
    for nid, data in zip(norad_ids, results):
        if not data or 'x' not in data:
            continue
        try:
            x = float(data['x'])
            y = float(data['y'])
            z = float(data['z'])
        except (KeyError, ValueError):
            continue

        lat, lon, alt = eci_to_geodetic(x, y, z)

        satellites.append({
            'norad_id': nid,
            'name': data.get('name', nid),
            'lat': round(lat, 4),
            'lon': round(lon, 4),
            'alt_km': round(alt, 1),
            'propagated_at': data.get('propagated_at', ''),
        })

    return jsonify({'satellites': satellites, 'count': len(satellites)})


@app.route('/api/orbit/<norad_id>')
def api_orbit(norad_id):
    """
    Propagate a satellite's TLE forward for one orbit and return
    a polyline of lat/lon/alt points for rendering.
    """
    r = get_redis()
    data = r.hgetall(f'{REDIS_KEY_PREFIX}{norad_id}')
    if not data or 'tle_line1' not in data:
        return jsonify({'error': 'Satellite not found'}), 404

    line1 = data['tle_line1']
    line2 = data['tle_line2']
    sat = Satrec.twoline2rv(line1, line2)

    # Estimate orbital period from mean motion (revs/day) in TLE line 2
    try:
        mean_motion = float(line2[52:63])  # revs per day
        period_min = 1440.0 / mean_motion  # minutes
    except (ValueError, IndexError):
        period_min = 92.0  # default ~LEO

    # Propagate for 1.2 orbits with 1-minute steps
    minutes = int(request.args.get('minutes', int(period_min * 1.2)))
    step = int(request.args.get('step', 1))
    now = datetime.now(timezone.utc)

    points = []
    for m in range(0, minutes, step):
        dt = now + timedelta(minutes=m)
        jd, fr = jday(dt.year, dt.month, dt.day,
                      dt.hour, dt.minute, dt.second + dt.microsecond / 1e6)
        err, pos, vel = sat.sgp4(jd, fr)
        if err != 0:
            continue

        lat, lon, alt = eci_to_geodetic(pos[0], pos[1], pos[2])
        points.append({
            'lat': round(lat, 4),
            'lon': round(lon, 4),
            'alt_km': round(alt, 1),
            'minutes_from_now': m,
        })

    return jsonify({
        'norad_id': norad_id,
        'name': data.get('name', norad_id),
        'period_min': round(period_min, 1),
        'points': points,
    })


@app.route('/api/alerts')
def api_alerts():
    """Return the latest conjunction alerts from Redis."""
    r = get_redis()
    raw = r.lrange(REDIS_ALERT_KEY, 0, 99)
    alerts = []
    for item in raw:
        try:
            alerts.append(json.loads(item))
        except json.JSONDecodeError:
            continue

    return jsonify({'alerts': alerts, 'count': len(alerts)})


@app.route('/api/encounters')
def api_encounters():
    """Return close encounters (pairs within 50 km) from Redis."""
    r = get_redis()
    raw = r.lrange(REDIS_ENCOUNTER_KEY, 0, 99)
    encounters = []
    for item in raw:
        try:
            encounters.append(json.loads(item))
        except json.JSONDecodeError:
            continue

    return jsonify({'encounters': encounters, 'count': len(encounters)})


@app.route('/api/stats')
def api_stats():
    """Return pipeline health stats."""
    r = get_redis()
    num_sats = r.scard(REDIS_INDEX_KEY)
    num_alerts = r.llen(REDIS_ALERT_KEY)
    num_encounters = r.llen(REDIS_ENCOUNTER_KEY)

    return jsonify({
        'satellites_tracked': num_sats,
        'active_alerts': num_alerts,
        'close_encounters': num_encounters,
        'timestamp': datetime.now(timezone.utc).isoformat(),
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    logging.info(f'Starting dashboard on http://localhost:{PORT}')
    app.run(host='0.0.0.0', port=PORT, debug=True)
