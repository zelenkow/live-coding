import logging
import queue
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

_local = threading.local()

DB_PATH = Path(__file__).parent / "weather.db"

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
CURRENT_FIELDS = "temperature_2m,wind_speed_10m"
TIMEOUT = 10

MAX_WORKERS = 3

CITIES = [
    {"name": "Berlin", "latitude": 52.52, "longitude": 13.41},
    {"name": "London", "latitude": 51.51, "longitude": -0.13},
    {"name": "Batumi", "latitude": 41.65, "longitude": 41.64},
    {"name": "Moscow", "latitude": 55.75, "longitude": 37.62},
    {"name": "Tokyo", "latitude": 35.68, "longitude": 139.69},
]

SCHEMA = """
CREATE TABLE IF NOT EXISTS cities (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    latitude REAL NOT NULL,
    longitude REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS weather (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    city_id INTEGER NOT NULL,
    temperature REAL NOT NULL,
    wind_speed REAL NOT NULL,
    created_at TEXT NOT NULL,
    FOREIGN KEY (city_id) REFERENCES cities(id)
);
"""


def get_session() -> requests.Session:
    """Возвращает thread-local Session для текущего потока."""
    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def get_conn(db_path: Path = DB_PATH) -> sqlite3.Connection:
    """Открывает соединение с SQLite и включает проверку foreign keys."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path = DB_PATH) -> None:
    """Создаёт таблицы cities и weather, если их ещё нет."""
    conn = get_conn(db_path)
    try:
        conn.executescript(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def seed_cities(db_path: Path = DB_PATH) -> None:
    """Наполняет таблицу cities городами из CITIES (идемпотентно)."""
    conn = get_conn(db_path)
    try:
        query = """
            INSERT INTO cities (name, latitude, longitude)
            VALUES (?, ?, ?)
            ON CONFLICT(name) DO NOTHING
        """
        rows = [(city["name"], city["latitude"], city["longitude"]) for city in CITIES]
        conn.executemany(query, rows)
        conn.commit()
    finally:
        conn.close()


def get_cities(db_path: Path = DB_PATH) -> list[dict]:
    """Возвращает список всех городов из БД."""
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    try:
        query = "SELECT id, name, latitude, longitude FROM cities"
        rows = conn.execute(query).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


def save_weather(
    city_id: int, temperature: float, wind_speed: float, db_path: Path = DB_PATH
) -> None:
    """Сохраняет измерение погоды для города в таблицу weather."""
    conn = get_conn(db_path)
    try:
        query = """
        INSERT INTO weather (city_id, temperature, wind_speed, created_at)
        VALUES (?, ?, ?, ?)
        """
        conn.execute(
            query,
            (
                city_id,
                temperature,
                wind_speed,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def fetch_weather(latitude: float, longitude: float) -> tuple[float, float]:
    """Возвращает (temperature, wind_speed) для координат через Open-Meteo."""
    session = get_session()
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "current": CURRENT_FIELDS,
    }
    response = session.get(OPEN_METEO_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    current = response.json()["current"]
    return current["temperature_2m"], current["wind_speed_10m"]


def get_last_weather(city_id: int, db_path: Path = DB_PATH) -> dict | None:
    """Возвращает последнее измерение погоды для города или None."""
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    try:
        query = """
            SELECT temperature, wind_speed, created_at
            FROM weather
            WHERE city_id = ?
            ORDER BY created_at DESC
            LIMIT 1
        """
        row = conn.execute(query, (city_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def get_previous_weather(city_id: int, db_path: Path = DB_PATH) -> dict | None:
    """Возвращает предпоследнее измерение погоды для города или None."""
    conn = get_conn(db_path)
    conn.row_factory = sqlite3.Row
    try:
        query = """
            SELECT temperature, wind_speed, created_at
            FROM weather
            WHERE city_id = ?
            ORDER BY created_at DESC
            LIMIT 1 OFFSET 1
        """
        row = conn.execute(query, (city_id,)).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def worker(q: queue.Queue, db_path: Path = DB_PATH) -> None:
    """Воркер: берёт город из очереди, получает погоду, сохраняет в БД."""
    while True:
        city = q.get()
        if city is None:
            q.task_done()
            break
        try:
            temperature, wind_speed = fetch_weather(city["latitude"], city["longitude"])
            save_weather(city["id"], temperature, wind_speed, db_path)
        except (requests.RequestException, KeyError, TypeError) as e:
            logger.warning("Failed for %s: %s", city["name"], e)
        finally:
            q.task_done()


def print_results(db_path: Path = DB_PATH) -> None:
    """Выводит последнее измерение погоды для каждого города."""
    cities = get_cities(db_path)
    for city in cities:
        weather = get_last_weather(city["id"], db_path)
        if weather is None:
            continue
        print(f"{city['name']}: {weather['temperature']:.1f}°C, {weather['wind_speed']:.1f} km/h")


def check_temperature_changes(db_path: Path = DB_PATH) -> None:
    """Предупреждает, если температура изменилась больше чем на 5°C."""
    cities = get_cities(db_path)
    for city in cities:
        last = get_last_weather(city["id"], db_path)
        prev = get_previous_weather(city["id"], db_path)
        if last is None or prev is None:
            continue
        diff = last["temperature"] - prev["temperature"]
        if abs(diff) > 5:
            print(f"WARNING: {city['name']} temperature changed by {diff:+.1f}°C")


def main() -> None:
    init_db()
    seed_cities()

    cities = get_cities()
    q: queue.Queue = queue.Queue()

    for city in cities:
        q.put(city)
    threads = [threading.Thread(target=worker, args=(q, DB_PATH)) for _ in range(MAX_WORKERS)]

    for t in threads:
        t.start()

    q.join()

    for _ in threads:
        q.put(None)

    for t in threads:
        t.join()

    print_results()
    check_temperature_changes()


if __name__ == "__main__":
    main()
