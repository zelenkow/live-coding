import queue
import threading
from pathlib import Path

import pytest
import requests
import responses
from solution import (
    check_temperature_changes,
    fetch_weather,
    get_cities,
    get_conn,
    get_last_weather,
    get_previous_weather,
    init_db,
    save_weather,
    seed_cities,
    worker,
)


@pytest.fixture
def db(tmp_path: Path) -> Path:
    """Создаёт временную БД с готовой схемой."""
    db_path = tmp_path / "test.db"
    init_db(db_path)
    return db_path


def test_init_db_creates_tables(db: Path) -> None:
    """Проверяет, что init_db создаёт таблицы cities и weather."""
    conn = get_conn(db)
    try:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        names = {row[0] for row in tables}
    finally:
        conn.close()
    assert "cities" in names
    assert "weather" in names


def test_seed_cities_is_idempotent(db: Path) -> None:
    """Повторный seed не создаёт дубликатов городов."""
    seed_cities(db)
    seed_cities(db)
    cities = get_cities(db)
    assert len(cities) == 5


def test_get_cities_returns_dicts(db: Path) -> None:
    """get_cities возвращает список словарей с нужными полями."""
    seed_cities(db)
    cities = get_cities(db)
    assert len(cities) == 5
    assert cities[0]["name"] in {"Berlin", "London", "Batumi", "Moscow", "Tokyo"}
    assert "latitude" in cities[0]
    assert "longitude" in cities[0]


def test_save_weather_and_get_last(db: Path) -> None:
    """save_weather записывает измерение, get_last_weather его возвращает."""
    seed_cities(db)
    city = get_cities(db)[0]

    save_weather(city["id"], 15.2, 3.8, db)
    last = get_last_weather(city["id"], db)

    assert last is not None
    assert last["temperature"] == 15.2
    assert last["wind_speed"] == 3.8


def test_get_last_weather_returns_none_when_empty(db: Path) -> None:
    """get_last_weather возвращает None, если измерений нет."""
    seed_cities(db)
    city = get_cities(db)[0]
    assert get_last_weather(city["id"], db) is None


def test_get_previous_weather(db: Path) -> None:
    """get_previous_weather возвращает предпоследнее измерение."""
    seed_cities(db)
    city = get_cities(db)[0]

    save_weather(city["id"], 10.0, 1.0, db)
    save_weather(city["id"], 20.0, 2.0, db)

    last = get_last_weather(city["id"], db)
    prev = get_previous_weather(city["id"], db)

    assert last is not None and last["temperature"] == 20.0
    assert prev is not None and prev["temperature"] == 10.0


@responses.activate
def test_fetch_weather_parses_response() -> None:
    """fetch_weather парсит temperature_2m и wind_speed_10m из ответа."""
    responses.get(
        "https://api.open-meteo.com/v1/forecast",
        json={"current": {"temperature_2m": 15.2, "wind_speed_10m": 3.8}},
        status=200,
    )
    temp, wind = fetch_weather(52.52, 13.41)
    assert temp == 15.2
    assert wind == 3.8


@responses.activate
def test_fetch_weather_passes_params() -> None:
    """fetch_weather передаёт latitude, longitude и current в query-параметрах."""
    responses.get(
        "https://api.open-meteo.com/v1/forecast",
        json={"current": {"temperature_2m": 0, "wind_speed_10m": 0}},
        status=200,
        match=[
            responses.matchers.query_param_matcher(
                {
                    "latitude": "52.52",
                    "longitude": "13.41",
                    "current": "temperature_2m,wind_speed_10m",
                }
            )
        ],
    )
    fetch_weather(52.52, 13.41)


@responses.activate
def test_fetch_weather_raises_on_500() -> None:
    """fetch_weather бросает HTTPError при статусе 500."""
    responses.get(
        "https://api.open-meteo.com/v1/forecast",
        status=500,
    )
    with pytest.raises(requests.HTTPError):
        fetch_weather(52.52, 13.41)


@responses.activate
def test_worker_saves_weather_and_calls_task_done(db: Path) -> None:
    """Воркер сохраняет погоду в БД и завершается по sentinel."""
    seed_cities(db)
    city = get_cities(db)[0]

    responses.get(
        "https://api.open-meteo.com/v1/forecast",
        json={"current": {"temperature_2m": 15.2, "wind_speed_10m": 3.8}},
        status=200,
    )

    q: queue.Queue = queue.Queue()
    q.put(city)
    q.put(None)

    t = threading.Thread(target=worker, args=(q, db))
    t.start()
    t.join(timeout=2)

    assert not t.is_alive()
    last = get_last_weather(city["id"], db)
    assert last is not None
    assert last["temperature"] == 15.2


@responses.activate
def test_worker_survives_http_error(db: Path) -> None:
    """Воркер не умирает при ошибке HTTP и не пишет данные в БД."""
    seed_cities(db)
    city = get_cities(db)[0]

    responses.get(
        "https://api.open-meteo.com/v1/forecast",
        status=500,
    )

    q: queue.Queue = queue.Queue()
    q.put(city)
    q.put(None)

    t = threading.Thread(target=worker, args=(q, db))
    t.start()
    t.join(timeout=2)

    assert not t.is_alive()
    assert get_last_weather(city["id"], db) is None


def test_check_temperature_warns_on_large_change(db: Path, capsys) -> None:
    """check_temperature_changes выводит WARNING при разнице > 5°C."""
    seed_cities(db)
    city = get_cities(db)[0]

    save_weather(city["id"], 10.0, 1.0, db)
    save_weather(city["id"], 18.0, 2.0, db)

    check_temperature_changes(db)

    captured = capsys.readouterr()
    assert "WARNING" in captured.out
    assert city["name"] in captured.out


def test_check_temperature_silent_on_small_change(db: Path, capsys) -> None:
    """check_temperature_changes молчит при разнице ≤ 5°C."""
    seed_cities(db)
    city = get_cities(db)[0]

    save_weather(city["id"], 10.0, 1.0, db)
    save_weather(city["id"], 12.0, 2.0, db)

    check_temperature_changes(db)

    captured = capsys.readouterr()
    assert "WARNING" not in captured.out
