import sqlite3
import time
from unittest.mock import patch

import pytest
import requests
import responses
from solution import (
    SCHEMA,
    UPSERT_SQL,
    collect_all_users,
    collect_user,
    fetch_comments,
    fetch_posts,
)


@pytest.fixture
def in_memory_db():
    """Своя БД на каждый тест."""

    conn = sqlite3.connect(":memory:")
    conn.execute(SCHEMA)
    conn.commit()
    yield conn
    conn.close()


@responses.activate
def test_fetch_posts_passes_user_id_param():
    """fetch_posts должен передавать userId в query-параметрах."""

    responses.get(
        "https://jsonplaceholder.typicode.com/posts",
        json=[{"id": 1, "userId": 1}],
        status=200,
        match=[responses.matchers.query_param_matcher({"userId": "1"})],
    )

    result = fetch_posts(1)
    assert result == [{"id": 1, "userId": 1}]


@responses.activate
def test_fetch_comments_timeout_raises():
    """Таймаут должен пробрасываться как RequestException."""

    responses.get(
        "https://jsonplaceholder.typicode.com/comments",
        body=requests.exceptions.Timeout("Mocked timeout"),
    )

    with pytest.raises(requests.exceptions.Timeout):
        fetch_comments(1)


@responses.activate
def test_collect_user_aggregates_correctly():
    """1 user → 2 posts → по 3 комментария на каждый пост."""

    responses.get(
        "https://jsonplaceholder.typicode.com/posts",
        json=[{"id": 10}, {"id": 11}],
        status=200,
    )
    responses.get(
        "https://jsonplaceholder.typicode.com/comments",
        json=[1, 2, 3],
        status=200,
    )

    user = {"id": 1, "name": "Leanne Graham"}
    result = collect_user(user)

    assert result is not None
    assert result["posts_count"] == 2
    assert result["comments_count"] == 6
    assert result["name"] == "Leanne Graham"


@responses.activate
def test_collect_user_returns_none_on_http_error():
    """500 от API → воркер возвращает None, не падает."""

    responses.get(
        "https://jsonplaceholder.typicode.com/posts",
        status=500,
    )

    user = {"id": 1, "name": "Leanne Graham"}
    result = collect_user(user)

    assert result is None


def test_upsert_updates_instead_of_duplicating(in_memory_db):
    """Повторная вставка того же user_id обновляет строку, а не плодит."""

    now = "2026-01-01T00:00:00+00:00"

    in_memory_db.executemany(UPSERT_SQL, [(1, "Alice", 10, 50, now)])

    in_memory_db.executemany(UPSERT_SQL, [(1, "Alice Updated", 10, 99, now)])
    in_memory_db.commit()

    row = in_memory_db.execute(
        "SELECT COUNT(*), name, comments_count FROM user_stats WHERE user_id = 1"
    ).fetchone()

    assert row[0] == 1
    assert row[1] == "Alice Updated"
    assert row[2] == 99


def test_collect_all_users_respects_max_workers():
    """Одновременно должно работать не больше 5 воркеров."""

    users = [{"id": i, "name": f"User {i}"} for i in range(10)]

    active = 0
    max_active = 0

    def mock_collect(user):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        time.sleep(0.05)  # имитация I/O, чтобы увидеть пик
        active -= 1
        return {
            "user_id": user["id"],
            "name": user["name"],
            "posts_count": 1,
            "comments_count": 1,
        }

    with patch("solution.collect_user", side_effect=mock_collect):
        collect_all_users(users)

    assert max_active <= 5
