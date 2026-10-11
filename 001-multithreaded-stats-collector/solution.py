import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests

logger = logging.getLogger(__name__)


MAX_WORKERS = 5

BASE_URL = "https://jsonplaceholder.typicode.com"
TIMEOUT = 10
DB_PATH = Path(__file__).parent / "stats.db"

_local = threading.local()

SCHEMA = """
CREATE TABLE IF NOT EXISTS user_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER UNIQUE NOT NULL,
    name TEXT NOT NULL,
    posts_count INTEGER NOT NULL,
    comments_count INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
"""

UPSERT_SQL = """
INSERT INTO user_stats (user_id, name, posts_count, comments_count, updated_at)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT(user_id) DO UPDATE SET
    name           = excluded.name,
    posts_count    = excluded.posts_count,
    comments_count = excluded.comments_count,
    updated_at     = excluded.updated_at;
"""


def init_db(db_path: Path = DB_PATH) -> None:
    """Создаёт таблицу user_stats, если её ещё нет."""

    conn = sqlite3.connect(db_path)
    try:
        conn.execute(SCHEMA)
        conn.commit()
    finally:
        conn.close()


def get_session() -> requests.Session:
    """Возвращает thread-local Session для текущего потока."""

    if not hasattr(_local, "session"):
        _local.session = requests.Session()
    return _local.session


def fetch_users() -> list[dict]:
    """Получает список всех пользователей."""

    session = get_session()
    url = f"{BASE_URL}/users"
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_posts(user_id: int) -> list[dict]:
    """Получает посты указанного пользователя."""

    session = get_session()
    url = f"{BASE_URL}/posts"
    response = session.get(url, params={"userId": user_id}, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def fetch_comments(post_id: int) -> list[dict]:
    """Получает комментарии указанного поста."""

    session = get_session()
    url = f"{BASE_URL}/comments"
    response = session.get(url, params={"postId": post_id}, timeout=TIMEOUT)
    response.raise_for_status()
    return response.json()


def collect_user(user: dict) -> dict | None:
    """Собирает статистику одного пользователя."""

    user_id = user["id"]
    try:
        posts = fetch_posts(user_id)
        comments_count = 0
        for post in posts:
            comments = fetch_comments(post["id"])
            comments_count += len(comments)

        return {
            "user_id": user_id,
            "name": user["name"],
            "posts_count": len(posts),
            "comments_count": comments_count,
        }
    except (requests.RequestException, KeyError, TypeError) as e:
        logger.warning("Failed to collect user %s: %s", user_id, e)
        return None


def collect_all_users(users: list[dict]) -> list[dict]:
    """Собирает статистику всех пользователей параллельно."""

    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(collect_user, user) for user in users]

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                results.append(result)

    return results


def save_stats(stats: list[dict], db_path: Path = DB_PATH) -> None:
    """Сохраняет статистику в БД. Обновляет существующие записи."""

    now = datetime.now(timezone.utc).isoformat()
    rows = [(s["user_id"], s["name"], s["posts_count"], s["comments_count"], now) for s in stats]

    conn = sqlite3.connect(db_path)
    try:
        conn.executemany(UPSERT_SQL, rows)
        conn.commit()
    finally:
        conn.close()


def print_top_users(limit: int = 3, db_path: Path = DB_PATH) -> None:
    """Выводит TOP-N пользователей по количеству комментариев."""

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            """
            SELECT name, posts_count, comments_count
            FROM user_stats
            ORDER BY comments_count DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()

    for name, posts_count, comments_count in rows:
        print(f"{name} | posts: {posts_count} | comments: {comments_count}")


if __name__ == "__main__":
    init_db()
    users = fetch_users()
    stats = collect_all_users(users)
    save_stats(stats)
    print_top_users()
