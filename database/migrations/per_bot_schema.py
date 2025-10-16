import os
import sqlite3
from contextlib import closing


__all__ = [
    "migrate_config_to_per_bot",
    "migrate_channel_intervals_to_per_bot",
    "main",
]


def get_db_path() -> str:
    """Resolve database file path without importing app config."""
    return os.getenv("DB_PATH", "forwarder.db")


def table_exists(cursor: sqlite3.Cursor, table: str) -> bool:
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cursor.fetchone() is not None


def migrate_config_to_per_bot(conn: sqlite3.Connection) -> None:
    """Ensure config table stores values per bot via bot_id column."""
    with closing(conn.cursor()) as cursor:
        if not table_exists(cursor, "config"):
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS config (
                    bot_id TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT,
                    PRIMARY KEY (bot_id, key)
                )
                """
            )
            return

        cursor.execute("PRAGMA table_info('config')")
        columns = {row[1] for row in cursor.fetchall()}
        if "bot_id" in columns:
            return

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS config_v2 (
                bot_id TEXT NOT NULL,
                key TEXT NOT NULL,
                value TEXT,
                PRIMARY KEY (bot_id, key)
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO config_v2 (bot_id, key, value)
            SELECT 'main', key, value FROM config
            """
        )
        cursor.execute("DROP TABLE config")
        cursor.execute("ALTER TABLE config_v2 RENAME TO config")


def migrate_channel_intervals_to_per_bot(conn: sqlite3.Connection) -> None:
    """Ensure channel intervals are scoped per bot."""
    with closing(conn.cursor()) as cursor:
        if not table_exists(cursor, "channel_intervals"):
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS channel_intervals (
                    bot_id TEXT NOT NULL,
                    channel_id TEXT NOT NULL,
                    next_channel_id TEXT,
                    interval_seconds INTEGER DEFAULT 900,
                    PRIMARY KEY (bot_id, channel_id)
                )
                """
            )
            return

        cursor.execute("PRAGMA table_info('channel_intervals')")
        columns = {row[1] for row in cursor.fetchall()}
        if "bot_id" in columns:
            return

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS channel_intervals_v2 (
                bot_id TEXT NOT NULL,
                channel_id TEXT NOT NULL,
                next_channel_id TEXT,
                interval_seconds INTEGER DEFAULT 900,
                PRIMARY KEY (bot_id, channel_id)
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO channel_intervals_v2 (bot_id, channel_id, next_channel_id, interval_seconds)
            SELECT 'main', channel_id, next_channel_id, interval_seconds FROM channel_intervals
            """
        )
        cursor.execute("DROP TABLE channel_intervals")
        cursor.execute("ALTER TABLE channel_intervals_v2 RENAME TO channel_intervals")


def main() -> None:
    db_path = get_db_path()
    print(f"Applying per-bot schema migration to: {db_path}")

    with sqlite3.connect(db_path) as conn:
        migrate_config_to_per_bot(conn)
        migrate_channel_intervals_to_per_bot(conn)

    print("Per-bot schema migration completed successfully.")


if __name__ == "__main__":
    main()
