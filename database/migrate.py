import os
import sqlite3
from contextlib import closing

try:
    from migrations.per_bot_schema import (
        migrate_channel_intervals_to_per_bot,
        migrate_config_to_per_bot,
    )
except ImportError:  # Fallback when executed as module
    from database.migrations.per_bot_schema import (  # type: ignore
        migrate_channel_intervals_to_per_bot,
        migrate_config_to_per_bot,
    )


def get_db_path() -> str:
    """Resolve database file path without importing app config."""
    return os.getenv("DB_PATH", "forwarder.db")


def column_exists(cursor: sqlite3.Cursor, table: str, column: str) -> bool:
    cursor.execute(f"PRAGMA table_info('{table}')")
    return any(row[1] == column for row in cursor.fetchall())


def ensure_table_admin_operations(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_operations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            operation_type TEXT,
            target_user_id INTEGER,
            target_channel_id TEXT,
            status TEXT,
            error_message TEXT,
            performed_by INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def ensure_table_bot_clones_columns(conn: sqlite3.Connection) -> None:
    with closing(conn.cursor()) as cursor:
        # Create table if totally missing
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bot_clones (
                bot_id TEXT PRIMARY KEY,
                bot_token TEXT
            )
            """
        )

        # Add missing columns individually (SQLite supports ADD COLUMN)
        for column_sql, check_name in [
            ("ALTER TABLE bot_clones ADD COLUMN bot_username TEXT", "bot_username"),
            ("ALTER TABLE bot_clones ADD COLUMN status TEXT DEFAULT 'inactive'", "status"),
            ("ALTER TABLE bot_clones ADD COLUMN pid INTEGER", "pid"),
            ("ALTER TABLE bot_clones ADD COLUMN started_at TIMESTAMP", "started_at"),
            ("ALTER TABLE bot_clones ADD COLUMN last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP", "last_seen"),
            ("ALTER TABLE bot_clones ADD COLUMN config_data TEXT", "config_data"),
            ("ALTER TABLE bot_clones ADD COLUMN notes TEXT", "notes"),
        ]:
            if not column_exists(cursor, "bot_clones", check_name):
                cursor.execute(column_sql)


def ensure_indices(conn: sqlite3.Connection) -> None:
    # Only create indices that are safe to (re)create
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_bot_clones_status ON bot_clones(status);
        """
    )


def main() -> None:
    db_path = get_db_path()
    print(f"Applying migrations to: {db_path}")

    with sqlite3.connect(db_path) as conn:
        # Use context-managed transaction provided by the connection
        # Avoid manual BEGIN/COMMIT to prevent 'no transaction is active' errors
        # in environments where autocommit may be enabled.
        ensure_table_admin_operations(conn)
        ensure_table_bot_clones_columns(conn)
        migrate_config_to_per_bot(conn)
        migrate_channel_intervals_to_per_bot(conn)
        ensure_indices(conn)

    print("Migrations completed successfully.")


if __name__ == "__main__":
    main()


