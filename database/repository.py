import asyncio
import weakref
from datetime import datetime
from typing import Optional, List, Dict, Any
import aiosqlite
from contextlib import asynccontextmanager
from loguru import logger
from utils.config import Config

class DatabaseConnectionPool:
    """Connection pool manager"""
    _pool = weakref.WeakSet()
    
    @classmethod
    async def close_all(cls):
        """Close all database connections"""
        for conn in cls._pool:
            try:
                await conn.close()
            except Exception as e:
                logger.error(f"Error closing connection: {e}")
        cls._pool.clear()
    
    @classmethod
    @asynccontextmanager
    async def get_connection(cls):
        """Get a database connection from the pool"""
        config = Config()
        
        # Try to get an available connection
        for conn in cls._pool:
            if not conn.in_use:
                conn.in_use = True
                try:
                    yield conn
                finally:
                    conn.in_use = False
                return

        # Create new connection if pool not full
        if len(cls._pool) < config.max_db_connections:
            conn = await aiosqlite.connect(config.db_path)
            conn.in_use = True
            cls._pool.add(conn)
            try:
                yield conn
            finally:
                conn.in_use = False
        else:
            # Wait for available connection
            while True:
                await asyncio.sleep(0.1)
                for conn in cls._pool:
                    if not conn.in_use:
                        conn.in_use = True
                        try:
                            yield conn
                        finally:
                            conn.in_use = False
                        return

class Repository:
    """Repository pattern implementation for database operations"""
    
    @staticmethod
    async def close_db() -> None:
        """Close all database connections"""
        await DatabaseConnectionPool.close_all()
    
    @staticmethod
    async def init_db() -> None:
        """Initialize database schema"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS config (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS target_chats (
                    chat_id INTEGER PRIMARY KEY,
                    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS forward_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS last_messages (
                    channel_id TEXT PRIMARY KEY,
                    message_id INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE TABLE IF NOT EXISTS channel_intervals (
                    channel_id TEXT PRIMARY KEY,
                    next_channel_id TEXT,
                    interval_seconds INTEGER DEFAULT 900,  /* 15 минут по умолчанию */
                    FOREIGN KEY(channel_id) REFERENCES last_messages(channel_id) ON DELETE CASCADE,
                    FOREIGN KEY(next_channel_id) REFERENCES last_messages(channel_id) ON DELETE SET NULL
                );
                CREATE INDEX IF NOT EXISTS idx_forward_stats_timestamp ON forward_stats(timestamp);
                CREATE INDEX IF NOT EXISTS idx_target_chats_added_at ON target_chats(added_at);
            """)
            await db.commit()

    # Add to Repository class
    @staticmethod
    async def set_channel_interval(channel1: str, channel2: str, interval_seconds: int) -> None:
        """Set interval between two channels"""
        # You can store this in a dedicated table or as a config entry
        # Example using config table with a compound key
        key = f"channel_interval_{channel1}_{channel2}"
        await Repository.set_config(key, str(interval_seconds))
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO channel_intervals 
                (channel_id, next_channel_id, interval_seconds) 
                VALUES (?, ?, ?)
                """,
                (channel1, channel2, interval_seconds)
            )
            await db.commit()

    @staticmethod
    async def get_channel_intervals() -> Dict[str, Dict[str, Any]]:
        """Get all channel intervals"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, next_channel_id, interval_seconds FROM channel_intervals"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: {"next_channel": row[1], "interval": row[2]} for row in results}

    @staticmethod
    async def delete_channel_interval(channel_id: str) -> None:
        """Delete interval for a channel"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM channel_intervals WHERE channel_id = ?",
                (channel_id,)
            )
            await db.commit()
            
    @staticmethod
    async def get_target_chats() -> List[int]:
        """Get list of target chat IDs"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute("SELECT chat_id FROM target_chats") as cursor:
                return [row[0] for row in await cursor.fetchall()]

    @staticmethod
    async def add_target_chat(chat_id: int) -> None:
        """Add new target chat"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR IGNORE INTO target_chats (chat_id) VALUES (?)",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def remove_target_chat(chat_id: int) -> None:
        """Remove target chat"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "DELETE FROM target_chats WHERE chat_id = ?",
                (chat_id,)
            )
            await db.commit()

    @staticmethod
    async def get_config(key: str, default: Optional[str] = None) -> Optional[str]:
        """Get configuration value"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT value FROM config WHERE key = ?",
                (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else default

    @staticmethod
    async def set_config(key: str, value: str) -> None:
        """Set configuration value"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
                (key, str(value))
            )
            await db.commit()

    @staticmethod
    async def log_forward(message_id: int) -> None:
        """Log forwarded message"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                "INSERT INTO forward_stats (message_id) VALUES (?)",
                (message_id,)
            )
            await db.commit()

    @staticmethod
    async def save_last_message(channel_id: str, message_id: int) -> None:
        """Save last message ID for channel"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute(
                """
                INSERT OR REPLACE INTO last_messages 
                (channel_id, message_id, timestamp) 
                VALUES (?, ?, CURRENT_TIMESTAMP)
                """,
                (channel_id, message_id)
            )
            await db.commit()

    @staticmethod
    async def get_last_message(channel_id: str) -> Optional[int]:
        """Get last message ID for channel"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT message_id FROM last_messages WHERE channel_id = ?",
                (channel_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    @staticmethod
    async def get_all_last_messages() -> Dict[str, Dict[str, Any]]:
        """Get last message IDs for all channels"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages"
            ) as cursor:
                results = await cursor.fetchall()
                return {row[0]: {"message_id": row[1], "timestamp": row[2]} for row in results}

    @staticmethod
    async def get_latest_message() -> tuple:
        """Get the most recent message across all channels"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    return (row[0], row[1])  # (channel_id, message_id)
                return (None, None)

    @staticmethod
    async def get_stats() -> Dict[str, Any]:
        """Get forwarding statistics"""
        async with DatabaseConnectionPool.get_connection() as db:
            # Get total forwards
            async with db.execute("SELECT COUNT(*) FROM forward_stats") as cursor:
                total = (await cursor.fetchone())[0]

            # Get last forward timestamp
            async with db.execute(
                "SELECT timestamp FROM forward_stats ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                last = (await cursor.fetchone() or [None])[0]

            # Get last messages for each channel
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages"
            ) as cursor:
                last_msgs = {
                    row[0]: {"message_id": row[1], "timestamp": row[2]}
                    for row in await cursor.fetchall()
                }

            return {
                "total_forwards": total,
                "last_forward": last,
                "last_messages": last_msgs
            }
