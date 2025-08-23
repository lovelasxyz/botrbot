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
        """Initialize database schema with new tables"""
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
                    interval_seconds INTEGER DEFAULT 900,
                    FOREIGN KEY(channel_id) REFERENCES last_messages(channel_id) ON DELETE CASCADE,
                    FOREIGN KEY(next_channel_id) REFERENCES last_messages(channel_id) ON DELETE SET NULL
                );
                CREATE TABLE IF NOT EXISTS bot_clones (
                    bot_id TEXT PRIMARY KEY,
                    bot_token TEXT,
                    bot_username TEXT,
                    status TEXT DEFAULT 'inactive',
                    pid INTEGER,
                    started_at TIMESTAMP,
                    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    config_data TEXT,
                    notes TEXT
                );
                CREATE TABLE IF NOT EXISTS channel_test_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id TEXT,
                    test_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT,
                    message_id INTEGER,
                    error_message TEXT,
                    test_duration_ms INTEGER
                );
                CREATE TABLE IF NOT EXISTS admin_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    action TEXT,
                    user_id INTEGER,
                    target TEXT,
                    status TEXT,
                    details TEXT,
                    initiated_by INTEGER,
                    batch_id TEXT
                );
                -- Ensure admin_operations table exists for history/stats queries
                CREATE TABLE IF NOT EXISTS admin_operations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    operation_type TEXT,
                    target_user_id INTEGER,
                    target_channel_id TEXT,
                    status TEXT,
                    error_message TEXT,
                    performed_by INTEGER,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_forward_stats_timestamp ON forward_stats(timestamp);
                CREATE INDEX IF NOT EXISTS idx_target_chats_added_at ON target_chats(added_at);
                CREATE INDEX IF NOT EXISTS idx_channel_test_results_timestamp ON channel_test_results(test_timestamp);
                CREATE INDEX IF NOT EXISTS idx_admin_log_timestamp ON admin_log(timestamp);
                CREATE INDEX IF NOT EXISTS idx_admin_log_batch_id ON admin_log(batch_id);
                CREATE INDEX IF NOT EXISTS idx_bot_clones_status ON bot_clones(status);
            """)
            
            # Apply lightweight migrations for existing databases
            try:
                async with db.execute("PRAGMA table_info('bot_clones')") as cursor:
                    columns_info = await cursor.fetchall()
                    existing_columns = {row[1] for row in columns_info}

                # Add missing columns one by one (SQLite supports ADD COLUMN)
                if 'bot_username' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN bot_username TEXT")
                if 'status' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN status TEXT DEFAULT 'inactive'")
                if 'pid' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN pid INTEGER")
                if 'started_at' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN started_at TIMESTAMP")
                if 'last_seen' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                if 'config_data' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN config_data TEXT")
                if 'notes' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN notes TEXT")
            except Exception as e:
                # Log but don't fail initialization
                logger.error(f"DB migration error: {e}")
            await db.commit()

    @staticmethod
    async def save_bot_clone(bot_id: str, bot_token: str, bot_username: str, status: str = 'inactive', 
                            pid: int = None, config_data: str = None, notes: str = None) -> None:
        """Save or update bot clone information"""
        async with DatabaseConnectionPool.get_connection() as db:
            # Defensive migration in case init_db wasn't called earlier
            try:
                async with db.execute("PRAGMA table_info('bot_clones')") as cursor:
                    columns_info = await cursor.fetchall()
                    existing_columns = {row[1] for row in columns_info}
                if 'started_at' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN started_at TIMESTAMP")
                if 'last_seen' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                if 'config_data' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN config_data TEXT")
                if 'notes' not in existing_columns:
                    await db.execute("ALTER TABLE bot_clones ADD COLUMN notes TEXT")
            except Exception as e:
                logger.warning(f"Deferred migration during save_bot_clone: {e}")
            await db.execute("""
                INSERT INTO bot_clones 
                (bot_id, bot_token, bot_username, status, pid, started_at, last_seen, config_data, notes)
                VALUES (
                    ?, ?, ?, ?, ?, 
                    CASE WHEN ? = 'active' THEN CURRENT_TIMESTAMP ELSE NULL END,
                    CURRENT_TIMESTAMP, ?, ?
                )
                ON CONFLICT(bot_id) DO UPDATE SET
                    bot_token = excluded.bot_token,
                    bot_username = excluded.bot_username,
                    status = excluded.status,
                    pid = excluded.pid,
                    started_at = CASE 
                        WHEN excluded.status = 'active' THEN CURRENT_TIMESTAMP 
                        ELSE bot_clones.started_at 
                    END,
                    last_seen = CURRENT_TIMESTAMP,
                    config_data = excluded.config_data,
                    notes = excluded.notes
            """, (bot_id, bot_token, bot_username, status, pid, status, config_data, notes))
            await db.commit()


    @staticmethod
    async def update_clone_status(bot_id: str, status: str, pid: int = None, notes: str = None) -> None:
        """Update clone status"""
        async with DatabaseConnectionPool.get_connection() as db:
            if pid is not None:
                await db.execute("""
                    UPDATE bot_clones 
                    SET status = ?, pid = ?, last_seen = CURRENT_TIMESTAMP, notes = ?
                    WHERE bot_id = ?
                """, (status, pid, notes, bot_id))
            else:
                await db.execute("""
                    UPDATE bot_clones 
                    SET status = ?, last_seen = CURRENT_TIMESTAMP, notes = ?
                    WHERE bot_id = ?
                """, (status, notes, bot_id))
            await db.commit()

    @staticmethod
    async def remove_bot_clone(bot_id: str) -> None:
        """Remove bot clone from database"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute("DELETE FROM bot_clones WHERE bot_id = ?", (bot_id,))
            await db.commit()

    # Методы для результатов тестирования каналов
    @staticmethod
    async def save_channel_test_result(channel_id: str, status: str, message_id: int = None, 
                                    error_message: str = None, test_duration_ms: int = None) -> None:
        """Save channel test result"""
        async with DatabaseConnectionPool.get_connection() as db:
            await db.execute("""
                INSERT INTO channel_test_results 
                (channel_id, status, message_id, error_message, test_duration_ms)
                VALUES (?, ?, ?, ?, ?)
            """, (channel_id, status, message_id, error_message, test_duration_ms))
            await db.commit()

    @staticmethod
    async def get_channel_test_history(channel_id: str = None, limit: int = 100) -> List[Dict[str, Any]]:
        """Get channel test history"""
        async with DatabaseConnectionPool.get_connection() as db:
            if channel_id:
                query = """
                    SELECT channel_id, test_timestamp, status, message_id, error_message, test_duration_ms
                    FROM channel_test_results 
                    WHERE channel_id = ?
                    ORDER BY test_timestamp DESC LIMIT ?
                """
                params = (channel_id, limit)
            else:
                query = """
                    SELECT channel_id, test_timestamp, status, message_id, error_message, test_duration_ms
                    FROM channel_test_results 
                    ORDER BY test_timestamp DESC LIMIT ?
                """
                params = (limit,)
            
            async with db.execute(query, params) as cursor:
                results = await cursor.fetchall()
                return [
                    {
                        'channel_id': row[0],
                        'test_timestamp': row[1],
                        'status': row[2],
                        'message_id': row[3],
                        'error_message': row[4],
                        'test_duration_ms': row[5]
                    }
                    for row in results
                ]

    # Методы для операций с администраторами
    @staticmethod
    async def log_admin_operation(action: str, user_id: int, target: str, status: str, details: str, initiated_by: int, batch_id: str = None) -> str:
        """Log an admin operation and return the operation ID."""
        async with DatabaseConnectionPool.get_connection() as conn:
            cursor = await conn.execute(
                "INSERT INTO admin_log (action, user_id, target, status, details, initiated_by, batch_id) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (action, user_id, target, status, details, initiated_by, batch_id)
            )
            await conn.commit()
            operation_id = str(cursor.lastrowid)
            await cursor.close()
            return operation_id

    @staticmethod
    async def get_admin_history(limit: int = 50):
        """Fetch admin operation history"""
        async with DatabaseConnectionPool.get_connection() as conn:
            cursor = await conn.execute("SELECT id, timestamp, action, user_id, target, status, details, initiated_by FROM admin_log ORDER BY timestamp DESC LIMIT ?", (limit,))
            logs = await cursor.fetchall()
            await cursor.close()
            return logs

    @staticmethod
    async def get_admin_operations_history(limit: int = 50) -> List[Dict[str, Any]]:
        """Get admin operations history"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute("""
                SELECT operation_type, target_user_id, target_channel_id, status, error_message, performed_by, timestamp
                FROM admin_operations 
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,)) as cursor:
                results = await cursor.fetchall()
                return [
                    {
                        'operation_type': row[0],
                        'target_user_id': row[1],
                        'target_channel_id': row[2],
                        'status': row[3],
                        'error_message': row[4],
                        'performed_by': row[5],
                        'timestamp': row[6]
                    }
                    for row in results
                ]

    @staticmethod
    async def get_batch_operation_details(batch_id: str):
        """Fetch all logs for a specific batch operation."""
        async with DatabaseConnectionPool.get_connection() as conn:
            cursor = await conn.execute(
                "SELECT id, timestamp, action, user_id, target, status, details, initiated_by FROM admin_log WHERE batch_id = ? ORDER BY id",
                (batch_id,)
            )
            logs = await cursor.fetchall()
            await cursor.close()
            return logs

    # Методы для получения статистики
    @staticmethod
    async def get_enhanced_stats() -> Dict[str, Any]:
        """Get enhanced statistics including clones and tests"""
        async with DatabaseConnectionPool.get_connection() as db:
            stats = {}
            
            # Базовая статистика пересылок
            async with db.execute("SELECT COUNT(*) FROM forward_stats") as cursor:
                stats['total_forwards'] = (await cursor.fetchone())[0]

            async with db.execute(
                "SELECT timestamp FROM forward_stats ORDER BY timestamp DESC LIMIT 1"
            ) as cursor:
                row = await cursor.fetchone()
                stats['last_forward'] = row[0] if row else None

            # Статистика клонов
            async with db.execute("SELECT COUNT(*) FROM bot_clones") as cursor:
                stats['total_clones'] = (await cursor.fetchone())[0]
                
            async with db.execute("SELECT COUNT(*) FROM bot_clones WHERE status = 'active'") as cursor:
                stats['active_clones'] = (await cursor.fetchone())[0]

            # Статистика тестов каналов
            async with db.execute("SELECT COUNT(*) FROM channel_test_results") as cursor:
                stats['total_tests'] = (await cursor.fetchone())[0]
                
            async with db.execute("""
                SELECT COUNT(*) FROM channel_test_results 
                WHERE status = 'success' AND test_timestamp > datetime('now', '-24 hours')
            """) as cursor:
                stats['successful_tests_24h'] = (await cursor.fetchone())[0]

            # Последние операции админов
            async with db.execute("SELECT COUNT(*) FROM admin_operations") as cursor:
                stats['total_admin_operations'] = (await cursor.fetchone())[0]
                
            async with db.execute("""
                SELECT COUNT(*) FROM admin_operations 
                WHERE status = 'success' AND timestamp > datetime('now', '-24 hours')
            """) as cursor:
                stats['successful_admin_ops_24h'] = (await cursor.fetchone())[0]

            # Последние сообщения по каналам
            async with db.execute(
                "SELECT channel_id, message_id, timestamp FROM last_messages"
            ) as cursor:
                results = await cursor.fetchall()
                stats['last_messages'] = {
                    row[0]: {"message_id": row[1], "timestamp": row[2]}
                    for row in results
                }

            return stats
    @staticmethod
    async def get_bot_clones() -> List[Dict[str, Any]]:
        """Get all bot clones"""
        async with DatabaseConnectionPool.get_connection() as db:
            async with db.execute("""
                SELECT bot_id, bot_token, bot_username, status, pid, started_at, last_seen, config_data, notes
                FROM bot_clones ORDER BY last_seen DESC
            """) as cursor:
                results = await cursor.fetchall()
                return [
                    {
                        'bot_id': row[0],
                        'bot_token': row[1],
                        'bot_username': row[2],
                        'status': row[3],
                        'pid': row[4],
                        'started_at': row[5],
                        'last_seen': row[6],
                        'config_data': row[7],
                        'notes': row[8]
                    }
                    for row in results
                ]
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
