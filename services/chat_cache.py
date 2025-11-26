from datetime import datetime
from typing import Optional, Dict, List, Protocol, Union
from dataclasses import dataclass
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNotFound
from loguru import logger
from utils.config import Config

@dataclass
class ChatInfo:
    """Data class for storing chat information"""
    id: int
    title: str
    type: str
    member_count: Optional[int] = None
    last_updated: float = 0.0
    is_valid: bool = True


@dataclass
class InvalidChatInfo:
    """Represents invalid chat metadata persisted across restarts."""
    chat_id: int
    error_type: str
    error_message: str
    last_error_at: float
    next_retry_at: Optional[float] = None

class CacheObserver(Protocol):
    """Protocol for cache update observers"""
    async def on_cache_update(self, chat_id: int, info: ChatInfo) -> None:
        """Called when chat info is updated in cache"""
        pass
    
    async def on_chat_invalid(self, chat_id: int, reason: str) -> None:
        """Called when a chat becomes invalid"""
        pass

class ChatCacheService:
    """Enhanced chat cache service with robust error handling"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ChatCacheService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        self._initialized = True
        self._config = Config()
        self._cache: Dict[int, ChatInfo] = {}
        self._observers: List[CacheObserver] = []
        self._invalid_chats: Dict[int, InvalidChatInfo] = {}
        self._alias_map: Dict[str, int] = {}
        self._invalid_loaded = False
    
    def add_observer(self, observer: CacheObserver) -> None:
        """Add observer for cache updates"""
        if observer not in self._observers:
            self._observers.append(observer)
    
    def remove_observer(self, observer: CacheObserver) -> None:
        """Remove observer"""
        if observer in self._observers:
            self._observers.remove(observer)
    
    async def _notify_observers(self, chat_id: int, info: ChatInfo) -> None:
        """Notify all observers about cache update"""
        for observer in self._observers:
            try:
                await observer.on_cache_update(chat_id, info)
            except Exception as e:
                logger.error(f"Error notifying observer about cache update: {e}")
    
    async def _notify_invalid_chat(self, chat_id: int, reason: str) -> None:
        """Notify observers about invalid chat"""
        for observer in self._observers:
            try:
                if hasattr(observer, 'on_chat_invalid'):
                    await observer.on_chat_invalid(chat_id, reason)
            except Exception as e:
                logger.error(f"Error notifying observer about invalid chat: {e}")
    
    async def initialize(self) -> None:
        """Load persisted invalid chat cache if needed."""
        if self._invalid_loaded:
            return
        await self._ensure_persistent_invalid_cache_loaded()

    async def _ensure_persistent_invalid_cache_loaded(self) -> None:
        if self._invalid_loaded:
            return
        try:
            from database.repository import Repository
            records = await Repository.get_invalid_chats_cache()
        except Exception as exc:
            logger.error(f"Failed to load invalid chat cache: {exc}")
            self._invalid_loaded = True
            return

        for raw_chat_id, payload in (records or {}).items():
            try:
                chat_id = int(raw_chat_id)
            except (TypeError, ValueError):
                continue

            data = payload or {}
            self._invalid_chats[chat_id] = InvalidChatInfo(
                chat_id=chat_id,
                error_type=data.get('error_type') or "",
                error_message=data.get('error_message') or "",
                last_error_at=data.get('last_error_at') or 0.0,
                next_retry_at=data.get('next_retry_at')
            )

        self._invalid_loaded = True

    def _resolve_cache_key(self, chat_id: Union[int, str]) -> Union[int, str]:
        if isinstance(chat_id, int):
            return chat_id
        if chat_id is None:
            return ""
        candidate = str(chat_id).strip()
        if not candidate:
            return candidate
        try:
            return int(candidate)
        except (TypeError, ValueError):
            alias = candidate.lower()
            return self._alias_map.get(alias, alias)

    def _remember_alias(self, original: Union[int, str], resolved_id: int) -> None:
        if isinstance(original, int):
            self._alias_map[str(original)] = resolved_id
            return
        key = str(original).strip().lower()
        if key:
            self._alias_map[key] = resolved_id

    def _is_chat_known_invalid(self, chat_id: int, *, now_ts: Optional[float] = None) -> bool:
        entry = self._invalid_chats.get(chat_id)
        if not entry:
            return False
        now_ts = now_ts or datetime.now().timestamp()
        if entry.next_retry_at is not None and now_ts >= entry.next_retry_at:
            return False
        return True

    async def _mark_chat_invalid(
        self,
        chat_id: int,
        error_type: str,
        error_message: str,
        retry_after_seconds: Optional[int] = None
    ) -> None:
        await self.initialize()
        now_ts = datetime.now().timestamp()
        next_retry = now_ts + retry_after_seconds if retry_after_seconds else None

        self._invalid_chats[chat_id] = InvalidChatInfo(
            chat_id=chat_id,
            error_type=error_type,
            error_message=error_message,
            last_error_at=now_ts,
            next_retry_at=next_retry
        )

        if chat_id in self._cache:
            self._cache[chat_id].is_valid = False

        try:
            from database.repository import Repository
            await Repository.upsert_invalid_chat_cache(chat_id, error_type, error_message, retry_after_seconds)
        except Exception as exc:
            logger.error(f"Failed to persist invalid chat {chat_id}: {exc}")

    async def mark_chat_invalid(
        self,
        chat_id: int,
        error_type: str,
        error_message: str,
        retry_after_seconds: Optional[int] = None
    ) -> None:
        """Public helper to mark a chat as invalid and persist the state."""
        await self._mark_chat_invalid(chat_id, error_type, error_message, retry_after_seconds)

    async def _clear_invalid_chat(self, chat_id: int) -> None:
        if chat_id in self._invalid_chats:
            del self._invalid_chats[chat_id]
        try:
            from database.repository import Repository
            await Repository.remove_invalid_chat_cache(chat_id)
        except Exception as exc:
            logger.warning(f"Failed to clear invalid chat cache for {chat_id}: {exc}")

    async def get_chat_info(self, bot: Bot, chat_id: Union[int, str], force_refresh: bool = False) -> Optional[ChatInfo]:
        """Get chat info from cache or fetch from API with enhanced error handling."""
        await self.initialize()
        now = datetime.now().timestamp()
        cache_key = self._resolve_cache_key(chat_id)
        numeric_key = cache_key if isinstance(cache_key, int) else None

        if not force_refresh and numeric_key is not None and self._is_chat_known_invalid(numeric_key, now_ts=now):
            logger.debug(f"Skipping known invalid chat {numeric_key}")
            return None

        if not force_refresh and numeric_key is not None:
            cached = self._cache.get(numeric_key)
            if cached and cached.is_valid and now - cached.last_updated < self._config.cache_ttl:
                return cached

        lookup_value = chat_id
        if isinstance(chat_id, str):
            stripped = chat_id.strip()
            if stripped.startswith('@') or stripped.lstrip('-').isdigit():
                lookup_value = stripped
            else:
                lookup_value = f"@{stripped}"

        try:
            chat = await bot.get_chat(lookup_value)

            member_count = None
            try:
                member_count = await bot.get_chat_member_count(chat.id)
            except Exception as member_error:
                logger.debug(f"Could not get member count for chat {chat.id}: {member_error}")

            info = ChatInfo(
                id=chat.id,
                title=(chat.title or getattr(chat, "full_name", None) or f"Chat {chat.id}"),
                type=chat.type,
                member_count=member_count,
                last_updated=now,
                is_valid=True
            )

            self._cache[chat.id] = info
            self._remember_alias(chat_id, chat.id)

            if numeric_key is not None and numeric_key != chat.id:
                self._cache.pop(numeric_key, None)

            if chat.id in self._invalid_chats:
                await self._clear_invalid_chat(chat.id)
                logger.info(f"Chat {chat.id} is now valid again")

            await self._notify_observers(chat.id, info)

            if len(self._cache) > self._config.max_cache_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_updated)
                del self._cache[oldest[0]]

            return info

        except TelegramBadRequest as e:
            error_msg = str(e).lower()
            target_id = numeric_key

            if "group chat was upgraded to a supergroup" in error_msg:
                logger.warning(f"Chat {chat_id} was upgraded to supergroup")
                if target_id is not None:
                    await self._mark_chat_invalid(target_id, "upgraded", str(e))
                    await self._notify_invalid_chat(target_id, "Chat was upgraded to supergroup")
            elif "chat not found" in error_msg or "chat_id_invalid" in error_msg:
                logger.warning(f"Chat {chat_id} not found")
                if target_id is not None:
                    await self._mark_chat_invalid(target_id, "not_found", str(e))
                    await self._notify_invalid_chat(target_id, "Chat not found")
            elif "bot was kicked" in error_msg or "bot is not a member" in error_msg:
                logger.warning(f"Bot removed from chat {chat_id}: {e}")
                if target_id is not None:
                    await self._mark_chat_invalid(target_id, "kicked", str(e))
                    await self._notify_invalid_chat(target_id, "Bot removed from chat")
            else:
                logger.error(f"Telegram bad request for chat {chat_id}: {e}")

            return None

        except TelegramForbiddenError as e:
            logger.warning(f"Bot forbidden in chat {chat_id}: {e}")
            if numeric_key is not None:
                await self._mark_chat_invalid(numeric_key, "forbidden", str(e))
                await self._notify_invalid_chat(numeric_key, "Bot was removed or blocked from chat")
            return None

        except TelegramNotFound as e:
            logger.warning(f"Chat {chat_id} not found: {e}")
            if numeric_key is not None:
                await self._mark_chat_invalid(numeric_key, "not_found", str(e))
                await self._notify_invalid_chat(numeric_key, "Chat not found")
            return None

        except Exception as e:
            logger.error(f"Unexpected error fetching chat info for {chat_id}: {e}")
            return None

    def get_invalid_chats(self) -> Dict[int, dict]:
        """Get list of invalid chats with error details."""
        return {
            chat_id: {
                'error_type': info.error_type,
                'error_message': info.error_message,
                'last_error_at': info.last_error_at,
                'next_retry_at': info.next_retry_at
            }
            for chat_id, info in self._invalid_chats.items()
        }

    async def clear_invalid_chat(self, chat_id: int) -> bool:
        """Clear a chat from invalid list (for manual retry)."""
        if chat_id not in self._invalid_chats:
            return False
        await self._clear_invalid_chat(chat_id)
        if chat_id in self._cache:
            self._cache[chat_id].is_valid = True
        return True

    def clear_cache(self) -> None:
        """Clear the entire cache."""
        self._cache.clear()
        self._alias_map.clear()

    def drop_cache_entry(self, chat_id: int) -> None:
        """Remove a chat from the in-memory cache only."""
        self._cache.pop(chat_id, None)
        aliases = [alias for alias, mapped in self._alias_map.items() if mapped == chat_id]
        for alias in aliases:
            self._alias_map.pop(alias, None)

    async def remove_from_cache(self, chat_id: int) -> None:
        """Remove specific chat from cache and persistent invalid list."""
        self._cache.pop(chat_id, None)
        aliases = [alias for alias, mapped in self._alias_map.items() if mapped == chat_id]
        for alias in aliases:
            self._alias_map.pop(alias, None)
        await self._clear_invalid_chat(chat_id)

    async def cleanup_invalid_chats(self, _bot) -> List[int]:
        """Return chats whose retry window elapsed for potential revalidation."""
        await self.initialize()
        now = datetime.now().timestamp()
        return [
            chat_id
            for chat_id, entry in self._invalid_chats.items()
            if entry.next_retry_at is not None and now >= entry.next_retry_at
        ]