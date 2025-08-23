from datetime import datetime
from typing import Optional, Dict, List, Protocol
from dataclasses import dataclass
from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramNotFound
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
    _cache: Dict[int, ChatInfo] = {}
    _observers: List[CacheObserver] = []
    _invalid_chats: Dict[int, dict] = {}  # Track invalid chats with error details
    
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
                from loguru import logger
                logger.error(f"Error notifying observer about cache update: {e}")
    
    async def _notify_invalid_chat(self, chat_id: int, reason: str) -> None:
        """Notify observers about invalid chat"""
        for observer in self._observers:
            try:
                if hasattr(observer, 'on_chat_invalid'):
                    await observer.on_chat_invalid(chat_id, reason)
            except Exception as e:
                from loguru import logger
                logger.error(f"Error notifying observer about invalid chat: {e}")
    
    def _is_chat_known_invalid(self, chat_id: int) -> bool:
        """Check if chat is known to be invalid"""
        if chat_id in self._invalid_chats:
            error_time = self._invalid_chats[chat_id].get('timestamp', 0)
            # Re-check invalid chats after 1 hour
            if datetime.now().timestamp() - error_time < 3600:
                return True
            else:
                # Remove from invalid list after 1 hour for re-checking
                del self._invalid_chats[chat_id]
        return False
    
    def _mark_chat_invalid(self, chat_id: int, error_type: str, error_message: str) -> None:
        """Mark a chat as invalid with error details"""
        self._invalid_chats[chat_id] = {
            'error_type': error_type,
            'error_message': error_message,
            'timestamp': datetime.now().timestamp()
        }
        
        # Also mark in cache if exists
        if chat_id in self._cache:
            self._cache[chat_id].is_valid = False
    
    async def get_chat_info(self, bot: Bot, chat_id: int, force_refresh: bool = False) -> Optional[ChatInfo]:
        """Get chat info from cache or fetch from API with enhanced error handling"""
        from loguru import logger
        
        # Check if chat is known to be invalid
        if not force_refresh and self._is_chat_known_invalid(chat_id):
            logger.debug(f"Skipping known invalid chat {chat_id}")
            return None
        
        now = datetime.now().timestamp()
        
        # Check cache first (unless force refresh)
        if not force_refresh and chat_id in self._cache:
            chat_info = self._cache[chat_id]
            if chat_info.is_valid and now - chat_info.last_updated < self._config.cache_ttl:
                return chat_info

        try:
            # Fetch fresh data from Telegram API
            chat = await bot.get_chat(chat_id)
            
            # Try to get member count, but don't fail if we can't
            member_count = None
            try:
                member_count = await bot.get_chat_member_count(chat_id)
            except Exception as member_error:
                logger.debug(f"Could not get member count for chat {chat_id}: {member_error}")
            
            info = ChatInfo(
                id=chat_id,
                title=chat.title or f"Chat {chat_id}",
                type=chat.type,
                member_count=member_count,
                last_updated=now,
                is_valid=True
            )
            
            # Update cache
            self._cache[chat_id] = info
            
            # Remove from invalid list if it was there
            if chat_id in self._invalid_chats:
                del self._invalid_chats[chat_id]
                logger.info(f"Chat {chat_id} is now valid again")
            
            # Notify observers
            await self._notify_observers(chat_id, info)
            
            # Cleanup old entries if cache is too large
            if len(self._cache) > self._config.max_cache_size:
                oldest = min(self._cache.items(), key=lambda x: x[1].last_updated)
                del self._cache[oldest[0]]
            
            return info
            
        except TelegramBadRequest as e:
            error_msg = str(e).lower()
            
            if "group chat was upgraded to a supergroup" in error_msg:
                logger.warning(f"Chat {chat_id} was upgraded to supergroup")
                self._mark_chat_invalid(chat_id, "upgraded", str(e))
                await self._notify_invalid_chat(chat_id, "Chat was upgraded to supergroup")
                
            elif "chat not found" in error_msg:
                logger.warning(f"Chat {chat_id} not found")
                self._mark_chat_invalid(chat_id, "not_found", str(e))
                await self._notify_invalid_chat(chat_id, "Chat not found")
                
            elif "bad request" in error_msg:
                logger.warning(f"Bad request for chat {chat_id}: {e}")
                self._mark_chat_invalid(chat_id, "bad_request", str(e))
                await self._notify_invalid_chat(chat_id, f"Bad request: {e}")
                
            else:
                logger.error(f"Telegram bad request for chat {chat_id}: {e}")
                self._mark_chat_invalid(chat_id, "bad_request", str(e))
            
            return None
            
        except TelegramForbiddenError as e:
            logger.warning(f"Bot forbidden in chat {chat_id}: {e}")
            self._mark_chat_invalid(chat_id, "forbidden", str(e))
            await self._notify_invalid_chat(chat_id, "Bot was removed or blocked from chat")
            return None
            
        except TelegramNotFound as e:
            logger.warning(f"Chat {chat_id} not found: {e}")
            self._mark_chat_invalid(chat_id, "not_found", str(e))
            await self._notify_invalid_chat(chat_id, "Chat not found")
            return None
            
        except Exception as e:
            logger.error(f"Unexpected error fetching chat info for {chat_id}: {e}")
            # Don't mark as invalid for unexpected errors, might be temporary
            return None
    
    def get_invalid_chats(self) -> Dict[int, dict]:
        """Get list of invalid chats with error details"""
        return self._invalid_chats.copy()
    
    def clear_invalid_chat(self, chat_id: int) -> bool:
        """Clear a chat from invalid list (for manual retry)"""
        if chat_id in self._invalid_chats:
            del self._invalid_chats[chat_id]
            return True
        return False
    
    def clear_cache(self) -> None:
        """Clear the entire cache"""
        self._cache.clear()
    
    def remove_from_cache(self, chat_id: int) -> None:
        """Remove specific chat from cache"""
        self._cache.pop(chat_id, None)
        self._invalid_chats.pop(chat_id, None)
    
    async def cleanup_invalid_chats(self, bot) -> List[int]:
        """Clean up invalid chats and return list of removed chat IDs"""
        from database.repository import Repository
        
        removed_chats = []
        
        for chat_id, error_info in self._invalid_chats.copy().items():
            error_type = error_info.get('error_type', '')
            
            # Auto-remove chats that are clearly invalid
            if error_type in ['upgraded', 'not_found', 'forbidden']:
                try:
                    await Repository.remove_target_chat(chat_id)
                    removed_chats.append(chat_id)
                    self.remove_from_cache(chat_id)
                    from loguru import logger
                    logger.info(f"Auto-removed invalid chat {chat_id} (reason: {error_type})")
                except Exception as e:
                    from loguru import logger
                    logger.error(f"Error removing invalid chat {chat_id}: {e}")
        
        return removed_chats