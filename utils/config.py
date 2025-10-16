import asyncio
import os
import json
from typing import List, Optional
from dotenv import load_dotenv
from loguru import logger

class Config:
    """Singleton configuration class"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        load_dotenv()
        
        # Load environment variables
        self.bot_token: str = os.getenv("BOT_TOKEN", "")
        
        # Load admin IDs from comma-separated list
        admin_ids_str = os.getenv("ADMIN_IDS", os.getenv("OWNER_ID", ""))
        self.admin_ids: List[int] = []
        
        if admin_ids_str:
            try:
                # Split by comma and convert to integers
                self.admin_ids = [int(id.strip()) for id in admin_ids_str.split(',') if id.strip()]
            except ValueError as e:
                logger.error(f"Error parsing admin IDs: {e}")
                # Fallback to empty list if parsing fails
                self.admin_ids = []
        
        # For backwards compatibility - still store the first admin as owner_id
        self.owner_id: int = self.admin_ids[0] if self.admin_ids else 0
        
        self.source_channels: List[str] = []
        self.bot_id: str = "main"
        self.clone_admins_path: str = "clone_admins.json"
        self._clone_admins_cache: set[int] = set()
        
        # Support for backwards compatibility - add initial source channel if provided
        initial_source = os.getenv("SOURCE_CHANNEL", "").lstrip('@')
        if initial_source:
            self.source_channels.append(initial_source)
            
        # Resolve database path to absolute to avoid per-process relative CWD issues
        db_env_path = os.getenv("DB_PATH", "forwarder.db")
        self.db_path: str = os.path.abspath(os.path.expanduser(db_env_path))
        logger.info(f"Using database file: {self.db_path}")
        
        # Try to load additional source channels from config file
        self._load_channels_from_config()

        # Load clone-specific administrators before validating required settings
        self._load_clone_admins()
        
        # Validate required settings
        if not all([self.bot_token, self.admin_ids]):
            raise ValueError("Missing required environment variables")

        # Cache settings
        self.cache_ttl: int = 300  # 5 minutes cache for chat info
        self.max_cache_size: int = 100
        
        # Database connection settings
        self.max_db_connections: int = 5
        
        self._initialized = True
    
    def save_config(self):
        """Save current configuration to bot_config.json"""
        try:
            admin_ids_to_save = list(self.admin_ids)
            if getattr(self, "bot_id", "main") != "main" and getattr(self, "_clone_admins_cache", None):
                admin_ids_to_save = [aid for aid in admin_ids_to_save if aid not in self._clone_admins_cache]

            with open('bot_config.json', 'w') as f:
                config_data = {
                    'source_channels': self.source_channels,
                    'admin_ids': admin_ids_to_save
                }
                json.dump(config_data, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save config: {e}")

    def is_admin(self, user_id: int) -> bool:
        """Check if user is an admin"""
        return user_id in self.admin_ids

    def set_bot_id(self, bot_id: str) -> None:
        """Assign identifier for current bot instance and reload clone admins"""
        if getattr(self, "bot_id", None) == bot_id:
            return

        # Remove previously loaded clone-specific admins before switching context
        if getattr(self, "_clone_admins_cache", None):
            self.admin_ids = [aid for aid in self.admin_ids if aid not in self._clone_admins_cache]
            self._clone_admins_cache.clear()

        self.bot_id = bot_id or "main"
        self._load_clone_admins()

    def add_clone_admin(self, user_id: int) -> bool:
        """Add admin specific to current bot clone and persist"""
        try:
            user_id = int(user_id)
        except (TypeError, ValueError):
            return False

        if user_id in self.admin_ids:
            # Already has admin rights (global or clone-specific)
            if getattr(self, "_clone_admins_cache", None) is not None:
                self._clone_admins_cache.add(user_id)
            return False

        self.admin_ids.append(user_id)
        if getattr(self, "_clone_admins_cache", None) is not None:
            self._clone_admins_cache.add(user_id)
        self._save_clone_admins()
        return True
    
    def _load_channels_from_config(self):
        """Load channels from configuration file"""
        try:
            with open('bot_config.json', 'r') as f:
                config = json.load(f)
                if 'source_channels' in config and isinstance(config['source_channels'], list):
                    # Add channels not already in the list
                    for channel in config['source_channels']:
                        channel = str(channel).lstrip('@')
                        if channel and channel not in self.source_channels:
                            self.source_channels.append(channel)
        except (FileNotFoundError, json.JSONDecodeError):
            # Create default config if not exists
            self._save_channels_to_config()
    
    def _save_channels_to_config(self):
        """Save channels to configuration file"""
        try:
            config = {}
            # Try to load existing config first
            try:
                with open('bot_config.json', 'r') as f:
                    config = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError):
                config = {"source_channels": [], "target_chats": [], "last_message_ids": {}}
            
            # Update source channels
            config['source_channels'] = self.source_channels
            
            # Save updated config
            with open('bot_config.json', 'w') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save channels to config: {e}")
            
    # Fix in utils/config.py
    def add_source_channel(self, channel: str) -> bool:
        """Add a new source channel and save to config"""
        channel = channel.lstrip('@')
        if channel and channel not in self.source_channels:
            self.source_channels.append(channel)
            self._save_channels_to_config()
            return True
        return False
        
    def remove_source_channel(self, channel: str) -> bool:
        """Remove a source channel and update config"""
        channel = channel.lstrip('@')
        if channel in self.source_channels:
            self.source_channels.remove(channel)
            self._save_channels_to_config()
            return True
        return False

    def _load_clone_admins(self) -> None:
        """Load clone-specific admins for current bot from file"""
        try:
            with open(self.clone_admins_path, 'r') as fh:
                data = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}

        clone_admins = data.get(self.bot_id, []) if isinstance(data, dict) else []

        # Ensure clone-specific admins recorded as ints
        cleaned_admins: List[int] = []
        for admin_id in clone_admins:
            try:
                cleaned_admins.append(int(admin_id))
            except (TypeError, ValueError):
                continue

        # Remove duplicates from base list before adding
        for existing_id in cleaned_admins:
            if existing_id not in self.admin_ids:
                self.admin_ids.append(existing_id)

        if hasattr(self, "_clone_admins_cache"):
            self._clone_admins_cache = set(cleaned_admins)

    def _save_clone_admins(self) -> None:
        """Persist clone-specific admins to file"""
        if not getattr(self, "bot_id", None):
            return

        try:
            with open(self.clone_admins_path, 'r') as fh:
                data = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            data = {}

        if not isinstance(data, dict):
            data = {}

        current_admins = data.get(self.bot_id, [])
        normalized = []
        for admin_id in current_admins:
            try:
                val = int(admin_id)
            except (TypeError, ValueError):
                continue
            normalized.append(val)

        for admin_id in getattr(self, "_clone_admins_cache", set()):
            if admin_id not in normalized:
                normalized.append(admin_id)

        data[self.bot_id] = sorted(normalized)

        try:
            with open(self.clone_admins_path, 'w') as fh:
                json.dump(data, fh, indent=2)
        except Exception as e:
            logger.error(f"Failed to save clone admins: {e}")