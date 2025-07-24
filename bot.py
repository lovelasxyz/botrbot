
import asyncio
import json
import os
import shutil
import sys
from datetime import datetime
import threading
import importlib.util
from typing import Optional, List, Dict, Any
from multiprocessing import Process
import multiprocessing

from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder

from utils.config import Config
from utils.bot_state import BotContext, IdleState, RunningState
from utils.keyboard_factory import KeyboardFactory
from database.repository import Repository
from services.chat_cache import ChatCacheService, CacheObserver, ChatInfo
from commands.commands import (
    StartCommand,
    HelpCommand,
    SetLastMessageCommand,
    GetLastMessageCommand,
    ForwardNowCommand,
    TestMessageCommand,
    FindLastMessageCommand
)
class BotManager:
    """Manages multiple bot instances"""
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(BotManager, cls).__new__(cls)
            # Create a manager instance properly
            import multiprocessing
            cls._instance.manager = multiprocessing.Manager()
            cls._instance.bots = cls._instance.manager.dict()
            cls._instance.processes = {}
            
            from loguru import logger
            logger.info("BotManager singleton created")
        return cls._instance
    
    def add_bot(self, bot_id: str, process: Process):
        """Add a bot process to the manager"""
        self.bots[bot_id] = {
            'status': 'running',
            'pid': process.pid,
            'started_at': datetime.now().isoformat()
        }
        self.processes[bot_id] = process
        
        from loguru import logger
        logger.info(f"Added bot {bot_id} to manager. Total bots: {len(self.bots)}")
        logger.debug(f"Current bots: {list(self.bots.keys())}")
    
    def remove_bot(self, bot_id: str):
        """Remove a bot from the manager"""
        if bot_id in self.processes:
            process = self.processes[bot_id]
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
            del self.processes[bot_id]
            del self.bots[bot_id]
    
    def get_bot_status(self, bot_id: str):
        """Get status of a specific bot"""
        return self.bots.get(bot_id, None)
    
    def list_bots(self):
        """List all managed bots"""
        from loguru import logger
        logger.debug(f"Listing bots. Total: {len(self.bots)}, Keys: {list(self.bots.keys())}")
        return dict(self.bots)



# Add this function to run a bot in a separate process
def run_bot_process(bot_token: str, owner_id: int, source_channels: list, bot_id: str):
    """Wrapper to run bot in a separate process"""
    # Set up logging for the subprocess
    from loguru import logger
    logger.add(f"bot_{bot_id}.log", rotation="10 MB")
    
    # Create new event loop for this process
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(run_bot_instance(bot_token, owner_id, source_channels, bot_id))
    except Exception as e:
        logger.error(f"Error in bot process {bot_id}: {e}")
    finally:
        loop.close()


# Add this function to run a bot in a separate process
async def run_bot_instance(bot_token: str, owner_id: int, source_channels: list, bot_id: str):
    """Run a bot instance with specific configuration"""
    import os
    import sys
    
    # Create a temporary config for this bot instance
    os.environ['BOT_TOKEN'] = bot_token
    os.environ['OWNER_ID'] = str(owner_id)
    
    # Create a custom config class for this instance
    from utils.config import Config
    
    # Override the singleton pattern for this process
    Config._instance = None
    config = Config()
    config.bot_token = bot_token
    config.owner_id = owner_id
    config.source_channels = source_channels
    
    # Create a new bot instance
    bot_instance = ForwarderBot()
    bot_instance.bot_id = bot_id  # Add identifier
    
    try:
        await bot_instance.start()
    except Exception as e:
        logger.error(f"Bot {bot_id} crashed: {e}")
        raise


class ForwarderBot(CacheObserver):
    """Main bot class with Observer pattern implementation"""
    
    def __init__(self):
        self.config = Config()
        self.bot = Bot(token=self.config.bot_token)
        self.dp = Dispatcher()
        self.context = BotContext(self.bot, self.config)
        self.cache_service = ChatCacheService()
        self.awaiting_channel_input = None  # Track if waiting for channel input
        self.bot_manager = BotManager()
        self.bot_id = "main"  # Identifier for the main bot
        self.child_bots = []  # Track spawned bots

        # Register as cache observer
        self.cache_service.add_observer(self)
        
        self._setup_handlers()
        
    def is_admin(self, user_id: int) -> bool:
        """Check if user is an admin"""
        return self.config.is_admin(user_id)
    
    async def clone_bot_prompt(self, callback: types.CallbackQuery):
        """Prompt for cloning the bot"""
        if not self.is_admin(callback.from_user.id): 
            return
        
        # Set state to wait for new token
        self.awaiting_clone_token = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_main")
        
        await callback.message.edit_text(
            "🤖 Клонирование бота\n\n"
            "1. Создайте нового бота через @BotFather\n"
            "2. Получите новый токен бота\n"
            "3. Отправьте токен сюда\n\n"
            "После проверки токена вы сможете выбрать:\n"
            "• Запустить клон в текущем процессе\n"
            "• Создать файлы для отдельного запуска\n\n"
            "Отправьте новый токен сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()


    # Let's also add the overwrite_clone method that was referenced earlier
    async def overwrite_clone(self, callback: types.CallbackQuery):
        """Handler for overwriting existing clone"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Parse data: overwrite_clone_dirname_token
        parts = callback.data.split('_', 3)
        if len(parts) != 4:
            await callback.answer("Ошибка в данных")
            return
        
        clone_dir = parts[2]
        new_token = parts[3]
        
        # Delete existing clone
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
        
        if os.path.exists(clone_path):
            shutil.rmtree(clone_path)
        
        # Perform clone
        await self._perform_bot_clone(new_token, clone_dir, callback.message)
        await callback.answer()

        
    async def _perform_bot_clone(self, new_token: str, clone_dir: str, progress_msg=None):
        """Perform the actual bot cloning"""
        try:
            # Get bot info for the new token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Get paths
            current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
            
            # Create clone directory
            os.makedirs(clone_path, exist_ok=True)
            
            # Files and directories to copy
            items_to_copy = [
                'bot.py',
                'requirements.txt',
                'Dockerfile',
                'utils',
                'commands',
                'services',
                'database'
            ]
            
            # Copy files and directories
            for item in items_to_copy:
                src = os.path.join(current_dir, item)
                dst = os.path.join(clone_path, item)
                
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                elif os.path.isfile(src):
                    shutil.copy2(src, dst)
            
            # Create new .env file with new token
            env_content = f"""# Telegram Bot Token from @BotFather
    BOT_TOKEN={new_token}

    # Your Telegram user ID (get from @userinfobot)
    OWNER_ID={self.config.owner_id}

    # Admin IDs (comma separated for multiple admins)
    ADMIN_IDS={','.join(map(str, self.config.admin_ids))}

    # Source channel username or ID (bot must be admin)
    # Can be either numeric ID (-100...) or channel username without @
    SOURCE_CHANNEL={self.config.source_channels[0] if self.config.source_channels else ''}
    """
            
            with open(os.path.join(clone_path, '.env'), 'w') as f:
                f.write(env_content)
            
            # Copy bot_config.json with same channels
            if os.path.exists(os.path.join(current_dir, 'bot_config.json')):
                shutil.copy2(
                    os.path.join(current_dir, 'bot_config.json'),
                    os.path.join(clone_path, 'bot_config.json')
                )
            
            # Create a start script for Linux
            start_script = f"""#!/bin/bash
    cd "{clone_path}"
    python bot.py
    """
            
            start_script_path = os.path.join(clone_path, 'start_bot.sh')
            with open(start_script_path, 'w') as f:
                f.write(start_script)
            
            # Make the script executable
            os.chmod(start_script_path, 0o755)
            
            # Create Windows start script
            start_script_windows = f"""@echo off
    cd /d "{clone_path}"
    python bot.py
    pause
    """
            
            with open(os.path.join(clone_path, 'start_bot.bat'), 'w') as f:
                f.write(start_script_windows)
            
            # Create README.md for the clone
            readme_content = f"""# Bot Clone: @{bot_info.username}

    This is a clone of the main forwarding bot.

    ## Configuration
    - Bot Token: Configured in .env
    - Owner ID: {self.config.owner_id}
    - Admin IDs: {', '.join(map(str, self.config.admin_ids))}
    - Source Channels: {', '.join(self.config.source_channels)}

    ## Running the bot

    ### Linux/Mac:
    ```bash
    ./start_bot.sh
    ```

    ### Windows:
    ```bash
    start_bot.bat
    ```

    ### Manual:
    ```bash
    python bot.py
    ```

    ## Important Notes
    - Make sure the bot is admin in all source channels
    - The bot will forward messages to the same target chats as the main bot
    - Database is separate from the main bot
    - All configured admins can manage this bot clone
    """
            
            with open(os.path.join(clone_path, 'README.md'), 'w') as f:
                f.write(readme_content)
            
            if progress_msg:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data="back_to_main")
                
                success_text = (
                    f"✅ Бот успешно клонирован!\n\n"
                    f"📁 Папка: {clone_dir}\n"
                    f"🤖 Имя бота: @{bot_info.username}\n\n"
                    f"Для запуска клона:\n"
                    f"1. Перейдите в папку: {clone_path}\n"
                    f"2. Запустите: `python bot.py` или используйте скрипт start_bot.sh (Linux) / start_bot.bat (Windows)\n\n"
                    f"Клон будет работать независимо с теми же настройками каналов и администраторами."
                )
                
                await progress_msg.edit_text(success_text, reply_markup=kb.as_markup())
            
            logger.info(f"Successfully cloned bot to {clone_dir}")
            
        except Exception as e:
            logger.error(f"Error during bot clone: {e}")
            if progress_msg:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад", callback_data="back_to_main")
                
                await progress_msg.edit_text(
                    f"❌ Ошибка при клонировании: {e}",
                    reply_markup=kb.as_markup()
                )
            raise


    async def create_clone_files(self, callback: types.CallbackQuery):
        """Create clone files for separate deployment"""
        if not self.is_admin(callback.from_user.id): 
            return
        
        # Parse data: clone_files_token
        parts = callback.data.split('_', 2)
        if len(parts) != 3:
            await callback.answer("Ошибка в данных")
            return
        
        new_token = parts[2]
        
        progress_msg = await callback.message.edit_text("🔄 Создание файлов клона...")
        
        try:
            # Verify the new token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            # Create clone directory name
            clone_dir = f"bot_clone_{bot_info.username}"
            
            # Check if clone already exists
            current_dir = os.path.dirname(os.path.abspath(__file__))
            clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
            
            if os.path.exists(clone_path):
                kb = InlineKeyboardBuilder()
                kb.button(text="Да, перезаписать", callback_data=f"overwrite_clone_{clone_dir}_{new_token}")
                kb.button(text="Отмена", callback_data="back_to_main")
                kb.adjust(2)
                
                await progress_msg.edit_text(
                    f"⚠️ Клон бота уже существует в папке: {clone_dir}\n\n"
                    "Перезаписать существующий клон?",
                    reply_markup=kb.as_markup()
                )
                return
            
            # Create clone files
            await self._perform_bot_clone(new_token, clone_dir, progress_msg)
            
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад", callback_data="back_to_main")
            
            await progress_msg.edit_text(
                f"❌ Ошибка при создании файлов клона: {e}",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to create clone files: {e}")
        
        await callback.answer()
    async def clone_bot_inline(self, callback: types.CallbackQuery):
        """Run cloned bot in the same solution"""
        if not self.is_admin(callback.from_user.id): 
            return
        
        # Parse data: clone_inline_token
        parts = callback.data.split('_', 2)
        if len(parts) != 3:
            await callback.answer("Ошибка в данных")
            return
        
        new_token = parts[2]
        
        await callback.message.edit_text("🚀 Запускаю клон бота...")
        
        try:
            # Verify the token
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            bot_id = f"bot_{bot_info.username}"
            
            # Check if this bot is already running
            if bot_id in self.bot_manager.processes:
                if self.bot_manager.processes[bot_id].is_alive():
                    kb = InlineKeyboardBuilder()
                    kb.button(text="Остановить", callback_data=f"stop_clone_{bot_id}")
                    kb.button(text="Назад", callback_data="manage_clones")
                    kb.adjust(2)
                    
                    await callback.message.edit_text(
                        f"⚠️ Бот @{bot_info.username} уже запущен!",
                        reply_markup=kb.as_markup()
                    )
                    await callback.answer()
                    return
            
            # Create a new process for the bot
            process = Process(
                target=run_bot_process,
                args=(new_token, self.config.owner_id, self.config.source_channels, bot_id),
                name=bot_id
            )
            
            process.start()
            self.bot_manager.add_bot(bot_id, process)
            self.child_bots.append(bot_id)
            
            kb = InlineKeyboardBuilder()
            kb.button(text="Управление клонами", callback_data="manage_clones")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(2)
            
            await callback.message.edit_text(
                f"✅ Бот @{bot_info.username} успешно запущен!\n\n"
                f"ID процесса: {process.pid}\n"
                f"Статус: Работает\n\n"
                "Бот работает в отдельном процессе и будет пересылать сообщения.",
                reply_markup=kb.as_markup()
            )
            
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад", callback_data="back_to_main")
            
            await callback.message.edit_text(
                f"❌ Ошибка при запуске клона: {e}",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to start clone bot: {e}")
        
        await callback.answer()

    async def manage_clones(self, callback: types.CallbackQuery):
        """Manage running bot clones"""
        if not self.is_admin(callback.from_user.id): 
            return
        
        bots = self.bot_manager.list_bots()
        
        # Count clones (excluding main bot)
        clone_count = len([b for b in bots if b != "main"])
        
        if clone_count == 0:
            kb = InlineKeyboardBuilder()
            kb.button(text="Добавить клон", callback_data="clone_bot")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(2)
            
            await callback.message.edit_text(
                "📋 Нет запущенных клонов.\n\n"
                "Добавьте новый клон для управления несколькими ботами.",
                reply_markup=kb.as_markup()
            )
        else:
            text = "🤖 Запущенные боты:\n\n"
            kb = InlineKeyboardBuilder()
            
            # Show main bot info first
            main_info = bots.get("main", {})
            text += f"• Основной бот\n  Статус: 🟢 Работает\n  PID: {main_info.get('pid', 'N/A')}\n\n"
            
            # Show clones
            for bot_id, info in bots.items():
                if bot_id == "main":
                    continue
                    
                # Check if process is alive
                process = self.bot_manager.processes.get(bot_id)
                if process and process.is_alive():
                    status = "🟢 Работает"
                else:
                    status = "🔴 Остановлен"
                
                # Extract bot username from bot_id
                bot_username = bot_id.replace("bot_", "@")
                text += f"• {bot_username}\n  Статус: {status}\n  PID: {info.get('pid', 'N/A')}\n  Запущен: {info.get('started_at', 'Неизвестно')}\n\n"
                
                if status == "🟢 Работает":
                    kb.button(text=f"Остановить {bot_username}", callback_data=f"stop_clone_{bot_id}")
                else:
                    kb.button(text=f"Запустить {bot_username}", callback_data=f"start_clone_{bot_id}")
            
            kb.button(text="Добавить клон", callback_data="clone_bot")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(1)
            
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        
        await callback.answer()

    async def stop_clone(self, callback: types.CallbackQuery):
        """Stop a running bot clone"""
        if not self.is_admin(callback.from_user.id):
            return
        
        bot_id = callback.data.replace("stop_clone_", "")
        
        try:
            self.bot_manager.remove_bot(bot_id)
            await callback.answer(f"Бот {bot_id} остановлен")
        except Exception as e:
            await callback.answer(f"Ошибка при остановке: {e}")
        
        await self.manage_clones(callback)

    # Update the clone_bot_submit method to provide inline option
    async def clone_bot_submit(self, message: types.Message):
        """Handler for new bot token submission"""
        if not self.is_admin(message.from_user.id): 
            return
        
        if not hasattr(self, 'awaiting_clone_token') or self.awaiting_clone_token != message.from_user.id:
            return
        
        new_token = message.text.strip()
        
        if not new_token or ':' not in new_token:
            await message.reply("⚠️ Неверный формат токена.")
            return
        
        self.awaiting_clone_token = None
        
        # Verify the token
        try:
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            kb = InlineKeyboardBuilder()
            kb.button(text="🚀 Запустить сейчас", callback_data=f"clone_inline_{new_token}")
            kb.button(text="💾 Создать файлы", callback_data=f"clone_files_{new_token}")
            kb.button(text="Отмена", callback_data="back_to_main")
            kb.adjust(2)
            
            await message.reply(
                f"✅ Токен проверен!\n"
                f"Бот: @{bot_info.username}\n\n"
                "Выберите действие:",
                reply_markup=kb.as_markup()
            )
            
        except Exception as e:
            await message.reply(f"❌ Ошибка проверки токена: {e}")

    # Add cleanup method to stop all child bots on shutdown
    async def cleanup(self):
        """Stop all child bots"""
        for bot_id in self.child_bots:
            try:
                self.bot_manager.remove_bot(bot_id)
            except Exception as e:
                logger.error(f"Error stopping bot {bot_id}: {e}")

    # Let's also add the overwrite_clone method that was referenced earlier
    async def overwrite_clone(self, callback: types.CallbackQuery):
        """Handler for overwriting existing clone"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Parse data: overwrite_clone_dirname_token
        parts = callback.data.split('_', 3)
        if len(parts) != 4:
            await callback.answer("Ошибка в данных")
            return
        
        clone_dir = parts[2]
        new_token = parts[3]
        
        # Delete existing clone
        current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        clone_path = os.path.join(os.path.dirname(current_dir), clone_dir)
        
        if os.path.exists(clone_path):
            shutil.rmtree(clone_path)
        
        # Perform clone
        await self._perform_bot_clone(new_token, clone_dir, callback.message)
        await callback.answer()
    def _setup_handlers(self):
        """Initialize message handlers with Command pattern"""
        # Admin command handlers
        commands = {
            "start": StartCommand(
                isinstance(self.context.state, RunningState)
            ),
            "help": HelpCommand(),
            "setlast": SetLastMessageCommand(
                self.bot
            ),
            "getlast": GetLastMessageCommand(),
            "forwardnow": ForwardNowCommand(
                self.context
            ),
            "test": TestMessageCommand(
                self.bot
            ),
            "findlast": FindLastMessageCommand(
                self.bot
            )
        }
        
        for cmd_name, cmd_handler in commands.items():
            self.dp.message.register(cmd_handler.execute, Command(cmd_name))
    
        self.dp.message.register(
            self.add_channel_submit,
            lambda message: message.from_user.id == self.awaiting_channel_input
        )
        self.dp.message.register(
            self.clone_bot_submit,
            lambda message: hasattr(self, 'awaiting_clone_token') and 
                          self.awaiting_clone_token == message.from_user.id
        )
        # Register the direct add channel command
        self.dp.message.register(
            self.add_channel_prompt,
            Command("addchannel")
        )        
        # Channel post handler
        self.dp.channel_post.register(self.handle_channel_post)
        
        # Callback query handlers
        callbacks = {
            "toggle_forward": self.toggle_forwarding,
            "toggle_auto_forward": self.toggle_auto_forward,
            "add_channel_input": self.add_channel_input,
            "interval_": self.set_interval,
            "interval_between_": self.set_interval,
            "set_interval_": self.set_interval,
            "clone_bot": self.clone_bot_prompt,
            "overwrite_clone_": self.overwrite_clone,
            "remove_channel_menu": self.remove_channel_menu,
            "remove_channel_page_": self.remove_channel_menu,  # Для пагинации удаления
            "remove_channel_": self.remove_channel,  # Прямое удаление без подтверждения
            "channel_intervals_page_": self.manage_channel_intervals,  # Для пагинации интервалов
            "remove_": self.remove_chat,  # Для удаления чатов (не каналов)
            "stats": self.show_stats,
            "list_chats": self.list_chats,
            "back_to_main": self.main_menu,
            "clone_inline_": self.clone_bot_inline,
            "manage_clones": self.manage_clones,
            "stop_clone_": self.stop_clone,
            "reorder_channels": self.reorder_channels,
            "move_": self.move_channel,  # общий префикс для move_up_/move_down_
            "clone_files_": self.create_clone_files,
            "channels": self.manage_channels,
            "add_channel": self.add_channel_prompt,
            "channel_intervals": self.manage_channel_intervals,
        }
        
        # Register handlers with specific order to avoid conflicts
        for prefix, handler in callbacks.items():
            self.dp.callback_query.register(
                handler,
                lambda c, p=prefix: c.data.startswith(p)
            )
        
        # Handler for bot being added to chats
        self.dp.my_chat_member.register(self.handle_chat_member)
        
    async def reorder_channels(self, callback: types.CallbackQuery):
        """Переход в режим сортировки каналов"""
        if not self.is_admin(callback.from_user.id):
            return

        channels = self.config.source_channels
        kb = InlineKeyboardBuilder()
        # Для каждого канала показываем ↑ и ↓
        for i, ch in enumerate(channels):
            # Получаем читаемое название
            try:
                title = (await self.bot.get_chat(ch)).title or ch
            except:
                title = ch
            kb.button(
                text=f"↑ {title}", 
                callback_data=f"move_up_{ch}"
            )
            kb.button(
                text=f"↓ {title}", 
                callback_data=f"move_down_{ch}"
            )
        # Кнопки подтвердить или отменить
        kb.button(text="Готово",    callback_data="channels")
        kb.button(text="Отменить",  callback_data="channels")
        kb.adjust(2)
        await callback.message.edit_text(
            "Измените порядок каналов, перемещая их вверх/вниз:",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def move_channel(self, callback: types.CallbackQuery):
        """Обработчик кнопок ↑ и ↓: меняет порядок каналов"""
        if not self.is_admin(callback.from_user.id):
            return

        data = callback.data  # e.g. "move_up_-1001234567890"
        parts = data.split("_", 2)
        direction, channel = parts[1], parts[2]
        lst = self.config.source_channels

        try:
            idx = lst.index(channel)
        except ValueError:
            await callback.answer("Канал не найден")
            return

        if direction == "up" and idx > 0:
            lst[idx], lst[idx-1] = lst[idx-1], lst[idx]
        elif direction == "down" and idx < len(lst)-1:
            lst[idx], lst[idx+1] = lst[idx+1], lst[idx]
        else:
            await callback.answer("Двигается за пределы списка")
            return

        # Сохраняем новый порядок
        self.config._save_channels_to_config()

        # Обновляем интерфейс сортировки
        await self.reorder_channels(callback)

    async def find_latest_message(self, channel_id: str) -> Optional[int]:
        """Helper method to find the latest valid message ID in a channel"""
        try:
            # Try to access more recent messages first
            max_id = 10000  # Start with a reasonably high number
            
            for test_id in range(max_id, 0, -1):
                try:
                    # Try to get message info
                    msg = await self.bot.get_messages(chat_id=channel_id, message_ids=test_id)
                    if msg and not msg.empty:
                        return test_id
                except Exception:
                    # Skip errors for non-existent messages
                    pass
                    
                # Don't make too many attempts to avoid rate limits
                if test_id % 1000 == 0:
                    await asyncio.sleep(0.5)
                    
                # Don't check too many IDs
                if max_id - test_id > 5000:
                    break
                    
            return None
        except Exception as e:
            logger.error(f"Error finding latest message in channel {channel_id}: {e}")
            return None
            
    async def find_last_message_handler(self, callback: types.CallbackQuery):
        """Handler for finding last message button"""
        if not self.is_admin(callback.from_user.id):
            return
        
        channel_id = callback.data.replace("findlast_", "")
        
        await callback.message.edit_text(
            f"🔍 Ищу последнее сообщение в канале {channel_id}...",
            reply_markup=None
        )
        
        try:
            latest_id = await self.find_latest_message(channel_id)
            
            if latest_id:
                await Repository.save_last_message(str(channel_id), latest_id)
                
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await callback.message.edit_text(
                    f"✅ Найдено и сохранено последнее сообщение (ID: {latest_id}) в канале {channel_id}",
                    reply_markup=kb.as_markup()
                )
            else:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await callback.message.edit_text(
                    f"⚠️ Не удалось найти валидные сообщения в канале {channel_id}.",
                    reply_markup=kb.as_markup()
                )
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад к каналам", callback_data="channels")
            
            await callback.message.edit_text(
                f"❌ Ошибка при поиске последнего сообщения: {e}",
                reply_markup=kb.as_markup()
            )
        
        await callback.answer()
        
    async def add_channel_prompt(self, callback: types.CallbackQuery):
        """Improved prompt to add a channel"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Create a keyboard with buttons for common channel types
        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Enter Channel ID/Username", callback_data="add_channel_input")
        kb.button(text="Back", callback_data="channels")
        kb.adjust(1)
        
        await callback.message.edit_text(
            "Please select an option to add a channel:\n\n"
            "• You can enter the channel ID (starts with -100...)\n"
            "• Or the channel username (without @)\n\n"
            "The bot must be an administrator in the channel.",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_channel_input(self, callback: types.CallbackQuery):
        """Handler for channel ID/username input"""
        if not self.is_admin(callback.from_user.id):
            return
        
        self.awaiting_channel_input = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="channels")
        
        await callback.message.edit_text(
            "Пожалуйста, введите ID канала или username для добавления:\n\n"
            "• Для публичных каналов: введите username без @\n"
            "• Для приватных каналов: введите ID канала (начинается с -100...)\n\n"
            "Отправьте ID/username сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_channel_submit(self, message: types.Message):
        """Handler for direct channel input message"""
        if not self.is_admin(message.from_user.id):
            return
        
        channel = message.text.strip()
        
        if not channel:
            await message.reply("⚠️ ID/username канала не может быть пустым")
            return
        
        self.awaiting_channel_input = None
        
        progress_msg = await message.reply("🔄 Проверяю доступ к каналу...")
        
        try:
            chat = await self.bot.get_chat(channel)
            
            bot_id = (await self.bot.get_me()).id
            member = await self.bot.get_chat_member(chat.id, bot_id)
            
            if member.status != "administrator":
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await progress_msg.edit_text(
                    "⚠️ Бот должен быть администратором канала.\n"
                    "Пожалуйста, добавьте бота как администратора и попробуйте снова.",
                    reply_markup=kb.as_markup()
                )
                return
            
            if self.config.add_source_channel(str(chat.id)):
                await progress_msg.edit_text(f"✅ Добавлен канал: {chat.title} ({chat.id})\n\n🔍 Теперь ищу последнее сообщение...")
                
                try:
                    latest_id = await self.find_latest_message(str(chat.id))
                    
                    if latest_id:
                        await Repository.save_last_message(str(chat.id), latest_id)
                        
                        kb = InlineKeyboardBuilder()
                        kb.button(text="Назад к каналам", callback_data="channels")
                        
                        await progress_msg.edit_text(
                            f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                            f"✅ Найдено и сохранено последнее сообщение (ID: {latest_id})",
                            reply_markup=kb.as_markup()
                        )
                    else:
                        kb = InlineKeyboardBuilder()
                        kb.button(text="Назад к каналам", callback_data="channels")
                        
                        await progress_msg.edit_text(
                            f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                            f"⚠️ Не удалось найти валидные сообщения. Будет использоваться следующее сообщение в канале.",
                            reply_markup=kb.as_markup()
                        )
                except Exception as e:
                    logger.error(f"Error finding latest message: {e}")
                    
                    kb = InlineKeyboardBuilder()
                    kb.button(text="Назад к каналам", callback_data="channels")
                    
                    await progress_msg.edit_text(
                        f"✅ Добавлен канал: {chat.title} ({chat.id})\n"
                        f"⚠️ Ошибка при поиске последнего сообщения.",
                        reply_markup=kb.as_markup()
                    )
            else:
                kb = InlineKeyboardBuilder()
                kb.button(text="Назад к каналам", callback_data="channels")
                
                await progress_msg.edit_text(
                    f"⚠️ Канал {chat.title} уже настроен.",
                    reply_markup=kb.as_markup()
                )
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="Назад к каналам", callback_data="channels")
            
            await progress_msg.edit_text(
                f"❌ Ошибка доступа к каналу: {e}\n\n"
                "Убедитесь что:\n"
                "• ID/username канала указан правильно\n"
                "• Бот является участником канала\n"
                "• Бот является администратором канала",
                reply_markup=kb.as_markup()
            )
            logger.error(f"Failed to add channel {channel}: {e}")

    async def toggle_auto_forward(self, callback: types.CallbackQuery):
        """Handler for auto-forward toggle button"""
        if not self.is_admin(callback.from_user.id):
            return

        if isinstance(self.context.state, RunningState):
            await self.context.state.toggle_auto_forward()
            await callback.message.edit_text(
                "Main Menu:",
                reply_markup=KeyboardFactory.create_main_keyboard(
                    True, 
                    self.context.state.auto_forward
                )
            )
        else:
            await callback.answer("Start forwarding first to enable auto-forward")
        
        await callback.answer()
    
    async def toggle_forwarding(self, callback: types.CallbackQuery):
        """Handler for forwarding toggle button"""
        if not self.is_admin(callback.from_user.id):
            return

        if isinstance(self.context.state, IdleState):
            await self.context.start()
        else:
            await self.context.stop()

        await callback.message.edit_text(
            f"Пересылка {'начата' if isinstance(self.context.state, RunningState) else 'остановлена'}!",
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
                isinstance(self.context.state, RunningState) and self.context.state.auto_forward
            )
        )
        await callback.answer()
    async def remove_channel_menu(self, callback: types.CallbackQuery):
        """Show channel removal menu with pagination"""
        if not self.is_admin(callback.from_user.id):
            return
        
        source_channels = self.config.source_channels
        page = 0
        
        # Определяем страницу из callback_data
        if callback.data.startswith("remove_channel_page_"):
            try:
                page = int(callback.data.split("_")[-1])
            except (ValueError, IndexError):
                page = 0
        
        if not source_channels:
            await callback.message.edit_text(
                "❌ Нет каналов для удаления.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 К каналам", callback_data="channels"
                ).as_markup()
            )
            await callback.answer()
            return
        
        # Создаем информационный текст
        text = f"❌ Удаление каналов\n\nВсего каналов: {len(source_channels)}\n\n"
        text += "Выберите канал для удаления:"
        
        # Получаем информацию о каналах для создания клавиатуры
        channel_info = {}
        for channel in source_channels:
            try:
                chat = await self.bot.get_chat(channel)
                channel_info[channel] = chat.title or channel
            except Exception:
                channel_info[channel] = channel
        
        await callback.message.edit_text(
            text,
            reply_markup=KeyboardFactory.create_channel_removal_keyboard(source_channels, page, channel_info)
        )
        await callback.answer()
    async def manage_channel_intervals(self, callback: types.CallbackQuery):
        """Manager for channel intervals with pagination"""
        if not self.is_admin(callback.from_user.id):
            return
            
        source_channels = self.config.source_channels
        
        if len(source_channels) < 2:
            await callback.message.edit_text(
                "Вам нужно минимум 2 канала для установки интервалов между ними.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 К каналам", callback_data="channels"
                ).as_markup()
            )
            await callback.answer()
            return
        
        # Определяем страницу из callback_data
        page = 0
        if callback.data.startswith("channel_intervals_page_"):
            try:
                page = int(callback.data.split("_")[-1])
            except (ValueError, IndexError):
                page = 0
        
        # Получаем текущие интервалы из базы данных
        current_intervals = await Repository.get_channel_intervals()
        
        # Получаем информацию о каналах (названия)
        channel_info = {}
        for channel in source_channels:
            try:
                chat = await self.bot.get_chat(channel)
                channel_info[channel] = chat.title or channel
            except Exception:
                channel_info[channel] = channel
        
        # Создаем текст с информацией о текущих интервалах
        text = "⏱️ Интервалы между каналами:\n\n(Если интервал не установлен используется глобальный интервал)\n\n"
        
        channel_pairs = []
        for i, channel in enumerate(source_channels):
            if i < len(source_channels) - 1:
                next_channel = source_channels[i + 1]
                channel_pairs.append((channel, next_channel))
                
                # Получаем название каналов для отображения
                name1 = channel_info.get(channel, channel)
                name2 = channel_info.get(next_channel, next_channel)
                
                # Сокращаем названия для красивого отображения
                display_name1 = name1[:20] + "..." if len(name1) > 20 else name1
                display_name2 = name2[:20] + "..." if len(name2) > 20 else name2
                
                # Проверяем установленный интервал
                interval_data = current_intervals.get(channel, {})
                if interval_data.get("next_channel") == next_channel:
                    interval_seconds = interval_data.get("interval", 300)
                    if interval_seconds >= 3600:
                        interval_str = f"{interval_seconds//3600}ч"
                    else:
                        interval_str = f"{interval_seconds//60}м"
                    text += f"• {display_name1} → {display_name2}: {interval_str}\n"
                else:
                    text += f"• {display_name1} → {display_name2}: не установлен\n"
        
        text += f"\nВыберите пару каналов для настройки:"
        
        await callback.message.edit_text(
            text,
            reply_markup=KeyboardFactory.create_channel_interval_keyboard(
                source_channels, page, channel_info, current_intervals
            )
        )
        await callback.answer()

    async def set_channel_interval_prompt(self, callback: types.CallbackQuery):
        """Prompt for setting interval between channels"""
        if not self.is_admin(callback.from_user.id):
            return
            
        # Parse the channel IDs from callback data
        parts = callback.data.split('_')
        if len(parts) >= 4:
            channel1 = parts[2]
            channel2 = parts[3]
            
            # Get channel names for display
            try:
                chat1 = await self.bot.get_chat(channel1)
                chat2 = await self.bot.get_chat(channel2)
                name1 = chat1.title or channel1
                name2 = chat2.title or channel2
            except Exception:
                name1 = channel1
                name2 = channel2
            
            await callback.message.edit_text(
                f"Set interval between forwarding from:\n"
                f"{name1} → {name2}",
                reply_markup=KeyboardFactory.create_channel_interval_options(channel1, channel2)
            )
            await callback.answer()
        else:
            await callback.answer("Invalid channel selection")

    async def set_channel_interval(self, callback: types.CallbackQuery):
        """Set interval between two channels"""
        if not self.is_admin(callback.from_user.id):
            return
            
        # Parse data: set_interval_channel1_channel2_seconds
        parts = callback.data.split('_')
        if len(parts) >= 5:
            channel1 = parts[2]
            channel2 = parts[3]
            interval = int(parts[4])
            
            # Save the interval
            await Repository.set_channel_interval(channel1, channel2, interval)
            
            # Format interval for display
            display = f"{interval//3600}h" if interval >= 3600 else f"{interval//60}m"
            
            # Get channel names for display
            try:
                chat1 = await self.bot.get_chat(channel1)
                chat2 = await self.bot.get_chat(channel2)
                name1 = chat1.title or channel1
                name2 = chat2.title or channel2
            except Exception:
                name1 = channel1
                name2 = channel2
            
            await callback.message.edit_text(
                f"✅ Interval set to {display} between:\n"
                f"{name1} → {name2}",
                reply_markup=InlineKeyboardBuilder().button(
                    text="Back to Intervals", callback_data="channel_intervals"
                ).as_markup()
            )
            await callback.answer()
        else:
            await callback.answer("Invalid interval selection")

    async def set_interval(self, callback: types.CallbackQuery):
        """Handler for interval setting"""
        if not self.is_admin(callback.from_user.id):
            return

        data = callback.data
        
        if "interval_between_" in data:
            channel_parts = data.split('_')
            if len(channel_parts) >= 4:
                channel1 = channel_parts[2]
                channel2 = channel_parts[3]
                
                try:
                    chat1 = await self.bot.get_chat(channel1)
                    chat2 = await self.bot.get_chat(channel2)
                    name1 = chat1.title or channel1
                    name2 = chat2.title or channel2
                except Exception:
                    name1 = channel1
                    name2 = channel2
                
                await callback.message.edit_text(
                    f"Установите интервал между пересылкой из:\n"
                    f"{name1} → {name2}",
                    reply_markup=KeyboardFactory.create_channel_interval_options(channel1, channel2)
                )
                await callback.answer()
            else:
                await callback.answer("Неверный выбор канала")
        elif "set_interval_" in data:
            parts = data.split('_')
            if len(parts) >= 5:
                channel1 = parts[2]
                channel2 = parts[3]
                interval = int(parts[4])
                
                await Repository.set_channel_interval(channel1, channel2, interval)
                
                display = f"{interval//3600}ч" if interval >= 3600 else f"{interval//60}м"
                
                try:
                    chat1 = await self.bot.get_chat(channel1)
                    chat2 = await self.bot.get_chat(channel2)
                    name1 = chat1.title or channel1
                    name2 = chat2.title or channel2
                except Exception:
                    name1 = channel1
                    name2 = channel2
                
                await callback.message.edit_text(
                    f"✅ Интервал установлен на {display} между:\n"
                    f"{name1} → {name2}",
                    reply_markup=InlineKeyboardBuilder().button(
                        text="Назад к интервалам", callback_data="channel_intervals"
                    ).as_markup()
                )
                await callback.answer()
            else:
                await callback.answer("Неверный выбор интервала")
        
        # Regular global interval setting
        if data == "interval_menu":
            # Получаем текущий интервал
            current_interval = await Repository.get_config("repost_interval", "3600")
            try:
                current_seconds = int(current_interval)
                
                # Форматируем текущий интервал для отображения
                if current_seconds >= 3600:
                    current_display = f"{current_seconds // 3600}ч"
                else:
                    current_display = f"{current_seconds // 60}м"
            except (ValueError, TypeError):
                current_display = "60м"  # По умолчанию
                
            await callback.message.edit_text(
                f"Текущий интервал: {current_display}\n\n"
                "Выберите новый интервал повторной отправки:",
                reply_markup=await KeyboardFactory.create_interval_keyboard()  # <-- Добавлен await здесь
            )
        elif data.startswith("interval_") and not "between" in data and not "menu" in data:
            try:
                interval = int(data.split("_")[1])
                
                await Repository.set_config("repost_interval", str(interval))
                
                if isinstance(self.context.state, RunningState):
                    self.context.state.interval = interval
                    
                    now = datetime.now().timestamp()
                    for channel in self.context.config.source_channels:
                        self.context.state._channel_last_post[channel] = now
                    
                    self.context.state._last_global_post_time = now
                    
                    display = f"{interval//3600}ч" if interval >= 3600 else f"{interval//60}м"
                    await callback.message.edit_text(
                        f"Интервал установлен на {display}. Первая отправка произойдет через этот интервал.",
                        reply_markup=KeyboardFactory.create_main_keyboard(
                            True, 
                            self.context.state.auto_forward
                        )
                    )
                    
                    logger.info(f"Установлен интервал пересылки {interval} секунд ({interval//60} минут)")
                else:
                    display = f"{interval//3600}ч" if interval >= 3600 else f"{interval//60}м"
                    await callback.message.edit_text(
                        f"Интервал установлен на {display}",
                        reply_markup=KeyboardFactory.create_main_keyboard(
                            False, 
                            False
                        )
                    )
            except Exception as e:
                logger.error(f"Ошибка установки интервала: {e}")
                await callback.answer("Ошибка установки интервала")

    async def remove_chat(self, callback: types.CallbackQuery):
        """Handler for chat removal"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Check if this is for removing a chat, not a channel
        if not callback.data.startswith("remove_") or callback.data.startswith("remove_channel_"):
            await callback.answer("Эта команда только для удаления чатов")
            return
        
        try:
            chat_id = int(callback.data.split("_")[1])
            await Repository.remove_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self.list_chats(callback)
            await callback.answer("Чат удален!")
        except ValueError:
            await callback.answer("Ошибка при удалении чата")
            logger.error(f"Invalid chat_id in callback data: {callback.data}")

    async def show_stats(self, callback: types.CallbackQuery):
        """Handler for statistics display"""
        if not self.is_admin(callback.from_user.id):
            return
        
        stats = await Repository.get_stats()
        text = (
            "📊 Статистика пересылки\n\n"
            f"Всего пересылок: {stats['total_forwards']}\n"
            f"Последняя пересылка: {stats['last_forward'] or 'Никогда'}\n\n"
            "Последние сохраненные сообщения:\n"
        )
        
        if stats["last_messages"]:
            text += "\n".join(
                f"Канал: {channel_id}\n"
                f"ID сообщения: {data['message_id']}\n"
                f"Время: {data['timestamp']}"
                for channel_id, data in stats["last_messages"].items()
            )
        else:
            text += "Нет"
        
        await callback.message.edit_text(
            text,
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
                isinstance(self.context.state, RunningState) and self.context.state.auto_forward
            )
        )
        await callback.answer()

    async def list_chats(self, callback: types.CallbackQuery):
        """Handler for chat listing"""
        if not self.is_admin(callback.from_user.id):
            return
        
        chats = await Repository.get_target_chats()
        chat_info = {}
        
        for chat_id in chats:
            info = await self.cache_service.get_chat_info(self.bot, chat_id)
            if info:
                chat_info[chat_id] = info.title
        
        if not chats:
            text = (
                "Нет настроенных целевых чатов.\n"
                "Убедитесь, что:\n"
                "1. Бот добавлен в целевые чаты\n"
                "2. Бот является администратором в исходных каналах"
            )
            markup = KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
                isinstance(self.context.state, RunningState) and self.context.state.auto_forward
            )
        else:
            text = "📡 Целевые чаты:\n\n"
            for chat_id, title in chat_info.items():
                text += f"• {title} ({chat_id})\n"
            markup = KeyboardFactory.create_chat_list_keyboard(chat_info)
        
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()

    async def main_menu(self, callback: types.CallbackQuery):
        """Handler for main menu button"""
        if not self.is_admin(callback.from_user.id):
            return
        
        await callback.message.edit_text(
            "Main Menu:",
            reply_markup=KeyboardFactory.create_main_keyboard(
                isinstance(self.context.state, RunningState),
                isinstance(self.context.state, RunningState) and self.context.state.auto_forward
            )
        )
        await callback.answer()

    async def manage_channels(self, callback: types.CallbackQuery):
        """Channel management menu"""
        if not self.is_admin(callback.from_user.id):
            return
                
        # Reset any channel input state
        self.awaiting_channel_input = None
        
        source_channels = self.config.source_channels
        
        if not source_channels:
            text = (
                "Нет настроенных исходных каналов.\n"
                "Добавьте канал, нажав кнопку ниже."
            )
        else:
            text = "📡 Исходные каналы:\n\n"
            for channel in source_channels:
                # Try to get chat info for better display
                try:
                    chat = await self.bot.get_chat(channel)
                    if chat.title:
                        text += f"• {chat.title} ({channel})\n"
                    else:
                        text += f"• {channel}\n"
                except Exception:
                    text += f"• {channel}\n"
        
        # Use KeyboardFactory to create management keyboard
        markup = KeyboardFactory.create_channel_management_keyboard(source_channels)
        
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()

    async def add_channel_prompt(self, callback: types.CallbackQuery):
        """Improved prompt to add a channel without command"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Set state to wait for channel input
        self.awaiting_channel_input = callback.from_user.id
        
        # Create a keyboard with cancel button
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="channels")
        
        await callback.message.edit_text(
            "Введите ID канала или его username для добавления:\n\n"
            "• Для публичных каналов: введите username без @\n"
            "• Для приватных каналов: введите ID канала (начинается с -100...)\n\n"
            "Просто отправьте ID канала или username сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def add_channel_handler(self, message: types.Message):
        """Handle channel addition from user input"""
        logger.info(f"Received channel addition message: {message.text}")
        channel = message.text.strip()
        
        if not channel:
            await message.reply("⚠️ Channel ID/username cannot be empty")
            return
            
        # Verify that bot can access the channel
        try:
            # Try to get basic info about the channel
            chat = await self.bot.get_chat(channel)
            
            # Check if bot is an admin in the channel
            bot_id = (await self.bot.get_me()).id
            member = await self.bot.get_chat_member(chat.id, bot_id)
            
            if member.status != "administrator":
                await message.reply(
                    "⚠️ Bot must be an administrator in the channel to forward messages.\n"
                    "Please add the bot as admin and try again."
                )
                return
                
            # Add channel to configuration
            if self.config.add_source_channel(str(chat.id)):
                await message.reply(
                    f"✅ Successfully added channel: {chat.title} ({chat.id})"
                )
                logger.info(f"Added channel: {chat.title} ({chat.id})")
            else:
                await message.reply("⚠️ This channel is already configured")
                
        except Exception as e:
            await message.reply(
                f"❌ Error accessing channel: {e}\n\n"
                "Make sure:\n"
                "• The channel ID/username is correct\n"
                "• The bot is a member of the channel\n"
                "• The bot is an administrator in the channel"
            )
            logger.error(f"Failed to add channel {channel}: {e}")

    async def remove_channel(self, callback: types.CallbackQuery):
        """Remove a source channel directly without confirmation"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Извлекаем ID канала из callback_data
        if not callback.data.startswith("remove_channel_"):
            await callback.answer("Неверный формат данных")
            return
        
        channel = callback.data.replace("remove_channel_", "")
        
        # Получаем название канала для уведомления
        try:
            chat = await self.bot.get_chat(channel)
            channel_name = chat.title or channel
        except Exception:
            channel_name = channel
        
        # Сокращаем название для отображения в уведомлении
        display_name = channel_name[:25] + "..." if len(channel_name) > 25 else channel_name
        
        # Удаляем канал
        if self.config.remove_source_channel(channel):
            # Также удаляем связанные интервалы
            try:
                await Repository.delete_channel_interval(channel)
            except Exception as e:
                logger.warning(f"Не удалось удалить интервалы для канала {channel}: {e}")
            
            await callback.answer(f"✅ Канал '{display_name}' удален")
            
            # Возвращаемся к меню удаления каналов, чтобы показать обновленный список
            await self.remove_channel_menu(callback)
        else:
            await callback.answer("❌ Не удалось удалить канал")
    
    async def handle_channel_post(self, message: types.Message | None):
        """Обработчик сообщений из канала с учетом ожидания интервала"""
        if message is None:
            return
                
        chat_id = str(message.chat.id)
        username = message.chat.username
        source_channels = self.config.source_channels
                
        is_source = False
        for channel in source_channels:
            if channel == chat_id or (username and channel.lower() == username.lower()):
                is_source = True
                break
                    
        if not is_source:
            logger.info(f"Сообщение не из канала-источника: {chat_id}/{username}")
            return
        
        # Сохраняем последний ID сообщения для канала
        await Repository.save_last_message(chat_id, message.message_id)
        
        if isinstance(self.context.state, RunningState):
            # Проверка, находимся ли мы в периоде ожидания для этого канала
            now = datetime.now().timestamp()
            last_post_time = self.context.state._channel_last_post.get(chat_id, 0)
            waiting_interval = (now - last_post_time) < self.context.state.interval
            
            # Также проверяем специальные интервалы между каналами
            is_next_in_sequence = False
            if self.context.state._last_processed_channel:
                channel_intervals = await Repository.get_channel_intervals()
                if self.context.state._last_processed_channel in channel_intervals:
                    interval_data = channel_intervals.get(self.context.state._last_processed_channel, {})
                    if interval_data.get("next_channel") == chat_id:
                        special_interval = interval_data.get("interval", 0)
                        waiting_special = (now - self.context.state._last_global_post_time) < special_interval
                        if not waiting_special:
                            is_next_in_sequence = True
            
            # Если канал ожидает интервала или не является следующим в последовательности,
            # просто сохраняем сообщение для будущей обработки
            if waiting_interval and not is_next_in_sequence:
                logger.info(f"Получено новое сообщение {message.message_id} из канала {chat_id} в период ожидания. "
                            f"Сообщение будет обработано при следующей пересылке.")
                
                # Добавляем информацию в структуру ожидающих сообщений
                if chat_id not in self.context.state._pending_messages:
                    self.context.state._pending_messages[chat_id] = []
                
                # Добавляем ID сообщения, если его еще нет в списке
                if message.message_id not in self.context.state._pending_messages[chat_id]:
                    self.context.state._pending_messages[chat_id].append(message.message_id)
                
                return
                
            # Проверяем, включена ли автопересылка
            if not self.context.state.auto_forward:
                logger.info(f"Получено новое сообщение из канала {chat_id}, но автопересылка отключена. Сообщение сохранено.")
                return
                
            # Проводим стандартную обработку, если не в периоде ожидания
            logger.info(f"Параллельная проверка и последовательная пересылка сообщений из канала {chat_id}")
            
            # Определяем диапазон ID сообщений для пересылки
            max_id = message.message_id
            start_id = max(1, max_id - 10)  # Берем только последние 10 сообщений
            
            # Создаем список ID сообщений в порядке от старых к новым
            message_ids = list(range(start_id, max_id + 1))
            
            # Инициализируем временный кэш недоступных сообщений
            if not hasattr(self.context, '_temp_unavailable_messages'):
                self.context._temp_unavailable_messages = {}
            
            # Очищаем устаревшие записи (старше 30 минут)
            current_time = datetime.now().timestamp()
            self.context._temp_unavailable_messages = {k: v for k, v in self.context._temp_unavailable_messages.items() 
                                                if current_time - v < 1800}  # 30 минут
            
            # 1. Сначала параллельно проверяем все сообщения
            check_tasks = []
            for msg_id in message_ids:
                msg_key = f"{chat_id}:{msg_id}"
                if msg_key in self.context._temp_unavailable_messages:
                    logger.debug(f"Пропуск недавно недоступного сообщения {msg_id} из канала {chat_id}")
                    continue
                
                # Создаем задачу проверки доступности сообщения
                check_tasks.append(self.context.state._check_message(chat_id, msg_id))
            
            # Запускаем все проверки параллельно
            available_messages = []
            if check_tasks:
                check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
                
                # Обрабатываем результаты проверки
                for i, result in enumerate(check_results):
                    if isinstance(result, Exception):
                        logger.error(f"Ошибка при проверке сообщения: {result}")
                    elif isinstance(result, tuple) and len(result) == 2:
                        success, info = result
                        if success:
                            # Сообщение доступно, добавляем его в список для отправки
                            available_messages.append(info)
                        elif 'error' in info and info['error'] == 'message_not_found':
                            # Сообщение недоступно, добавляем в кэш недоступных
                            msg_id = message_ids[i] if i < len(message_ids) else None
                            if msg_id is not None:
                                self.context._temp_unavailable_messages[f"{chat_id}:{msg_id}"] = current_time
            
            # 2. Теперь отправляем доступные сообщения последовательно в нужном порядке
            # Сортируем сообщения по ID (от старых к новым)
            available_messages.sort(key=lambda x: x['message_id'])
            
            # Счетчики для статистики
            forwarded_count = 0
            skipped_count = len(message_ids) - len(available_messages)
            error_count = 0
            
            # Отправляем сообщения последовательно
            for message_info in available_messages:
                channel_id = message_info['channel_id']
                msg_id = message_info['message_id']
                
                try:
                    success = await self.context.state._forward_specific_message(channel_id, msg_id)
                    if success:
                        forwarded_count += 1
                    else:
                        skipped_count += 1
                except Exception as e:
                    error_count += 1
                    logger.error(f"Ошибка при пересылке сообщения {msg_id} из канала {channel_id}: {e}")
            
            logger.info(f"Пересылка сообщений из канала {chat_id} завершена: переслано {forwarded_count}, пропущено {skipped_count}, ошибок {error_count}")
            
            # Обновляем время последней пересылки для этого канала в RunningState
            if hasattr(self.context.state, '_channel_last_post'):
                self.context.state._channel_last_post[chat_id] = datetime.now().timestamp()
        else:
            logger.info("Бот не запущен, игнорирую сообщение")

    async def handle_chat_member(self, update: types.ChatMemberUpdated):
        """Handler for bot being added/removed from chats"""
        if update.new_chat_member.user.id != self.bot.id:
            return

        chat_id = update.chat.id
        is_member = update.new_chat_member.status in ['member', 'administrator']
        
        if is_member and update.chat.type in ['group', 'supergroup']:
            await Repository.add_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self._notify_admins(f"Бот добавлен в {update.chat.type}: {update.chat.title} ({chat_id})")
            logger.info(f"Бот добавлен в {update.chat.type}: {update.chat.title} ({chat_id})")
        elif not is_member:
            await Repository.remove_target_chat(chat_id)
            self.cache_service.remove_from_cache(chat_id)
            await self._notify_admins(f"Бот удален из чата {chat_id}")
            logger.info(f"Бот удален из чата {chat_id}")

    async def _notify_owner(self, message: str):
        """Send notification to bot owner (first admin for compatibility)"""
        try:
            await self.bot.send_message(self.config.owner_id, message)
        except Exception as e:
            logger.error(f"Не удалось уведомить владельца: {e}")
            
    async def _notify_admins(self, message: str):
        """Send notification to all bot admins"""
        for admin_id in self.config.admin_ids:
            try:
                await self.bot.send_message(admin_id, message)
            except Exception as e:
                logger.error(f"Не удалось уведомить администратора {admin_id}: {e}")

    async def start(self):
        """Start the bot"""
        await Repository.init_db()
        
        # Set default interval if not set
        if not await Repository.get_config("repost_interval"):
            await Repository.set_config("repost_interval", "3600")
        
        logger.info("Бот успешно запущен!")
        try:
            # Get the last update ID to avoid duplicates
            offset = 0
            try:
                updates = await self.bot.get_updates(limit=1, timeout=1)
                if updates:
                    offset = updates[-1].update_id + 1
            except Exception as e:
                logger.warning(f"Не удалось получить начальные обновления: {e}")

            await self.dp.start_polling(self.bot, offset=offset)
        finally:
            self.cache_service.remove_observer(self)
            await self.bot.session.close()

# Update the bottom of bot.py with proper Windows multiprocessing support

# Update the main function to handle cleanup
async def main():
    """Main entry point with improved error handling"""
    lock_file = "bot.lock"
    bot = None
    
    if os.path.exists(lock_file):
        try:
            with open(lock_file, 'r') as f:
                pid = int(f.read().strip())
            
            import psutil
            if psutil.pid_exists(pid):
                logger.error(f"Another instance is running (PID: {pid})")
                return
            os.remove(lock_file)
            logger.info("Cleaned up stale lock file")
        except Exception as e:
            logger.warning(f"Error handling lock file: {e}")
            if os.path.exists(lock_file):
                os.remove(lock_file)

    try:
        with open(lock_file, 'w') as f:
            f.write(str(os.getpid()))

        bot = ForwarderBot()
        await bot.start()
    finally:
        try:
            if bot:
                await bot.cleanup()  # Stop all child bots
            await Repository.close_db()
            if os.path.exists(lock_file):
                os.remove(lock_file)
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

# Main entry point with proper Windows multiprocessing support
if __name__ == "__main__":
    # Set multiprocessing start method for Windows compatibility
    if sys.platform.startswith('win'):
        multiprocessing.set_start_method('spawn', force=True)
    else:
        multiprocessing.set_start_method('spawn', force=True)  # Use spawn for all platforms for consistency
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    except Exception as e:
        logger.error(f"Бот остановлен из-за ошибки: {e}")