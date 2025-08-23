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
        self._state_save_task = None
        self._start_state_save_task()
        # Register as cache observer
        self.cache_service.add_observer(self)
        
        self._setup_handlers()
        
    def is_admin(self, user_id: int) -> bool:
        """Check if user is an admin"""
        return self.config.is_admin(user_id)
    
    def _start_state_save_task(self):
        """Запуск задачи периодического сохранения состояния"""
        if not self._state_save_task or self._state_save_task.done():
            self._state_save_task = asyncio.create_task(self._periodic_state_save())

    async def _periodic_state_save(self):
        """Периодическое сохранение состояния клонов"""
        while True:
            try:
                await asyncio.sleep(300)  # Сохраняем каждые 5 минут
                await self.save_clone_state()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка при периодическом сохранении состояния: {e}")

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
        """Run cloned bot in the same solution with database integration"""
        if not self.is_admin(callback.from_user.id): 
            return
        
        parts = callback.data.split('_', 2)
        if len(parts) != 3:
            await callback.answer("Ошибка в данных")
            return
        
        new_token = parts[2]
        
        await callback.message.edit_text("🚀 Запускаю клон бота...")
        
        try:
            test_bot = Bot(token=new_token)
            bot_info = await test_bot.get_me()
            await test_bot.session.close()
            
            bot_id = f"bot_{bot_info.username}"
            
            # Проверяем, что клон не запущен
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
            
            # Сохраняем информацию о клоне в БД
            config_data = json.dumps({
                'source_channels': self.config.source_channels,
                'owner_id': self.config.owner_id,
                'admin_ids': self.config.admin_ids
            })
            
            await Repository.save_bot_clone(
                bot_id, new_token, bot_info.username, 'starting', 
                None, config_data, 'Запускается через интерфейс'
            )
            
            # Создаем процесс
            process = Process(
                target=run_bot_process,
                args=(new_token, self.config.owner_id, self.config.source_channels, bot_id),
                name=bot_id
            )
            
            process.start()
            self.bot_manager.add_bot(bot_id, process)
            self.child_bots.append(bot_id)
            
            # Обновляем статус в БД
            await Repository.update_clone_status(
                bot_id, 'active', process.pid, 'Запущен успешно'
            )
            
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
            # Обновляем статус как ошибка
            if 'bot_id' in locals():
                await Repository.update_clone_status(
                    bot_id, 'error', None, f'Ошибка запуска: {str(e)}'
                )
            
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
    async def check_clone_status(self, callback: types.CallbackQuery):
        """Проверка статуса восстановленного клона"""
        if not self.is_admin(callback.from_user.id):
            return
        
        bot_id = callback.data.replace("check_clone_", "")
        
        if bot_id not in self.bot_manager.bots:
            await callback.answer("Клон не найден")
            return
        
        progress_msg = await callback.message.edit_text(
            f"🔍 Проверяю статус клона {bot_id}..."
        )
        
        clone_info = self.bot_manager.bots[bot_id]
        pid = clone_info.get('pid')
        
        try:
            import psutil
            
            if pid and psutil.pid_exists(pid):
                # Процесс существует
                process = psutil.Process(pid)
                
                # Получаем информацию о процессе
                try:
                    process_info = {
                        'name': process.name(),
                        'status': process.status(),
                        'cpu_percent': process.cpu_percent(),
                        'memory_mb': process.memory_info().rss / 1024 / 1024,
                        'create_time': datetime.fromtimestamp(process.create_time()).strftime('%H:%M:%S %d.%m.%Y')
                    }
                    
                    kb = InlineKeyboardBuilder()
                    kb.button(text="🔄 Подключиться", callback_data=f"reconnect_clone_{bot_id}")
                    kb.button(text="⏹ Принудительно остановить", callback_data=f"force_stop_clone_{bot_id}")
                    kb.button(text="🔙 Назад", callback_data="manage_clones")
                    kb.adjust(2, 1)
                    
                    await progress_msg.edit_text(
                        f"🔍 Статус клона {bot_id}\n\n"
                        f"📊 Информация о процессе:\n"
                        f"• PID: {pid}\n"
                        f"• Имя: {process_info['name']}\n"
                        f"• Статус: {process_info['status']}\n"
                        f"• CPU: {process_info['cpu_percent']:.1f}%\n"
                        f"• Память: {process_info['memory_mb']:.1f} MB\n"
                        f"• Запущен: {process_info['create_time']}\n\n"
                        f"✅ Процесс активен и работает",
                        reply_markup=kb.as_markup()
                    )
                    
                except psutil.AccessDenied:
                    kb = InlineKeyboardBuilder()
                    kb.button(text="🗑 Удалить из списка", callback_data=f"remove_dead_clone_{bot_id}")
                    kb.button(text="🔙 Назад", callback_data="manage_clones")
                    kb.adjust(1)
                    
                    await progress_msg.edit_text(
                        f"⚠️ Клон {bot_id}\n\n"
                        f"PID: {pid}\n"
                        f"Процесс существует, но нет доступа к информации.\n"
                        f"Возможно, процесс запущен другим пользователем.",
                        reply_markup=kb.as_markup()
                    )
                    
            else:
                # Процесс не существует
                kb = InlineKeyboardBuilder()
                kb.button(text="🗑 Удалить из списка", callback_data=f"remove_dead_clone_{bot_id}")
                kb.button(text="🔙 Назад", callback_data="manage_clones")
                kb.adjust(1)
                
                await progress_msg.edit_text(
                    f"❌ Клон {bot_id}\n\n"
                    f"PID: {pid}\n"
                    f"Процесс не найден или завершен.\n"
                    f"Клон больше не активен.",
                    reply_markup=kb.as_markup()
                )
                
        except Exception as e:
            kb = InlineKeyboardBuilder()
            kb.button(text="🗑 Удалить из списка", callback_data=f"remove_dead_clone_{bot_id}")
            kb.button(text="🔙 Назад", callback_data="manage_clones")
            kb.adjust(1)
            
            await progress_msg.edit_text(
                f"❌ Ошибка проверки клона {bot_id}\n\n"
                f"Ошибка: {str(e)}\n"
                f"Не удалось получить информацию о процессе.",
                reply_markup=kb.as_markup()
            )
        
        await callback.answer()

    async def reconnect_clone(self, callback: types.CallbackQuery):
        """Переподключение к существующему процессу клона"""
        if not self.is_admin(callback.from_user.id):
            return
        
        bot_id = callback.data.replace("reconnect_clone_", "")
        
        try:
            # Обновляем статус клона как активный
            if bot_id in self.bot_manager.bots:
                self.bot_manager.bots[bot_id]['status'] = 'reconnected'
                self.bot_manager.bots[bot_id]['note'] = 'Переподключен к существующему процессу'
                
                # Сохраняем состояние
                await self.save_clone_state()
                
                await callback.answer("✅ Клон помечен как подключенный")
                await self.manage_clones(callback)
            else:
                await callback.answer("❌ Клон не найден")
                
        except Exception as e:
            await callback.answer(f"❌ Ошибка переподключения: {e}")

    async def force_stop_clone(self, callback: types.CallbackQuery):
        """Принудительная остановка клона по PID"""
        if not self.is_admin(callback.from_user.id):
            return
        
        bot_id = callback.data.replace("force_stop_clone_", "")
        
        if bot_id not in self.bot_manager.bots:
            await callback.answer("Клон не найден")
            return
        
        clone_info = self.bot_manager.bots[bot_id]
        pid = clone_info.get('pid')
        
        try:
            import psutil
            
            if pid and psutil.pid_exists(pid):
                process = psutil.Process(pid)
                process.terminate()
                
                # Ждем завершения
                try:
                    process.wait(timeout=5)
                except psutil.TimeoutExpired:
                    # Если не завершился, убиваем принудительно
                    process.kill()
                    process.wait(timeout=3)
                
                # Удаляем из менеджера
                del self.bot_manager.bots[bot_id]
                if bot_id in self.bot_manager.processes:
                    del self.bot_manager.processes[bot_id]
                
                await callback.answer("✅ Клон принудительно остановлен")
                await self.manage_clones(callback)
                
            else:
                await callback.answer("❌ Процесс не найден")
                
        except Exception as e:
            await callback.answer(f"❌ Ошибка остановки: {e}")

    async def remove_dead_clone(self, callback: types.CallbackQuery):
        """Удаление мертвого клона из списка"""
        if not self.is_admin(callback.from_user.id):
            return
        
        bot_id = callback.data.replace("remove_dead_clone_", "")
        
        try:
            # Удаляем из менеджера
            if bot_id in self.bot_manager.bots:
                del self.bot_manager.bots[bot_id]
            if bot_id in self.bot_manager.processes:
                del self.bot_manager.processes[bot_id]
            if bot_id in self.child_bots:
                self.child_bots.remove(bot_id)
            
            # Сохраняем состояние
            await self.save_clone_state()
            
            await callback.answer("✅ Клон удален из списка")
            await self.manage_clones(callback)
            
        except Exception as e:
            await callback.answer(f"❌ Ошибка удаления: {e}")

    # Обновите метод manage_clones для использования новой клавиатуры:
    async def manage_clones(self, callback: types.CallbackQuery):
        """Manage running bot clones with recovery support"""
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
            text = "🤖 Управление ботами:\n\n"
            kb = InlineKeyboardBuilder()
            
            # Show main bot info first
            main_info = bots.get("main", {})
            text += f"• Основной бот\n  Статус: 🟢 Работает\n  PID: {main_info.get('pid', 'N/A')}\n\n"
            
            # Show clones with enhanced status
            for bot_id, info in bots.items():
                if bot_id == "main":
                    continue
                    
                # Extract bot username from bot_id
                bot_username = bot_id.replace("bot_", "@")
                
                # Determine status and appropriate action
                if 'note' in info and 'восстановлен' in info.get('note', '').lower():
                    status = "🔄 Восстановлен после перезапуска"
                    action_text = f"Проверить {bot_username}"
                    action_callback = f"check_clone_{bot_id}"
                elif 'note' in info and 'переподключен' in info.get('note', '').lower():
                    status = "🔗 Переподключен"
                    action_text = f"Остановить {bot_username}"
                    action_callback = f"force_stop_clone_{bot_id}"
                else:
                    # Check if process is alive
                    process = self.bot_manager.processes.get(bot_id)
                    if process and process.is_alive():
                        status = "🟢 Работает"
                        action_text = f"Остановить {bot_username}"
                        action_callback = f"stop_clone_{bot_id}"
                    else:
                        status = "🔴 Остановлен"
                        action_text = f"Запустить {bot_username}"
                        action_callback = f"start_clone_{bot_id}"
                
                text += f"• {bot_username}\n  Статус: {status}\n  PID: {info.get('pid', 'N/A')}\n"
                if info.get('started_at'):
                    text += f"  Запущен: {info.get('started_at')}\n"
                if info.get('note'):
                    text += f"  Заметка: {info.get('note')}\n"
                text += "\n"
                
                kb.button(text=action_text, callback_data=action_callback)
            
            kb.button(text="Добавить клон", callback_data="clone_bot")
            kb.button(text="🔄 Обновить список", callback_data="manage_clones")
            kb.button(text="Назад", callback_data="back_to_main")
            kb.adjust(1)
            
            await callback.message.edit_text(text, reply_markup=kb.as_markup())
        
        await callback.answer()
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
        """Stop all child bots and save state"""
        # Сохраняем состояние перед остановкой
        await self.save_clone_state()
        
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
            self.add_user_as_admin_submit,
            lambda message: hasattr(self, 'awaiting_user_id') and 
                        self.awaiting_user_id == message.from_user.id
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
            "show_detailed_channels": self.show_detailed_channels,
            "test_channels": self.test_channel_forwarding,
            "add_user_admin": self.add_user_as_admin_prompt,
            "confirm_add_admin_": self.confirm_add_admin,
            "retry_promote_": self.retry_promote,
            "check_clone_": self.check_clone_status,
            "reconnect_clone_": self.reconnect_clone,
            "force_stop_clone_": self.force_stop_clone,
            "remove_dead_clone_": self.remove_dead_clone,
            "test_history": self.show_test_history,
            "admin_history": self.show_admin_history,
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
        # Синхронизируем интервалы между каналами с новым порядком
        await self.sync_intervals_with_order()

        # Обновляем интерфейс сортировки
        await self.reorder_channels(callback)

    async def sync_intervals_with_order(self):
        """Синхронизирует записи интервалов между каналами с текущим порядком каналов.

        Логика:
        - Для каждой смежной пары (ch[i] -> ch[i+1]) оставляем/обновляем запись,
          сохраняя старое значение интервала, если оно было установлено для ch[i].
        - Для последнего канала и каналов, отсутствующих в списке, удаляем записи.
        """
        try:
            channels = list(self.config.source_channels)
            if not channels:
                return

            current_intervals = await Repository.get_channel_intervals()

            # Удаляем записи для каналов, отсутствующих в списке, и для последнего канала
            last_channel = channels[-1]
            for channel_id in list(current_intervals.keys()):
                if channel_id not in channels or channel_id == last_channel:
                    try:
                        await Repository.delete_channel_interval(channel_id)
                    except Exception as e:
                        logger.warning(f"Не удалось удалить интервал для {channel_id}: {e}")

            # Обновляем/устанавливаем записи для смежных пар
            for i in range(len(channels) - 1):
                ch = channels[i]
                nxt = channels[i + 1]
                existing = current_intervals.get(ch)
                # Если уже настроен другой next_channel — переносим интервал на новую пару
                if existing and (existing.get("next_channel") != nxt):
                    interval_seconds = int(existing.get("interval", 0) or 0)
                    if interval_seconds > 0:
                        try:
                            await Repository.set_channel_interval(ch, nxt, interval_seconds)
                        except Exception as e:
                            logger.warning(f"Не удалось обновить интервал {ch} → {nxt}: {e}")
                    else:
                        # Если интервал не задан (>0), не создаем запись
                        try:
                            await Repository.delete_channel_interval(ch)
                        except Exception:
                            pass
                # Если записи нет — оставляем как есть (глобальный интервал будет использоваться)
        except Exception as e:
            logger.error(f"Ошибка синхронизации интервалов: {e}")
    async def confirm_add_admin(self, callback: types.CallbackQuery):
        """Подтверждение и добавление пользователя как админа во все каналы с логированием"""
        if not self.is_admin(callback.from_user.id):
            return
        
        user_id = int(callback.data.replace("confirm_add_admin_", ""))
        
        # Добавляем ID нового админа в конфиг
        if user_id not in self.config.admin_ids:
            self.config.admin_ids.append(user_id)
            self.config.save_config()  # Сохраняем изменения в bot_config.json
            
        progress_msg = await callback.message.edit_text(
            f"🔄 Добавляю пользователя {user_id} как администратора во все каналы...\n\n"
            "Это может занять некоторое время..."
        )
        
        results = []
        source_channels = self.config.source_channels
        
        # Логируем операцию и получаем ID
        operation_id = await Repository.log_admin_operation(
            'promote_admin_batch', user_id, 'all', 'started', 
            f'Starting batch promotion for {len(source_channels)} channels', 
            callback.from_user.id
        )
        
        for i, channel in enumerate(source_channels, 1):
            try:
                await progress_msg.edit_text(
                    f"🔄 Добавление админа ({i}/{len(source_channels)})\n\n"
                    f"Обрабатываю канал: {channel}..."
                )
                
                try:
                    chat = await self.bot.get_chat(channel)
                    channel_name = chat.title or channel
                except Exception:
                    channel_name = channel
                
                # Проверяем, что пользователь уже не является админом
                try:
                    member = await self.bot.get_chat_member(channel, user_id)
                    if member.status in ['administrator', 'creator']:
                        results.append({
                            'channel': channel_name,
                            'status': 'already_admin',
                            'message': 'Уже является администратором'
                        })
                        # Логируем операцию
                        await Repository.log_admin_operation(
                            'promote_admin', user_id, channel, 'already_admin', 
                            'User already admin', callback.from_user.id
                        )
                        continue
                except Exception:
                    pass
                
                # Добавляем пользователя как администратора
                # ВАЖНО: В aiogram 3.20+ изменился API - promote_chat_member больше не принимает privileges,
                # а принимает права как отдельные параметры
                try:
                    # Проверяем права бота в канале
                    try:
                        bot_member = await self.bot.get_chat_member(channel, self.bot.id)
                        if bot_member.status != 'administrator' or not bot_member.can_promote_members:
                            results.append({
                                'channel': channel_name,
                                'status': 'error',
                                'message': 'Бот не имеет прав для назначения администраторов'
                            })
                            await Repository.log_admin_operation(
                                'promote_admin', user_id, channel, 'insufficient_bot_rights', 
                                'Bot lacks promote permissions', callback.from_user.id
                            )
                            continue
                    except Exception as e:
                        results.append({
                            'channel': channel_name,
                            'status': 'error',
                            'message': f'Не удалось проверить права бота: {str(e)[:50]}'
                        })
                        await Repository.log_admin_operation(
                            'promote_admin', user_id, channel, 'bot_check_error', 
                            str(e), callback.from_user.id
                        )
                        continue
                    
                    # Сначала пытаемся добавить пользователя в канал, если его там нет
                    try:
                        member_info = await self.bot.get_chat_member(channel, user_id)
                        if member_info.status == 'left' or member_info.status == 'kicked':
                            # Пользователь не в канале, пробуем снять возможный бан
                            try:
                                await self.bot.unban_chat_member(channel, user_id, only_if_banned=True)
                            except:
                                pass
                    except Exception:
                        # Если не можем получить информацию о пользователе, продолжим попытку назначения
                        pass
                    
                    # Теперь повышаем до администратора
                    await self.bot.promote_chat_member(
                        chat_id=channel,
                        user_id=user_id,
                        can_post_messages=True,
                        can_delete_messages=True,
                        can_invite_users=True,
                        can_restrict_members=True,
                        can_pin_messages=True,
                        can_manage_topics=True,
                        can_promote_members=False,  # Оставляем False для безопасности
                        can_manage_video_chats=True,
                        can_edit_messages=True
                    )
                    
                    results.append({
                        'channel': channel_name,
                        'status': 'success',
                        'message': 'Успешно добавлен как администратор'
                    })
                    
                    # Логируем успешную операцию
                    await Repository.log_admin_operation(
                        'promote_admin', user_id, channel, 'success', 
                        None, callback.from_user.id
                    )
                    
                except Exception as e:
                    error_msg = str(e)
                    
                    # Более детальная обработка ошибок Telegram
                    if "user not found" in error_msg.lower():
                        status = 'user_not_found'
                        message = 'Пользователь не найден в канале'
                    elif "not enough rights" in error_msg.lower() or "not enough rights to restrict/unrestrict chat member" in error_msg.lower():
                        status = 'insufficient_rights'
                        message = 'Недостаточно прав бота для назначения админов'
                    elif "bad request" in error_msg.lower():
                        if "user is an administrator of the chat" in error_msg.lower():
                            status = 'already_admin'
                            message = 'Пользователь уже администратор'
                        elif "can't demote chat creator" in error_msg.lower():
                            status = 'creator_error'
                            message = 'Нельзя изменить права создателя'
                        elif "user not found" in error_msg.lower():
                            status = 'user_not_member'
                            message = 'Пользователь не является участником канала'
                        elif "chat not found" in error_msg.lower():
                            status = 'chat_not_found'
                            message = 'Канал не найден'
                        else:
                            status = 'bad_request'
                            message = f'Ошибка запроса: {error_msg[:100]}'
                    else:
                        status = 'error'
                        message = f'Ошибка: {error_msg[:50]}'

                    # Попытка создать ссылку-приглашение и отправить пользователю/админу
                    invite_sent = False
                    if status in ['user_not_found', 'user_not_member']:
                        try:
                            link = await self.bot.create_chat_invite_link(channel, name=f"Admin invite for {user_id}")
                            invite = getattr(link, 'invite_link', None) or getattr(link, 'link', None) or str(link)
                            # Пытаемся отправить пользователю напрямую
                            try:
                                await self.bot.send_message(
                                    user_id,
                                    f"Вас добавляют администратором в канал: {channel_name}\n\n"
                                    f"Пожалуйста, присоединитесь по ссылке, затем вас автоматически повысят: {invite}"
                                )
                                invite_sent = True
                            except Exception:
                                # Если нельзя написать пользователю, отправим инициатору для пересылки
                                try:
                                    await callback.message.answer(
                                        f"Не удалось отправить пользователю. Перешлите ему ссылку для входа в {channel_name}:\n{invite}"
                                    )
                                    invite_sent = True
                                except Exception:
                                    pass
                            if invite_sent:
                                status = 'invite_sent'
                                message = 'Отправлена ссылка-приглашение'
                        except Exception:
                            pass
                    
                    results.append({
                        'channel': channel_name,
                        'status': status if status in ['invite_sent', 'already_admin', 'success'] else 'error',
                        'message': message
                    })
                    
                    # Логируем ошибку
                    await Repository.log_admin_operation(
                        'promote_admin', user_id, channel, status, 
                        error_msg, callback.from_user.id
                    )
                        
            except Exception as e:
                results.append({
                    'channel': channel,
                    'status': 'error',
                    'message': f'Общая ошибка: {str(e)[:50]}'
                })
                
                # Логируем общую ошибку
                await Repository.log_admin_operation(
                    'promote_admin', user_id, channel, 'error', 
                    str(e), callback.from_user.id
                )
        
        # Формируем отчет
        success_count = sum(1 for r in results if r['status'] == 'success')
        already_admin_count = sum(1 for r in results if r['status'] == 'already_admin')
        invite_sent_count = sum(1 for r in results if r['status'] == 'invite_sent')
        error_count = len(results) - success_count - already_admin_count - invite_sent_count
        
        report = f"👥 Результаты добавления администратора\n\n"
        report += f"Пользователь ID: {user_id}\n\n"
        report += f"✅ Добавлен: {success_count}\n"
        report += f"ℹ️ Уже админ: {already_admin_count}\n"
        report += f"🔗 Отправлено приглашений: {invite_sent_count}\n"
        report += f"❌ Ошибок: {error_count}\n\n"
        
        # Группируем результаты по статусу
        for status_type, icon in [('success', '✅'), ('already_admin', 'ℹ️'), ('invite_sent', '🔗'), ('error', '❌')]:
            status_results = [r for r in results if r['status'] == status_type]
            if status_results:
                report += f"{icon} "
                if status_type == 'success':
                    report += "Успешно добавлен:\n"
                elif status_type == 'already_admin':
                    report += "Уже администратор:\n"
                elif status_type == 'invite_sent':
                    report += "Отправлены приглашения для вступления:\n"
                else:
                    report += "Ошибки:\n"
                
                for result in status_results[:5]:
                    channel_name = result['channel'][:25] + "..." if len(result['channel']) > 25 else result['channel']
                    report += f"  • {channel_name}\n"
                
                if len(status_results) > 5:
                    report += f"  • ... и еще {len(status_results) - 5}\n"
                report += "\n"
        
        if len(report) > 4000:
            report = report[:3800] + "\n\n... (отчет сокращен)"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Проверить и повысить", callback_data=f"retry_promote_{user_id}")
        kb.button(text="📊 История операций", callback_data=f"admin_history_{operation_id}")
        kb.button(text="🔙 Назад", callback_data="back_to_main")
        kb.adjust(1)
        
        await progress_msg.edit_text(report, reply_markup=kb.as_markup())
        await callback.answer()

    async def retry_promote(self, callback: types.CallbackQuery):
        """Повторная проверка членства и попытка повышения до администратора после приглашения"""
        if not self.is_admin(callback.from_user.id):
            return
        try:
            user_id = int(callback.data.replace("retry_promote_", ""))
        except Exception:
            await callback.answer()
            return

        progress_msg = await callback.message.edit_text(
            "🔄 Повторная проверка и повышение прав..."
        )

        results = []
        source_channels = self.config.source_channels

        for i, channel in enumerate(source_channels, 1):
            try:
                await progress_msg.edit_text(
                    f"🔄 Повышение ({i}/{len(source_channels)})\n\n"
                    f"Обрабатываю канал: {channel}..."
                )

                try:
                    chat = await self.bot.get_chat(channel)
                    channel_name = chat.title or channel
                except Exception:
                    channel_name = channel

                # Проверяем, что уже админ
                try:
                    member = await self.bot.get_chat_member(channel, user_id)
                    if member.status in ['administrator', 'creator']:
                        results.append({'channel': channel_name, 'status': 'already_admin', 'message': 'Уже администратор'})
                        continue
                except Exception:
                    pass

                # Пробуем повысить
                try:
                    await self.bot.promote_chat_member(
                        chat_id=channel,
                        user_id=user_id,
                        can_post_messages=True,
                        can_delete_messages=True,
                        can_invite_users=True,
                        can_restrict_members=True,
                        can_pin_messages=True,
                        can_manage_topics=True,
                        can_promote_members=False,
                        can_manage_video_chats=True,
                        can_edit_messages=True
                    )
                    results.append({'channel': channel_name, 'status': 'success', 'message': 'Повышен'})
                except Exception:
                    # Если снова не получилось — создадим пригласительную ссылку
                    try:
                        link = await self.bot.create_chat_invite_link(channel, name=f"Admin invite for {user_id}")
                        invite = getattr(link, 'invite_link', None) or getattr(link, 'link', None) or str(link)
                        try:
                            await self.bot.send_message(
                                user_id,
                                f"Для повышения до администратора присоединитесь в {channel_name}: {invite}"
                            )
                            results.append({'channel': channel_name, 'status': 'invite_sent', 'message': 'Отправлена ссылка'})
                        except Exception:
                            await callback.message.answer(
                                f"Перешлите пользователю ссылку для входа в {channel_name}:\n{invite}"
                            )
                            results.append({'channel': channel_name, 'status': 'invite_sent', 'message': 'Ссылка отправлена администратору'})
                    except Exception as e:
                        results.append({'channel': channel_name, 'status': 'error', 'message': f'Ошибка: {str(e)[:50]}'})

            except Exception as e:
                results.append({'channel': channel, 'status': 'error', 'message': f'Ошибка: {str(e)[:50]}'})

        success_count = sum(1 for r in results if r['status'] == 'success')
        already_admin_count = sum(1 for r in results if r['status'] == 'already_admin')
        invite_sent_count = sum(1 for r in results if r['status'] == 'invite_sent')
        error_count = len(results) - success_count - already_admin_count - invite_sent_count

        report = (
            "👥 Результаты повторной попытки\n\n"
            f"✅ Повышены: {success_count}\n"
            f"ℹ️ Уже админ: {already_admin_count}\n"
            f"🔗 Отправлено приглашений: {invite_sent_count}\n"
            f"❌ Ошибок: {error_count}\n\n"
        )

        for status_type, icon in [('success', '✅'), ('already_admin', 'ℹ️'), ('invite_sent', '🔗'), ('error', '❌')]:
            status_results = [r for r in results if r['status'] == status_type]
            if status_results:
                report += f"{icon} "
                if status_type == 'success':
                    report += "Повышены:\n"
                elif status_type == 'already_admin':
                    report += "Уже администраторы:\n"
                elif status_type == 'invite_sent':
                    report += "Отправлены приглашения:\n"
                else:
                    report += "Ошибки:\n"
                for result in status_results[:5]:
                    channel_name = result['channel'][:25] + "..." if len(result['channel']) > 25 else result['channel']
                    report += f"  • {channel_name}\n"
                if len(status_results) > 5:
                    report += f"  • ... и еще {len(status_results) - 5}\n"
                report += "\n"

        kb = InlineKeyboardBuilder()
        kb.button(text="🔄 Повторить", callback_data=f"retry_promote_{user_id}")
        kb.button(text="🔙 Назад", callback_data="back_to_main")
        kb.adjust(1)
        await progress_msg.edit_text(report, reply_markup=kb.as_markup())
        await callback.answer()

    async def add_user_as_admin_submit(self, message: types.Message):
        """Обработка ID пользователя для добавления как админа"""
        if not self.is_admin(message.from_user.id):
            return
        
        if not hasattr(self, 'awaiting_user_id') or self.awaiting_user_id != message.from_user.id:
            return
        
        try:
            user_id = int(message.text.strip())
        except ValueError:
            await message.reply("❌ ID пользователя должен быть числом")
            return
        
        self.awaiting_user_id = None
        
        # Проверяем, что пользователь существует
        try:
            user = await self.bot.get_chat(user_id)
            user_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
            if not user_name:
                user_name = user.username or f"ID: {user_id}"
        except Exception as e:
            await message.reply(f"❌ Не удалось найти пользователя с ID {user_id}: {e}")
            return
        
        # Создаем клавиатуру подтверждения
        kb = InlineKeyboardBuilder()
        kb.button(text="✅ Сгенерировать ссылки", callback_data=f"confirm_add_admin_{user_id}")
        kb.button(text="❌ Отмена", callback_data="back_to_main")
        kb.adjust(2)
        
        await message.reply(
            f"👤 Подтверждение выдачи ссылок\n\n"
            f"Пользователь: {user_name}\n"
            f"ID: {user_id}\n\n"
            f"Будут сгенерированы ссылки для вступления во все {len(self.config.source_channels)} каналов:\n\n"
            + "\n".join([f"• {ch}" for ch in self.config.source_channels[:5]]) +
            (f"\n• ... и еще {len(self.config.source_channels) - 5}" if len(self.config.source_channels) > 5 else "") +
            "\n\nПродолжить?",
            reply_markup=kb.as_markup()
        ) 
    async def show_admin_history(self, callback: types.CallbackQuery):
        """Показать историю операций с администраторами"""
        if not self.is_admin(callback.from_user.id):
            return
        
        try:
            history = await Repository.get_admin_operations_history(limit=30)
            
            if not history:
                await callback.message.edit_text(
                    "📊 История операций пуста.",
                    reply_markup=InlineKeyboardBuilder().button(
                        text="🔙 Назад", callback_data="back_to_main"
                    ).as_markup()
                )
                await callback.answer()
                return
            
            report = "📊 История операций с администраторами\n\n"
            
            # Группируем по типам операций
            from collections import defaultdict
            by_type = defaultdict(list)
            
            for op in history:
                by_type[op['operation_type']].append(op)
            
            for op_type, operations in by_type.items():
                success_count = sum(1 for op in operations if op['status'] == 'success')
                total_count = len(operations)
                
                type_name = {
                    'promote_admin': 'Назначение админов',
                    'demote_admin': 'Снятие админов',
                    'ban_user': 'Блокировка пользователей'
                }.get(op_type, op_type)
                
                report += f"🔧 {type_name} ({success_count}/{total_count} успешно)\n"
                
                # Показываем последние операции
                for op in operations[:5]:
                    timestamp = op['timestamp'][:16] if op['timestamp'] else 'Неизвестно'
                    status_icon = "✅" if op['status'] == 'success' else "❌"
                    user_id = op['target_user_id']
                    
                    # Получаем информацию о канале если есть
                    channel_info = ""
                    if op['target_channel_id']:
                        try:
                            chat = await self.bot.get_chat(op['target_channel_id'])
                            channel_name = chat.title[:10] + "..." if len(chat.title) > 10 else chat.title
                            channel_info = f" в {channel_name}"
                        except:
                            channel_info = f" в {op['target_channel_id'][-8:]}"
                    
                    report += f"  {timestamp} {status_icon} Пользователь {user_id}{channel_info}\n"
                    
                    if op['error_message'] and op['status'] != 'success':
                        error_short = op['error_message'][:30] + "..." if len(op['error_message']) > 30 else op['error_message']
                        report += f"    Ошибка: {error_short}\n"
                
                if len(operations) > 5:
                    report += f"  ... и еще {len(operations) - 5} операций\n"
                report += "\n"
            
            if len(report) > 4000:
                report = report[:3800] + "\n\n... (отчет сокращен)"
            
            kb = InlineKeyboardBuilder()
            kb.button(text="👥 Добавить админа", callback_data="add_user_admin")
            kb.button(text="🔙 Назад", callback_data="back_to_main")
            kb.adjust(1)
            
            await callback.message.edit_text(report, reply_markup=kb.as_markup())
            await callback.answer()
            
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка получения истории: {e}",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 Назад", callback_data="back_to_main"
                ).as_markup()
            )
            await callback.answer()   
    async def add_user_as_admin_prompt(self, callback: types.CallbackQuery):
        """Запрос ID пользователя для добавления как админа"""
        if not self.is_admin(callback.from_user.id):
            return
        
        self.awaiting_user_id = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_main")
        
        await callback.message.edit_text(
            "👥 Добавление пользователя как админа\n\n"
            "Отправьте ID пользователя, которого нужно добавить как администратора во все каналы.\n\n"
            "Как получить ID пользователя:\n"
            "1. Попросите пользователя написать боту @userinfobot\n"
            "2. Или используйте @getidsbot\n"
            "3. Отправьте полученный ID числом\n\n"
            "Отправьте ID пользователя сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()

    async def save_clone_state(self):
        """Сохранение состояния клонов в файл"""
        try:
            clone_states = {}
            
            for bot_id, process in self.bot_manager.processes.items():
                clone_states[bot_id] = {
                    'active': process.is_alive() if process else False,
                    'pid': process.pid if process and process.is_alive() else None,
                    'started_at': self.bot_manager.bots.get(bot_id, {}).get('started_at'),
                    'bot_id': bot_id
                }
            
            with open('clone_states.json', 'w') as f:
                json.dump(clone_states, f, indent=2)
            
            logger.info(f"Сохранено состояние {len(clone_states)} клонов")
        except Exception as e:
            logger.error(f"Ошибка сохранения состояния клонов: {e}")

    async def load_clone_state(self):
        """Загрузка состояния клонов из базы данных"""
        try:
            clones = await Repository.get_bot_clones()
            
            for clone in clones:
                bot_id = clone['bot_id']
                pid = clone['pid']
                
                if clone['status'] == 'active' and pid:
                    # Проверяем, существует ли процесс
                    try:
                        import psutil
                        if psutil.pid_exists(pid):
                            # Процесс существует, добавляем в менеджер
                            self.bot_manager.bots[bot_id] = {
                                'status': 'recovered',
                                'pid': pid,
                                'started_at': clone['started_at'],
                                'note': 'Восстановлен из БД после перезапуска'
                            }
                            logger.info(f"Восстановлен клон {bot_id} с PID {pid}")
                        else:
                            # Процесс не существует, обновляем статус
                            await Repository.update_clone_status(
                                bot_id, 'inactive', None, 'Процесс не найден после перезапуска'
                            )
                    except Exception as e:
                        logger.warning(f"Не удалось проверить процесс клона {bot_id}: {e}")
                        await Repository.update_clone_status(
                            bot_id, 'error', None, f'Ошибка проверки: {str(e)}'
                        )
            
            logger.info(f"Загружено состояние {len(clones)} клонов из БД")
        except Exception as e:
            logger.error(f"Ошибка загрузки состояния клонов из БД: {e}")

    async def test_channel_forwarding(self, callback: types.CallbackQuery):
        """Тестовая пересылка для всех каналов"""
        if not self.is_admin(callback.from_user.id):
            return
        
        source_channels = self.config.source_channels
        if not source_channels:
            await callback.message.edit_text(
                "❌ Нет настроенных каналов для тестирования.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 Назад", callback_data="back_to_main"
                ).as_markup()
            )
            await callback.answer()
            return
        
        # Создаем прогресс-сообщение
        progress_msg = await callback.message.edit_text(
            f"🧪 Начинаю тестирование {len(source_channels)} каналов...\n\n"
            "Проверяю доступность последних сообщений..."
        )
        
        test_results = []
        target_chats = await Repository.get_target_chats()
        
        for i, channel in enumerate(source_channels, 1):
            try:
                # Обновляем прогресс
                await progress_msg.edit_text(
                    f"🧪 Тестирование каналов ({i}/{len(source_channels)})\n\n"
                    f"Проверяю канал: {channel}..."
                )
                
                # Получаем информацию о канале
                try:
                    chat = await self.bot.get_chat(channel)
                    channel_name = chat.title or channel
                except Exception:
                    channel_name = channel
                
                # Получаем последнее сообщение
                message_id = await Repository.get_last_message(channel)
                
                if not message_id:
                    # Пробуем найти последнее сообщение
                    latest_id = await self.find_latest_message(channel)
                    if latest_id:
                        message_id = latest_id
                        await Repository.save_last_message(channel, latest_id)
                    else:
                        test_results.append({
                            'channel': channel,
                            'name': channel_name,
                            'status': 'error',
                            'message': 'Не найдено сообщений'
                        })
                        continue
                
                # Тестируем пересылку в первый доступный чат
                forwarded = False
                forward_errors = []
                
                for chat_id in target_chats:
                    if str(chat_id) == channel:
                        continue
                    
                    try:
                        # Пробуем переслать последнее сообщение
                        await self.bot.forward_message(
                            chat_id=chat_id,
                            from_chat_id=channel,
                            message_id=message_id
                        )
                        forwarded = True
                        break
                    except Exception as e:
                        forward_errors.append(f"Чат {chat_id}: {str(e)[:50]}")
                
                if forwarded:
                    test_results.append({
                        'channel': channel,
                        'name': channel_name,
                        'status': 'success',
                        'message': f'Сообщение {message_id} успешно переслано'
                    })
                else:
                    test_results.append({
                        'channel': channel,
                        'name': channel_name,
                        'status': 'error',
                        'message': f'Ошибки пересылки: {"; ".join(forward_errors[:2])}'
                    })
                    
            except Exception as e:
                test_results.append({
                    'channel': channel,
                    'name': channel_name,
                    'status': 'error',
                    'message': f'Общая ошибка: {str(e)[:50]}'
                })
        
        # Формируем отчет
        success_count = sum(1 for r in test_results if r['status'] == 'success')
        total_count = len(test_results)
        
        report = f"🧪 Результаты тестирования каналов\n\n"
        report += f"✅ Успешно: {success_count}/{total_count}\n"
        report += f"❌ Ошибок: {total_count - success_count}/{total_count}\n\n"
        
        for result in test_results:
            status_icon = "✅" if result['status'] == 'success' else "❌"
            name = result['name'][:20] + "..." if len(result['name']) > 20 else result['name']
            report += f"{status_icon} {name}\n"
            report += f"   {result['message']}\n\n"
        
        if len(report) > 4000:
            # Если отчет слишком длинный, сокращаем
            report = report[:3800] + "\n\n... (отчет сокращен)"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 К каналам", callback_data="channels")
        
        await progress_msg.edit_text(report, reply_markup=kb.as_markup())
        await callback.answer()
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
                        # Синхронизируем интервалы с учетом нового канала
                        await self.sync_intervals_with_order()
                        
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
        """Show enhanced channel removal menu with channel IDs and full list"""
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
        
        # Создаем детальный список каналов с информацией
        text = f"❌ Удаление каналов\n\nВсего каналов: {len(source_channels)}\n\n"
        text += "📋 Список всех каналов:\n"
        
        # Получаем информацию о каналах для отображения
        channel_info = {}
        for i, channel in enumerate(source_channels):
            try:
                chat = await self.bot.get_chat(channel)
                title = chat.title or f"Канал {i+1}"
                # Обрезаем длинные названия
                if len(title) > 25:
                    title = title[:22] + "..."
                channel_info[channel] = title
            except Exception as e:
                channel_info[channel] = f"Канал {i+1}"
                logger.debug(f"Не удалось получить название канала {channel}: {e}")
        
        # Показываем все каналы с номерами и ID
        for i, channel in enumerate(source_channels):
            title = channel_info[channel]
            # Показываем краткий ID (последние 8 символов)
            short_id = channel[-8:] if len(channel) > 8 else channel
            text += f"{i+1}. {title} ({short_id})\n"
        
        text += f"\n⚠️ Выберите канал для удаления:"
        
        # Создаем клавиатуру с пагинацией
        markup = self.create_detailed_removal_keyboard(source_channels, page, channel_info)
        
        await callback.message.edit_text(text, reply_markup=markup)
        await callback.answer()

    def create_detailed_removal_keyboard(self, channels, page=0, channel_info=None, per_page=5):
        """Create detailed removal keyboard with full channel info"""
        from aiogram.utils.keyboard import InlineKeyboardBuilder
        
        kb = InlineKeyboardBuilder()
        
        if not channels:
            kb.button(text="Нет каналов", callback_data="dummy")
            kb.button(text="🔙 К каналам", callback_data="channels")
            return kb.as_markup()
        
        # Пагинация
        total_channels = len(channels)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total_channels)
        current_channels = channels[start_idx:end_idx]
        
        # Добавляем кнопки для текущей страницы
        for i, channel in enumerate(current_channels):
            actual_index = start_idx + i + 1  # Номер в общем списке
            
            # Получаем название канала
            if channel_info and channel in channel_info:
                channel_name = channel_info[channel]
            else:
                channel_name = f"Канал {actual_index}"
            
            # Создаем краткий ID для кнопки
            short_id = channel[-6:] if len(channel) > 6 else channel
            
            # Обрезаем название для кнопки если слишком длинное
            if len(channel_name) > 20:
                display_name = channel_name[:17] + "..."
            else:
                display_name = channel_name
            
            # Кнопка удаления с номером, названием и ID
            kb.button(
                text=f"❌ {actual_index}. {display_name} ({short_id})",
                callback_data=f"remove_channel_{channel}"
            )
        
        # Навигационные кнопки для пагинации
        navigation_buttons = []
        
        # Кнопка "Предыдущая страница"
        if page > 0:
            navigation_buttons.append(("⬅️ Назад", f"remove_channel_page_{page - 1}"))
        
        # Показать текущую страницу
        total_pages = (total_channels + per_page - 1) // per_page
        if total_pages > 1:
            navigation_buttons.append((f"📄 {page + 1}/{total_pages}", "dummy"))
        
        # Кнопка "Следующая страница"
        if end_idx < total_channels:
            navigation_buttons.append(("➡️ Далее", f"remove_channel_page_{page + 1}"))
        
        # Добавляем навигационные кнопки
        for text, callback in navigation_buttons:
            kb.button(text=text, callback_data=callback)
        
        # Кнопка "Назад к каналам"
        kb.button(text="🔙 К каналам", callback_data="channels")
        
        # Настройка раскладки: кнопки удаления по одной в ряд
        kb.adjust(1)  # Каналы по одному в ряд
        if len(navigation_buttons) > 1:
            # Если есть навигация, размещаем её в отдельном ряду
            kb.adjust(*([1] * len(current_channels) + [len(navigation_buttons)] + [1]))
        
        return kb.as_markup()
    async def test_channel_forwarding(self, callback: types.CallbackQuery):
        """Тестовая пересылка для всех каналов с сохранением результатов"""
        if not self.is_admin(callback.from_user.id):
            return
        
        source_channels = self.config.source_channels
        if not source_channels:
            await callback.message.edit_text(
                "❌ Нет настроенных каналов для тестирования.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 Назад", callback_data="back_to_main"
                ).as_markup()
            )
            await callback.answer()
            return
        
        progress_msg = await callback.message.edit_text(
            f"🧪 Начинаю тестирование {len(source_channels)} каналов...\n\n"
            "Проверяю доступность последних сообщений..."
        )
        
        test_results = []
        target_chats = await Repository.get_target_chats()
        start_time = datetime.now()
        
        for i, channel in enumerate(source_channels, 1):
            channel_start_time = datetime.now()
            
            try:
                await progress_msg.edit_text(
                    f"🧪 Тестирование каналов ({i}/{len(source_channels)})\n\n"
                    f"Проверяю канал: {channel}..."
                )
                
                try:
                    chat = await self.bot.get_chat(channel)
                    channel_name = chat.title or channel
                except Exception:
                    channel_name = channel
                
                message_id = await Repository.get_last_message(channel)
                
                if not message_id:
                    latest_id = await self.find_latest_message(channel)
                    if latest_id:
                        message_id = latest_id
                        await Repository.save_last_message(channel, latest_id)
                    else:
                        duration_ms = int((datetime.now() - channel_start_time).total_seconds() * 1000)
                        await Repository.save_channel_test_result(
                            channel, 'error', None, 'Не найдено сообщений', duration_ms
                        )
                        test_results.append({
                            'channel': channel,
                            'name': channel_name,
                            'status': 'error',
                            'message': 'Не найдено сообщений'
                        })
                        continue
                
                forwarded = False
                forward_errors = []
                
                for chat_id in target_chats:
                    if str(chat_id) == channel:
                        continue
                    
                    try:
                        await self.bot.forward_message(
                            chat_id=chat_id,
                            from_chat_id=channel,
                            message_id=message_id
                        )
                        forwarded = True
                        break
                    except Exception as e:
                        forward_errors.append(f"Чат {chat_id}: {str(e)[:50]}")
                
                duration_ms = int((datetime.now() - channel_start_time).total_seconds() * 1000)
                
                if forwarded:
                    await Repository.save_channel_test_result(
                        channel, 'success', message_id, None, duration_ms
                    )
                    test_results.append({
                        'channel': channel,
                        'name': channel_name,
                        'status': 'success',
                        'message': f'Сообщение {message_id} успешно переслано'
                    })
                else:
                    error_msg = "; ".join(forward_errors[:2])
                    await Repository.save_channel_test_result(
                        channel, 'error', message_id, error_msg, duration_ms
                    )
                    test_results.append({
                        'channel': channel,
                        'name': channel_name,
                        'status': 'error',
                        'message': f'Ошибки пересылки: {error_msg}'
                    })
                    
            except Exception as e:
                duration_ms = int((datetime.now() - channel_start_time).total_seconds() * 1000)
                error_msg = str(e)[:100]
                await Repository.save_channel_test_result(
                    channel, 'error', None, error_msg, duration_ms
                )
                test_results.append({
                    'channel': channel,
                    'name': channel_name,
                    'status': 'error',
                    'message': f'Общая ошибка: {error_msg[:50]}'
                })
        
        # Формируем отчет
        total_time = (datetime.now() - start_time).total_seconds()
        success_count = sum(1 for r in test_results if r['status'] == 'success')
        total_count = len(test_results)
        
        report = f"🧪 Результаты тестирования каналов\n\n"
        report += f"⏱️ Время выполнения: {total_time:.1f} сек\n"
        report += f"✅ Успешно: {success_count}/{total_count}\n"
        report += f"❌ Ошибок: {total_count - success_count}/{total_count}\n\n"
        
        for result in test_results:
            status_icon = "✅" if result['status'] == 'success' else "❌"
            name = result['name'][:20] + "..." if len(result['name']) > 20 else result['name']
            report += f"{status_icon} {name}\n"
            report += f"   {result['message']}\n\n"
        
        if len(report) > 4000:
            report = report[:3800] + "\n\n... (отчет сокращен)"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="📊 История тестов", callback_data="test_history")
        kb.button(text="🔙 К каналам", callback_data="channels")
        kb.adjust(1)
        
        await progress_msg.edit_text(report, reply_markup=kb.as_markup())
        await callback.answer()

    async def show_test_history(self, callback: types.CallbackQuery):
        """Показать историю тестирования каналов"""
        if not self.is_admin(callback.from_user.id):
            return
        
        try:
            history = await Repository.get_channel_test_history(limit=20)
            
            if not history:
                await callback.message.edit_text(
                    "📊 История тестирования пуста.",
                    reply_markup=InlineKeyboardBuilder().button(
                        text="🔙 Назад", callback_data="channels"
                    ).as_markup()
                )
                await callback.answer()
                return
            
            report = "📊 История тестирования каналов\n\n"
            
            # Группируем по дням
            from collections import defaultdict
            by_date = defaultdict(list)
            
            for test in history:
                date_str = test['test_timestamp'][:10] if test['test_timestamp'] else 'Неизвестно'
                by_date[date_str].append(test)
            
            for date_str in sorted(by_date.keys(), reverse=True):
                tests = by_date[date_str]
                success_count = sum(1 for t in tests if t['status'] == 'success')
                total_count = len(tests)
                
                report += f"📅 {date_str} ({success_count}/{total_count} успешно)\n"
                
                # Показываем последние 3 теста за день
                for test in tests[:3]:
                    time_str = test['test_timestamp'][11:16] if len(test['test_timestamp']) > 10 else ''
                    status_icon = "✅" if test['status'] == 'success' else "❌"
                    channel_id = test['channel_id']
                    duration = f" ({test['test_duration_ms']}ms)" if test['test_duration_ms'] else ""
                    
                    # Получаем название канала
                    try:
                        chat = await self.bot.get_chat(channel_id)
                        channel_name = chat.title[:15] + "..." if len(chat.title) > 15 else chat.title
                    except:
                        channel_name = channel_id[-8:] if len(channel_id) > 8 else channel_id
                    
                    report += f"  {time_str} {status_icon} {channel_name}{duration}\n"
                
                if len(tests) > 3:
                    report += f"  ... и еще {len(tests) - 3} тестов\n"
                report += "\n"
            
            if len(report) > 4000:
                report = report[:3800] + "\n\n... (отчет сокращен)"
            
            kb = InlineKeyboardBuilder()
            kb.button(text="🧪 Новый тест", callback_data="test_channels")
            kb.button(text="🔙 К каналам", callback_data="channels")
            kb.adjust(2)
            
            await callback.message.edit_text(report, reply_markup=kb.as_markup())
            await callback.answer()
            
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка получения истории: {e}",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 Назад", callback_data="channels"
                ).as_markup()
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
        """Remove a source channel with detailed confirmation"""
        if not self.is_admin(callback.from_user.id):
            return
        
        # Извлекаем ID канала из callback_data
        if not callback.data.startswith("remove_channel_"):
            await callback.answer("Неверный формат данных")
            return
        
        channel = callback.data.replace("remove_channel_", "")
        
        # Получаем детальную информацию о канале
        try:
            chat = await self.bot.get_chat(channel)
            channel_name = chat.title or f"ID: {channel}"
            channel_type = "публичный" if chat.username else "приватный"
            member_count = ""
            try:
                count = await self.bot.get_chat_member_count(channel)
                member_count = f", участников: {count}"
            except:
                pass
            channel_details = f"{channel_name} ({channel_type}{member_count})"
        except Exception:
            channel_details = f"ID: {channel}"
        
        # Проверяем позицию канала в списке
        try:
            position = self.config.source_channels.index(channel) + 1
            position_info = f"Позиция в списке: {position}/{len(self.config.source_channels)}"
        except ValueError:
            position_info = "Канал не найден в списке"
        
        # Удаляем канал
        if self.config.remove_source_channel(channel):
            # Также удаляем связанные интервалы
            try:
                await Repository.delete_channel_interval(channel)
            except Exception as e:
                logger.warning(f"Не удалось удалить интервалы для канала {channel}: {e}")
            # Синхронизируем интервалы после удаления канала
            try:
                await self.sync_intervals_with_order()
            except Exception as e:
                logger.warning(f"Ошибка синхронизации интервалов после удаления {channel}: {e}")
            
            # Создаем подробное уведомление
            success_text = (
                f"✅ Канал успешно удален!\n\n"
                f"📋 Удаленный канал:\n{channel_details}\n"
                f"🆔 Полный ID: {channel}\n"
                f"📍 {position_info}\n\n"
                f"🔧 Также удалены связанные интервалы пересылки."
            )
            
            await callback.answer(f"✅ Канал удален")
            
            # Возвращаемся к обновленному меню удаления
            await self.remove_channel_menu(callback)
        else:
            await callback.answer("❌ Не удалось удалить канал")

    async def show_detailed_channels(self, callback: types.CallbackQuery):
        """Show detailed information about all channels"""
        if not self.is_admin(callback.from_user.id):
            return
        
        source_channels = self.config.source_channels
        
        if not source_channels:
            await callback.message.edit_text(
                "📋 Нет настроенных каналов.",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 К каналам", callback_data="channels"
                ).as_markup()
            )
            return
        
        text = f"📋 Детальная информация о каналах ({len(source_channels)}):\n\n"
        
        for i, channel in enumerate(source_channels):
            try:
                chat = await self.bot.get_chat(channel)
                title = chat.title or f"Канал {i+1}"
                channel_type = "публичный" if chat.username else "приватный"
                username_info = f"@{chat.username}" if chat.username else "нет username"
                
                try:
                    member_count = await self.bot.get_chat_member_count(channel)
                    members_info = f"{member_count} участников"
                except:
                    members_info = "неизвестно участников"
                
                text += (
                    f"{i+1}. {title}\n"
                    f"   🆔 ID: {channel}\n"
                    f"   🔗 {username_info}\n"
                    f"   👥 {members_info} ({channel_type})\n\n"
                )
                
            except Exception as e:
                text += (
                    f"{i+1}. ❌ Ошибка доступа\n"
                    f"   🆔 ID: {channel}\n"
                    f"   ⚠️ {str(e)[:50]}...\n\n"
                )
        
        # Ограничиваем длину сообщения
        if len(text) > 4000:
            text = text[:3900] + "\n... (список обрезан)\n"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Удалить канал", callback_data="remove_channel_menu")
        kb.button(text="🔙 К каналам", callback_data="channels")
        kb.adjust(2)
        
        await callback.message.edit_text(text, reply_markup=kb.as_markup())
        await callback.answer()
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
        
        # Загружаем состояние клонов
        await self.load_clone_state()
        
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
            # Сохраняем состояние клонов перед закрытием
            await self.save_clone_state()
            self.cache_service.remove_observer(self)
            await self.bot.session.close()

    def _format_admin_add_results(self, results: list, operation_id: str) -> tuple[str, types.InlineKeyboardMarkup]:
        """Форматирует результаты добавления админа для вывода пользователю."""
        successful = [r for r in results if r['status'] == 'success']
        already_admin = [r for r in results if r['status'] == 'already_admin']
        errors = [r for r in results if r['status'] == 'error']

        final_message = f"✅ Успешно добавлено: {len(successful)}\n"
        final_message += f"☑️ Уже были админами: {len(already_admin)}\n"
        final_message += f"❌ Ошибок: {len(errors)}\n\n"

        error_summary = {}
        for res in errors:
            msg = res['message']
            if msg not in error_summary:
                error_summary[msg] = []
            error_summary[msg].append(res['channel'])

        if error_summary:
            final_message += "‼️ Обнаружены ошибки:\n"
            for msg, channels in error_summary.items():
                final_message += f"\n👉 Ошибка: {msg}\n"
                final_message += "   Затронутые каналы:\n"
                for ch in channels[:10]:
                    final_message += f"   - {ch}\n"
                if len(channels) > 10:
                    final_message += f"   ... и еще {len(channels) - 10}\n"

            final_message += "\n💡 Как исправить:\n"
            if "Бот не имеет прав для назначения администраторов" in error_summary:
                final_message += "1. Убедитесь, что бот является администратором в указанных каналах.\n"
                final_message += "2. Проверьте, что у бота есть право 'Добавлять новых администраторов'.\n"
            if any("Bad Request" in msg for msg in error_summary.keys()):
                final_message += "3. Ошибка 'Bad Request' может означать, что канал недоступен или его тип не поддерживается. Проверьте правильность юзернейма/ID канала.\n"

        kb = InlineKeyboardBuilder()
        kb.button(text="📋 Показать полный лог", callback_data=f"admin_history_{operation_id}")
        kb.button(text="🔙 Назад в меню", callback_data="back_to_main")
        kb.adjust(1)
        
        return final_message, kb.as_markup()

    async def confirm_add_admin(self, callback: types.CallbackQuery):
        """Временно: вместо автодобавления генерируем и выдаём ссылки-приглашения."""
        if not self.is_admin(callback.from_user.id):
            return
        
        user_id = int(callback.data.replace("confirm_add_admin_", ""))
        
        progress_msg = await callback.message.edit_text(
            "🔗 Генерация ссылок для вступления во все каналы..."
        )
        
        source_channels = self.config.source_channels
        links_report_lines = []
        sent_to_user = 0
        errors = []
        
        # Логируем операцию
        operation_id = await Repository.log_admin_operation(
            'promote_admin_batch', user_id, 'all', 'invite_links', 
            f'Generating invite links for {len(source_channels)} channels', 
            callback.from_user.id
        )
        
        for i, channel in enumerate(source_channels, 1):
            try:
                await progress_msg.edit_text(
                    f"🔗 Генерация ({i}/{len(source_channels)})\n\nКанал: {channel}"
                )
                try:
                    chat = await self.bot.get_chat(channel)
                    channel_name = chat.title or channel
                except Exception:
                    channel_name = channel
                
                link_obj = await self.bot.create_chat_invite_link(
                    channel, name=f"Admin setup invite for {user_id}"
                )
                invite = getattr(link_obj, 'invite_link', None) or getattr(link_obj, 'link', None) or str(link_obj)
                links_report_lines.append(f"• {channel_name}: {invite}")
                
                # Пытаемся отправить ссылку пользователю сразу
                try:
                    await self.bot.send_message(
                        user_id,
                        f"Приглашение в канал {channel_name} для дальнейшего назначения администратором:\n{invite}"
                    )
                    sent_to_user += 1
                except Exception:
                    pass
                
                await Repository.log_admin_operation(
                    'promote_admin', user_id, channel, 'invite_link', 
                    'Invite link generated', callback.from_user.id
                )
            except Exception as e:
                err = f"{channel}: {str(e)[:80]}"
                errors.append(err)
                await Repository.log_admin_operation(
                    'promote_admin', user_id, channel, 'error', err, callback.from_user.id
                )
        
        report = (
            "🔗 Ссылки для вступления в каналы\n\n"
            f"Пользователь ID: {user_id}\n"
            f"Отправлено пользователю: {sent_to_user}/{len(source_channels)} (если нельзя написать — ссылки ниже)\n\n"
        )
        if links_report_lines:
            # Ограничим сообщение, если слишком длинное
            links_text = "\n".join(links_report_lines)
            if len(links_text) > 3500:
                links_text = links_text[:3400] + "\n... (список обрезан)"
            report += links_text + "\n\n"
        if errors:
            report += "❌ Ошибки:\n" + "\n".join(errors[:10])
            if len(errors) > 10:
                report += f"\n... и еще {len(errors) - 10}"
        
        kb = InlineKeyboardBuilder()
        kb.button(text="🔙 Назад", callback_data="back_to_main")
        kb.adjust(1)
        await progress_msg.edit_text(report, reply_markup=kb.as_markup())
        await callback.answer()
        
    async def show_admin_history(self, callback: types.CallbackQuery):
        """Показывает историю операций добавления админов"""
        if not self.is_admin(callback.from_user.id):
            return
        
        try:
            history = await Repository.get_admin_operations_history(limit=30)
            
            if not history:
                await callback.message.edit_text(
                    "📊 История операций пуста.",
                    reply_markup=InlineKeyboardBuilder().button(
                        text="🔙 Назад", callback_data="back_to_main"
                    ).as_markup()
                )
                await callback.answer()
                return
            
            report = "📊 История операций с администраторами\n\n"
            
            # Группируем по типам операций
            from collections import defaultdict
            by_type = defaultdict(list)
            
            for op in history:
                by_type[op['operation_type']].append(op)
            
            for op_type, operations in by_type.items():
                success_count = sum(1 for op in operations if op['status'] == 'success')
                total_count = len(operations)
                
                type_name = {
                    'promote_admin': 'Назначение админов',
                    'demote_admin': 'Снятие админов',
                    'ban_user': 'Блокировка пользователей'
                }.get(op_type, op_type)
                
                report += f"🔧 {type_name} ({success_count}/{total_count} успешно)\n"
                
                # Показываем последние операции
                for op in operations[:5]:
                    timestamp = op['timestamp'][:16] if op['timestamp'] else 'Неизвестно'
                    status_icon = "✅" if op['status'] == 'success' else "❌"
                    user_id = op['target_user_id']
                    
                    # Получаем информацию о канале если есть
                    channel_info = ""
                    if op['target_channel_id']:
                        try:
                            chat = await self.bot.get_chat(op['target_channel_id'])
                            channel_name = chat.title[:10] + "..." if len(chat.title) > 10 else chat.title
                            channel_info = f" в {channel_name}"
                        except:
                            channel_info = f" в {op['target_channel_id'][-8:]}"
                    
                    report += f"  {timestamp} {status_icon} Пользователь {user_id}{channel_info}\n"
                    
                    if op['error_message'] and op['status'] != 'success':
                        error_short = op['error_message'][:30] + "..." if len(op['error_message']) > 30 else op['error_message']
                        report += f"    Ошибка: {error_short}\n"
                
                if len(operations) > 5:
                    report += f"  ... и еще {len(operations) - 5} операций\n"
                report += "\n"
            
            if len(report) > 4000:
                report = report[:3800] + "\n\n... (отчет сокращен)"
            
            kb = InlineKeyboardBuilder()
            kb.button(text="👥 Добавить админа", callback_data="add_user_admin")
            kb.button(text="🔙 Назад", callback_data="back_to_main")
            kb.adjust(1)
            
            await callback.message.edit_text(report, reply_markup=kb.as_markup())
            await callback.answer()
            
        except Exception as e:
            await callback.message.edit_text(
                f"❌ Ошибка получения истории: {e}",
                reply_markup=InlineKeyboardBuilder().button(
                    text="🔙 Назад", callback_data="back_to_main"
                ).as_markup()
            )
            await callback.answer()   
    async def add_user_as_admin_prompt(self, callback: types.CallbackQuery):
        """Запрос ID пользователя для добавления как админа"""
        if not self.is_admin(callback.from_user.id):
            return
        
        self.awaiting_user_id = callback.from_user.id
        
        kb = InlineKeyboardBuilder()
        kb.button(text="Отмена", callback_data="back_to_main")
        
        await callback.message.edit_text(
            "👥 Добавление пользователя как админа\n\n"
            "Отправьте ID пользователя, которого нужно добавить как администратора во все каналы.\n\n"
            "Как получить ID пользователя:\n"
            "1. Попросите пользователя написать боту @userinfobot\n"
            "2. Или используйте @getidsbot\n"
            "3. Отправьте полученный ID числом\n\n"
            "Отправьте ID пользователя сообщением 💬",
            reply_markup=kb.as_markup()
        )
        await callback.answer()


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