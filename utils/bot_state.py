from abc import ABC, abstractmethod
from typing import Optional
import asyncio
from loguru import logger
from database.repository import Repository
from datetime import datetime
from aiogram import types

class BotState(ABC):
    """Abstract base class for bot states"""
    
    @abstractmethod
    async def start(self) -> None:
        """Handle start action"""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Handle stop action"""
        pass
    
    @abstractmethod
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Handle message forwarding"""
        pass

class IdleState(BotState):
    """State when bot is not forwarding messages"""
    
    def __init__(self, bot_context, auto_forward: bool = False):
        self.context = bot_context
        self.auto_forward = auto_forward
    
    async def start(self) -> None:
        interval = int(await Repository.get_config("repost_interval", "3600"))
        self.context.state = RunningState(self.context, interval, self.auto_forward)
        await self.context._notify_admins("Бот начал пересылку")
    
    async def stop(self) -> None:
        # Already stopped
        pass
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        # Don't forward messages in idle state
        logger.info("Bot is idle, not forwarding messages")

class RunningState(BotState):
    """State when bot is actively forwarding messages"""
    
    # В класс RunningState добавим отслеживание новых сообщений, которые появились во время ожидания
    def __init__(self, bot_context, interval: int, auto_forward: bool = False):
        self.context = bot_context
        self.interval = interval  # Global repost interval
        self._repost_task: Optional[asyncio.Task] = None
        self.auto_forward = auto_forward
        
        # Initialize tracking for each channel's last post time
        now = datetime.now().timestamp()
        self._channel_last_post = {}
        
        # Initialize all channels with current time
        for channel in self.context.config.source_channels:
            self._channel_last_post[channel] = now
        
        # Initialize these attributes to track channel rotation
        self._last_processed_channel = None
        self._last_global_post_time = now
        
        # Добавляем структуру для отслеживания новых сообщений в период ожидания
        self._pending_messages = {}  # Формат: {channel_id: [message_ids]}
        
        # Start the repost task
        self._start_repost_task()
        
    def _start_repost_task(self):
        # Always start the repost task if it's not running, regardless of auto_forward setting
        if not self._repost_task or self._repost_task.done():
            self._repost_task = asyncio.create_task(self._fallback_repost())

    async def toggle_auto_forward(self):
        """Toggle automatic message forwarding"""
        self.auto_forward = not self.auto_forward
        logger.info(f"Автопересылка: {self.auto_forward}")
        
    async def start(self) -> None:
        # Already running
        pass
    
    async def stop(self) -> None:
        if self._repost_task and not self._repost_task.done():
            self._repost_task.cancel()
        self.auto_forward = False
        self.context.state = IdleState(self.context, self.auto_forward)
        await self.context._notify_admins("Бот остановил пересылку")
    
    # Также модифицируем метод handle_message в классе RunningState в файле utils/bot_state.py

    # Также модифицируем метод handle_message в классе RunningState в файле utils/bot_state.py
# для пересылки в прямом порядке

    async def handle_message(self, channel_id: str, message_id: int) -> None:
        """Обрабатывает пересылку сообщений с учетом настроек автопересылки и правильным порядком"""
        # Проверяем, включена ли автопересылка
        if not self.auto_forward:
            logger.info(f"Получена команда пересылки сообщения {message_id} из канала {channel_id}, но автопересылка отключена")
            return
        
        # Инициализируем временный кэш недоступных сообщений, если он не существует
        if not hasattr(self.context, '_temp_unavailable_messages'):
            self.context._temp_unavailable_messages = {}
        
        # Очищаем устаревшие записи (старше 30 минут)
        current_time = datetime.now().timestamp()
        self.context._temp_unavailable_messages = {k: v for k, v in self.context._temp_unavailable_messages.items() 
                                            if current_time - v < 1800}  # 30 минут
        
        # Определяем диапазон ID сообщений для пересылки
        max_id = message_id
        start_id = max(1, max_id - 10)  # Берем только последние 10 сообщений
        
        # Создаем список ID сообщений в обратном порядке (от новых к старым)
        message_ids = list(range(start_id, max_id + 1))
        message_ids.reverse()  # Переворачиваем список, чтобы начать с самых новых сообщений
        
        logger.info(f"Одновременная пересылка сообщений из канала {channel_id} (от новых к старым)")
        
        # Счетчики для статистики
        forwarded_count = 0
        skipped_count = 0
        error_count = 0
        
        # Создаем список задач для параллельной отправки
        tasks = []
        for msg_id in message_ids:
            msg_key = f"{channel_id}:{msg_id}"
            if msg_key in self.context._temp_unavailable_messages:
                logger.debug(f"Пропуск недавно недоступного сообщения {msg_id} из канала {channel_id}")
                skipped_count += 1
                continue
            
            # Создаем асинхронную задачу
            async def forward_task(msg_id):
                try:
                    return await self.context._forward_message(channel_id, msg_id)
                except Exception as e:
                    error_text = str(e).lower()
                    if "message to forward not found" in error_text or "message can't be forwarded" in error_text:
                        # Добавляем в кэш недоступных сообщений
                        self.context._temp_unavailable_messages[f"{channel_id}:{msg_id}"] = current_time
                    
                    if "message to forward not found" not in error_text and "message can't be forwarded" not in error_text:
                        logger.error(f"Ошибка при пересылке сообщения {msg_id} из канала {channel_id}: {e}")
                    
                    return False
            
            tasks.append(forward_task(msg_id))
        
        # Запускаем все задачи одновременно
        if tasks:
            results = await asyncio.gather(*tasks)
            
            # Обрабатываем результаты
            for success in results:
                if success:
                    forwarded_count += 1
                else:
                    error_count += 1
        
        logger.info(f"Пересылка сообщений из канала {channel_id} завершена: переслано {forwarded_count}, пропущено {skipped_count}, ошибок {error_count}")
        
        # Обновляем время последней пересылки для этого канала
        self._channel_last_post[channel_id] = datetime.now().timestamp()
    
    async def _get_next_channel_to_repost(self):
        """Get the next channel that should be reposted based on intervals"""
        now = datetime.now().timestamp()
        source_channels = self.context.config.source_channels
        
        if not source_channels:
            return None
        
        # Use the interval set by the user
        channel_interval = self.interval
        
        # Find the channel that hasn't been posted for the longest time
        oldest_channel = None
        oldest_time = now
        
        for channel in source_channels:
            last_post_time = self._channel_last_post.get(channel, 0)
            
            # If this channel hasn't been posted for more than the interval
            # and is older than our current oldest, select it
            if now - last_post_time >= channel_interval and last_post_time < oldest_time:
                oldest_channel = channel
                oldest_time = last_post_time
                
        return oldest_channel
        
    async def _get_channel_pair_interval(self, channel1: str, channel2: str) -> Optional[int]:
        """Get the interval between two channels (if set)"""
        try:
            # Get from database
            intervals = await Repository.get_channel_intervals()
            
            # Check if this pair has a configured interval
            for pair_key, pair_data in intervals.items():
                if pair_key == channel1 and pair_data["next_channel"] == channel2:
                    return pair_data["interval"]
            
            return None  # No specific interval set
        except Exception as e:
            logger.error(f"Error getting channel pair interval: {e}")
            return None
        
    # Также улучшим периодическую пересылку в методе _fallback_repost в RunningState:

        # 1. Сначала добавим новый метод для проверки доступности сообщения без пересылки
    async def _check_message(self, channel_id: str, message_id: int) -> tuple:
        """
        Упрощенная проверка доступности сообщения
        Мы просто считаем, что сообщение существует, и будем обрабатывать ошибки при его пересылке
        """
        # В этом методе мы просто предполагаем, что сообщение существует
        # Реальная проверка будет выполнена при пересылке
        return (True, {
            'channel_id': channel_id,
            'message_id': message_id,
            'check_time': datetime.now().timestamp()
        })

    # 2. Метод для пересылки конкретного сообщения во все целевые чаты
    async def _forward_specific_message(self, channel_id: str, message_id: int) -> bool:
        """Пересылает конкретное сообщение во все целевые чаты"""
        success = False
        target_chats = await Repository.get_target_chats()
        
        # Используем bot из контекста
        bot = self.context.bot
        
        if not target_chats:
            logger.warning("Нет целевых чатов для пересылки")
            return False
            
        for chat_id in target_chats:
            if str(chat_id) == channel_id:
                logger.info(f"Пропускаю пересылку в исходный канал {chat_id}")
                continue
                
            try:
                await bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                await Repository.log_forward(message_id)
                success = True
                logger.debug(f"Сообщение {message_id} успешно переслано в {chat_id}")
            except Exception as e:
                error_text = str(e).lower()
                if "message to forward not found" in error_text or "message can't be forwarded" in error_text:
                    logger.debug(f"Сообщение {message_id} недоступно для пересылки в {chat_id}")
                elif "bot was blocked by the user" in error_text:
                    logger.warning(f"Бот заблокирован в чате {chat_id}")
                elif "chat not found" in error_text:
                    logger.warning(f"Чат {chat_id} не найден")
                else:
                    logger.error(f"Ошибка при пересылке в {chat_id}: {e}")
        
        return success

    # 3. Теперь обновляем метод _fallback_repost для использования новой логики
    async def _fallback_repost(self):
        """Periodic repost task with parallel message checking but sequential sending"""
        while True:
            try:
                await asyncio.sleep(10)
                
                now = datetime.now().timestamp()
                source_channels = self.context.config.source_channels
                if not source_channels:
                    logger.warning("Нет настроенных исходных каналов")
                    continue
                
                # Получаем все интервалы для каналов
                channel_intervals = await Repository.get_channel_intervals()
                
                # Если предыдущий канал не определен, начинаем с первого канала
                if self._last_processed_channel is None:
                    next_channel = source_channels[0]
                 #   logger.debug(f"Первый запуск, выбран канал {next_channel}")
                else:
                    next_channel = None
                    current_idx = -1
                    
                    try:
                        # Находим индекс последнего обработанного канала
                        current_idx = source_channels.index(self._last_processed_channel)
                    except ValueError:
                        # Если канал больше не в списке, начинаем с начала
                        current_idx = -1
                    
                    # Проверяем специальные интервалы между каналами
                    if current_idx != -1:
                        # Проверяем, есть ли настроенный интервал для последнего канала
                        last_channel = self._last_processed_channel
                        
                        # Проверяем, есть ли прямое указание следующего канала через интервалы
                        next_channel_defined = False
                        special_interval = 0
                        
                        if last_channel in channel_intervals:
                            interval_data = channel_intervals.get(last_channel, {})
                            if interval_data.get("next_channel") in source_channels:
                                next_defined_channel = interval_data.get("next_channel")
                                special_interval = interval_data.get("interval", 0)
                                
                                # Проверяем, прошло ли достаточно времени
                                if now - self._last_global_post_time >= special_interval:
                                    next_channel = next_defined_channel
                                    next_channel_defined = True
                                    logger.debug(f"Использую настроенный следующий канал {next_channel} после {last_channel} (интервал {special_interval}с)")
                                else:
                                    # Интервал между каналами еще не прошел
                                    time_left = special_interval - (now - self._last_global_post_time)
                                    logger.debug(f"Ожидание {time_left:.1f}с для интервала между {last_channel} и {next_defined_channel}")
                                    continue
                        
                        # Если нет прямого указания, следуем последовательности в списке
                        if not next_channel_defined:
                            # Проверим, прошел ли глобальный интервал
                            if now - self._last_global_post_time < self.interval:
                                time_left = self.interval - (now - self._last_global_post_time)
                                logger.debug(f"Ожидание {time_left:.1f}с для глобального интервала пересылки")
                                continue
                            
                            # Берем следующий канал по порядку
                            next_idx = (current_idx + 1) % len(source_channels)
                            next_channel = source_channels[next_idx]
                            logger.debug(f"Выбран следующий канал по порядку: {next_channel}")
                    
                    # Если next_channel все еще не определен, начинаем сначала
                    if next_channel is None:
                        next_channel = source_channels[0]
                        logger.debug(f"Начинаем цикл заново с канала {next_channel}")
                
                # Проверяем время последней пересылки для выбранного канала
                last_post_time = self._channel_last_post.get(next_channel, 0)
                if now - last_post_time < self.interval:
                    # Для этого канала еще не прошел интервал пересылки
                    continue
                
                # Получаем ID последнего сообщения в канале
                message_id = await Repository.get_last_message(next_channel)
                
                if not message_id:
                    logger.warning(f"Не найдено сообщение для канала {next_channel}")
                    
                    try:
                        latest_id = await self.context.find_latest_message(next_channel)
                        if latest_id:
                            message_id = latest_id
                            await Repository.save_last_message(next_channel, latest_id)
                        else:
                            self._channel_last_post[next_channel] = now
                            continue
                    except Exception as e:
                        logger.error(f"Ошибка при поиске последнего сообщения: {e}")
                        self._channel_last_post[next_channel] = now
                        continue
                
                pending_messages = self._pending_messages.get(next_channel, [])
                if pending_messages:
                    # Добавляем отложенные сообщения в диапазон для пересылки
                    logger.info(f"Найдены отложенные сообщения для канала {next_channel}: {pending_messages}")
                    # Определяем диапазон, включая все отложенные сообщения
                    all_message_ids = set(pending_messages)
                    
                    # Добавляем стандартный диапазон
                    max_id = message_id
                    start_id = max(1, max_id - 10)  # Берем только последние 10 сообщений
                    all_message_ids.update(range(start_id, max_id + 1))
                    
                    # Сортируем все ID в порядке возрастания (от старых к новым)
                    message_ids = sorted(list(all_message_ids))
                    
                    # Очищаем список отложенных сообщений для этого канала
                    self._pending_messages[next_channel] = []
                else:
                    # Стандартный диапазон ID сообщений для пересылки
                    max_id = message_id
                    start_id = max(1, max_id - 10)  # Берем только последние 10 сообщений
                    message_ids = list(range(start_id, max_id + 1))

                # Определяем диапазон ID сообщений для пересылки
                max_id = message_id
                start_id = max(1, max_id - 10)  # Берем только последние 10 сообщений
                
                # Создаем список ID сообщений в порядке от старых к новым
                message_ids = list(range(start_id, max_id + 1))
                
                # Инициализируем временный кэш недоступных сообщений
                if not hasattr(self.context, '_temp_unavailable_messages'):
                    self.context._temp_unavailable_messages = {}
                
                # Очищаем устаревшие записи (старше 30 минут)
                current_time = now
                self.context._temp_unavailable_messages = {k: v for k, v in self.context._temp_unavailable_messages.items() 
                                                    if current_time - v < 1800}  # 30 минут
                
                logger.info(f"Параллельная проверка и последовательная пересылка сообщений из канала {next_channel}")
                
                # 1. Сначала параллельно проверяем все сообщения
                check_tasks = []
                for msg_id in message_ids:
                    msg_key = f"{next_channel}:{msg_id}"
                    if msg_key in self.context._temp_unavailable_messages:
                        logger.debug(f"Пропуск недавно недоступного сообщения {msg_id} из канала {next_channel}")
                        continue
                    
                    # Создаем задачу проверки доступности сообщения
                    check_tasks.append(self._check_message(next_channel, msg_id))
                
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
                                msg_id = message_ids[i]
                                self.context._temp_unavailable_messages[f"{next_channel}:{msg_id}"] = current_time
                
                # 2. Теперь отправляем доступные сообщения последовательно в нужном порядке
                # Сортируем сообщения по ID (от старых к новым)
                available_messages.sort(key=lambda x: x['message_id'])
                
                # Счетчики для статистики
                forwarded_count = 0
                skipped_count = len(message_ids) - len(available_messages)
                error_count = 0
                
                # Отправляем сообщения последовательно
                for message_info in sorted(available_messages, key=lambda x: x['message_id']):
                    channel_id = message_info['channel_id']
                    msg_id = message_info['message_id']
                    
                    # Пробуем переслать сообщение
                    success = False
                    try:
                        success = await self._forward_specific_message(channel_id, msg_id)
                        if success:
                            forwarded_count += 1
                        else:
                            # Сообщение не удалось переслать, добавляем в кэш недоступных
                            self.context._temp_unavailable_messages[f"{channel_id}:{msg_id}"] = current_time
                            skipped_count += 1
                    except Exception as e:
                        error_count += 1
                        logger.error(f"Ошибка при пересылке сообщения {msg_id} из канала {channel_id}: {e}")
                        # Добавляем в кэш недоступных, если произошла ошибка
                        self.context._temp_unavailable_messages[f"{channel_id}:{msg_id}"] = current_time
                
                # Обновляем время последней пересылки
                now = datetime.now().timestamp()
                self._channel_last_post[next_channel] = now
                self._last_global_post_time = now
                self._last_processed_channel = next_channel
                
                # Логируем результаты и предсказываем следующую пересылку
                next_global_time = now + self.interval
                next_time_str = datetime.fromtimestamp(next_global_time).strftime('%H:%M:%S')
                
                # Проверяем следующий канал для логирования
                next_channel_for_log = None
                special_interval = 0
                
                if next_channel in channel_intervals:
                    interval_data = channel_intervals.get(next_channel, {})
                    next_defined = interval_data.get("next_channel")
                    if next_defined in source_channels:
                        next_channel_for_log = next_defined
                        special_interval = interval_data.get("interval", self.interval)
                
                if not next_channel_for_log:
                    current_idx = source_channels.index(next_channel)
                    next_idx = (current_idx + 1) % len(source_channels)
                    next_channel_for_log = source_channels[next_idx]
                
                # Рассчитываем время следующей пересылки
                next_time = now + (special_interval if special_interval > 0 else self.interval)
                next_time_str = datetime.fromtimestamp(next_time).strftime('%H:%M:%S')
                
                # Форматируем интервал для вывода
                interval_display = f"{special_interval // 60}м" if special_interval > 0 else f"{self.interval // 60}м"
                
                logger.info(f"Переслано {forwarded_count} сообщений из канала {next_channel} (пропущено: {skipped_count}, ошибок: {error_count}). "
                        f"Следующий канал {next_channel_for_log} через {interval_display} (в {next_time_str}).")
                
            except asyncio.CancelledError:
                logger.info("Задача рассылки отменена")
                break
            except Exception as e:
                logger.error(f"Ошибка в периодической рассылке: {e}")
                await asyncio.sleep(60)

    # Добавляем вспомогательные методы для улучшения структуры кода
    async def _create_forward_task(self, channel_id, msg_id, current_time):
        """Создает задачу для пересылки одного сообщения"""
        try:
            success = await self.context._forward_message(channel_id, msg_id)
            return (success, msg_id)
        except Exception as e:
            error_text = str(e).lower()
            if "message to forward not found" in error_text or "message can't be forwarded" in error_text:
                # Добавляем в кэш недоступных сообщений
                self.context._temp_unavailable_messages[f"{channel_id}:{msg_id}"] = current_time
            
            if "message to forward not found" not in error_text and "message can't be forwarded" not in error_text:
                logger.error(f"Ошибка при пересылке сообщения {msg_id} из канала {channel_id}: {e}")
            
            return (False, msg_id)

    def _log_forwarding_results(self, channel, forwarded_count, skipped_count, error_count, channel_intervals):
        """Логирует результаты пересылки и предсказывает следующую пересылку"""
        now = datetime.now().timestamp()
        source_channels = self.context.config.source_channels
        
        # Находим индекс текущего канала
        try:
            current_idx = source_channels.index(channel)
        except ValueError:
            current_idx = -1
        
        # Определяем следующий канал по порядку
        next_channel = None
        next_interval = self.interval
        next_time_str = "неизвестно"
        
        # Проверяем специальный интервал для текущего канала
        if channel in channel_intervals:
            interval_data = channel_intervals.get(channel, {})
            next_defined = interval_data.get("next_channel")
            if next_defined in source_channels:
                next_channel = next_defined
                next_interval = interval_data.get("interval", self.interval)
        
        # Если нет специального интервала, берем следующий канал по порядку
        if next_channel is None and current_idx != -1:
            next_idx = (current_idx + 1) % len(source_channels)
            next_channel = source_channels[next_idx]
        
        # Рассчитываем время следующей пересылки
        next_time = now + next_interval
        next_time_str = datetime.fromtimestamp(next_time).strftime('%H:%M:%S')
        
        # Форматируем интервал для вывода
        if next_interval >= 3600:
            interval_display = f"{next_interval // 3600}ч"
        else:
            interval_display = f"{next_interval // 60}м"
        
        logger.info(f"Переслано {forwarded_count} сообщений из канала {channel} (пропущено: {skipped_count}, ошибок: {error_count}). "
                f"Следующий канал {next_channel} через {interval_display} (в {next_time_str}).")

    # Добавим вспомогательный метод для работы с задачами пересылки
    async def _forward_message_task(self, channel_id, message_id, timestamp):
        """Вспомогательный метод для асинхронной пересылки сообщений"""
        try:
            success = await self.context._forward_message(channel_id, message_id)
            return (success, message_id)
        except Exception as e:
            # Если сообщение недоступно, добавляем его в кэш
            error_text = str(e).lower()
            if "message to forward not found" in error_text or "message can't be forwarded" in error_text:
                msg_key = f"{channel_id}:{message_id}"
                self.context._temp_unavailable_messages[msg_key] = timestamp
            raise e
                
class BotContext:
    """Context class that maintains current bot state"""
    
    def __init__(self, bot, config):
        self.bot = bot
        self.config = config
        self.state: BotState = IdleState(self)
    
    async def start(self) -> None:
        await self.state.start()
    
    async def stop(self) -> None:
        await self.state.stop()
    
    async def handle_message(self, channel_id: str, message_id: int) -> None:
        await self.state.handle_message(channel_id, message_id)
    
    async def _forward_message(self, channel_id: str, message_id: int) -> bool:
        """Forward a message to all target chats with improved reliability and speed"""
        success = False
        target_chats = await Repository.get_target_chats()
        
        if not target_chats:
            logger.warning("Нет целевых чатов для пересылки")
            return False

        # Используем временный кэш недоступных сообщений только для текущей сессии
        # с ограниченным временем жизни (30 минут)
        current_time = datetime.now().timestamp()
        if not hasattr(self, '_temp_unavailable_messages'):
            self._temp_unavailable_messages = {}
        
        # Очищаем устаревшие записи (старше 30 минут)
        self._temp_unavailable_messages = {k: v for k, v in self._temp_unavailable_messages.items() 
                                        if current_time - v < 1800}  # 30 минут
        
        msg_key = f"{channel_id}:{message_id}"
        if msg_key in self._temp_unavailable_messages:
            logger.debug(f"Пропуск недавно недоступного сообщения {message_id} из канала {channel_id}")
            return False

        # Пробуем получить сообщение только один раз для всех чатов
        try:
            await self.bot.get_messages(channel_id, message_id)
        except Exception as e:
            if "message to forward not found" in str(e) or "message not found" in str(e):
                logger.debug(f"Сообщение {message_id} не найдено в канале {channel_id}")
                self._temp_unavailable_messages[msg_key] = current_time
                return False
        
        for chat_id in target_chats:
            if str(chat_id) == channel_id:
                logger.info(f"Пропускаю пересылку в исходный канал {chat_id}")
                continue
                
            try:
                await self.bot.forward_message(
                    chat_id=chat_id,
                    from_chat_id=channel_id,
                    message_id=message_id
                )
                await Repository.log_forward(message_id)
                success = True
                logger.debug(f"Сообщение {message_id} успешно переслано в {chat_id}")
            except Exception as e:
                error_text = str(e).lower()
                # Временно помечаем сообщение как недоступное
                if "message to forward not found" in error_text or "message can't be forwarded" in error_text:
                    logger.debug(f"Сообщение {message_id} недоступно для пересылки в {chat_id}")
                    self._temp_unavailable_messages[msg_key] = current_time
                elif "bot was blocked by the user" in error_text:
                    logger.warning(f"Бот заблокирован в чате {chat_id}")
                elif "chat not found" in error_text:
                    logger.warning(f"Чат {chat_id} не найден")
                else:
                    logger.error(f"Ошибка при пересылке в {chat_id}: {e}")

        return success
    
    async def _notify_owner(self, message: str):
        """Send notification to bot owner (for compatibility)"""
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