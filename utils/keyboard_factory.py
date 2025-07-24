from aiogram.utils.keyboard import InlineKeyboardBuilder
from typing import Dict, List, Any

class KeyboardFactory:
    """Factory Pattern implementation for creating keyboards"""
    
    @staticmethod
    def create_main_keyboard(running: bool = False, auto_forward: bool = False) -> Any:
        """Create main menu keyboard"""
        kb = InlineKeyboardBuilder()
        kb.button(
            text="🔄 Начать пересылку" if not running else "⏹ Остановить пересылку",
            callback_data="toggle_forward"
        )
        kb.button(
            text=f"⚡ Автопересылка: {'ВКЛ' if auto_forward else 'ВЫКЛ'}",
            callback_data="toggle_auto_forward"
        )
        kb.button(text="⏱️ Установить интервал", callback_data="interval_menu")
        kb.button(text="📊 Показать статистику", callback_data="stats")
        kb.button(text="⚙️ Управление каналами", callback_data="channels")
        kb.button(text="💬 Список целевых чатов", callback_data="list_chats")
        kb.button(text="🤖 Клонировать бота", callback_data="clone_bot")
        kb.button(text="👥 Управление клонами", callback_data="manage_clones")
        kb.adjust(2)
        return kb.as_markup()

    @staticmethod
    async def create_interval_keyboard() -> Any:
        """Create interval selection keyboard"""
        kb = InlineKeyboardBuilder()
        intervals = [
            ("5м", 300), ("15м", 900), ("30м", 1800),
            ("1ч", 3600), ("2ч", 7200), ("6ч", 21600), 
            ("12ч", 43200), ("24ч", 86400)
        ]
        for label, seconds in intervals:
            kb.button(text=label, callback_data=f"interval_{seconds}")
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(4)
        return kb.as_markup()

    @staticmethod
    def create_chat_list_keyboard(chats: Dict[int, str]) -> Any:
        """Create chat list keyboard with remove buttons"""
        kb = InlineKeyboardBuilder()
        for chat_id, title in chats.items():
            kb.button(
                text=f"❌ Удалить {title}",
                callback_data=f"remove_{chat_id}"
            )
        kb.button(text="Назад", callback_data="back_to_main")
        kb.adjust(1)
        return kb.as_markup()

    @staticmethod
    def create_channel_interval_keyboard(channels: List[str], page: int = 0, channel_info: Dict[str, str] = None, current_intervals: Dict[str, Dict[str, Any]] = None, per_page: int = 5) -> Any:
        """Create keyboard for setting intervals between channels with pagination and real channel names"""
        kb = InlineKeyboardBuilder()
        
        # Создаем список пар каналов
        channel_pairs = []
        for i, channel in enumerate(channels):
            if i < len(channels) - 1:
                next_channel = channels[i + 1]
                channel_pairs.append((channel, next_channel))
        
        if not channel_pairs:
            kb.button(text="Нет доступных пар каналов", callback_data="dummy")
            kb.button(text="Назад", callback_data="channels")
            return kb.as_markup()
        
        # Пагинация
        total_pairs = len(channel_pairs)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total_pairs)
        current_pairs = channel_pairs[start_idx:end_idx]
        
        # Добавляем кнопки для текущей страницы
        for channel1, channel2 in current_pairs:
            # Получаем названия каналов
            if channel_info:
                name1 = channel_info.get(channel1, channel1)
                name2 = channel_info.get(channel2, channel2)
            else:
                name1 = channel1
                name2 = channel2
            
            # Сокращаем названия каналов для красивого отображения в кнопке
            if name1 == channel1 and channel1.startswith('-100'):
                display_name1 = f"ID:{channel1[-6:]}"  # Короткий ID
            else:
                display_name1 = name1[:10] + "..." if len(name1) > 10 else name1
                
            if name2 == channel2 and channel2.startswith('-100'):
                display_name2 = f"ID:{channel2[-6:]}"  # Короткий ID
            else:
                display_name2 = name2[:10] + "..." if len(name2) > 10 else name2
            
            # Проверяем установленный интервал для этой пары
            interval_text = ""
            if current_intervals and channel1 in current_intervals:
                interval_data = current_intervals.get(channel1, {})
                if interval_data.get("next_channel") == channel2:
                    interval_seconds = interval_data.get("interval", 0)
                    if interval_seconds > 0:
                        if interval_seconds >= 3600:
                            interval_text = f" ({interval_seconds//3600}ч)"
                        else:
                            interval_text = f" ({interval_seconds//60}м)"
                    else:
                        interval_text = " (не уст.)"
                else:
                    interval_text = " (не уст.)"
            else:
                interval_text = " (не уст.)"
            
            kb.button(
                text=f"⏱️ {display_name1} → {display_name2}{interval_text}",
                callback_data=f"interval_between_{channel1}_{channel2}"
            )
        
        # Навигационные кнопки
        navigation_buttons = []
        
        # Кнопка "Предыдущая страница"
        if page > 0:
            navigation_buttons.append(("⬅️ Назад", f"channel_intervals_page_{page - 1}"))
        
        # Показать текущую страницу
        total_pages = (total_pairs + per_page - 1) // per_page
        if total_pages > 1:
            navigation_buttons.append((f"📄 {page + 1}/{total_pages}", "dummy"))
        
        # Кнопка "Следующая страница"
        if end_idx < total_pairs:
            navigation_buttons.append(("➡️ Далее", f"channel_intervals_page_{page + 1}"))
        
        # Добавляем навигационные кнопки
        for text, callback in navigation_buttons:
            kb.button(text=text, callback_data=callback)
        
        kb.button(text="🔙 К каналам", callback_data="channels")
        
        # Настройка раскладки
        kb.adjust(1)  # Основные кнопки интервалов по одной в ряд
        
        # Навигационные кнопки в один ряд
        if len(navigation_buttons) > 0:
            kb.adjust(1, len(navigation_buttons), 1)  # 1 кнопка интервалов, навигация в ряд, 1 кнопка назад
        
        return kb.as_markup()

    @staticmethod
    def create_channel_interval_options(channel1: str, channel2: str) -> Any:
        """Create keyboard with interval options between two channels"""
        kb = InlineKeyboardBuilder()
        intervals = [
            ("1м", 60), ("5м", 300), ("10м", 600),
            ("15м", 900), ("30м", 1800), ("1ч", 3600)
        ]
        
        for label, seconds in intervals:
            kb.button(
                text=label, 
                callback_data=f"set_interval_{channel1}_{channel2}_{seconds}"
            )
        
        kb.button(text="Назад", callback_data="channel_intervals")
        kb.adjust(3)
        return kb.as_markup()

    @staticmethod
    def create_channel_management_keyboard(channels: List[str]) -> Any:
        """Create simplified channel management keyboard"""
        kb = InlineKeyboardBuilder()
        
        # Основные действия
        kb.button(text="➕ Добавить канал", callback_data="add_channel")
        
        if channels:
            kb.button(text="❌ Удалить канал", callback_data="remove_channel_menu")
            kb.button(text="↕️ Изменить порядок", callback_data="reorder_channels")
        
        if len(channels) >= 2:
            kb.button(text="⏱️ Интервалы между каналами", callback_data="channel_intervals")
        
        kb.button(text="🔙 Назад", callback_data="back_to_main")
        kb.adjust(2, 1, 1, 1)  # 2 кнопки в первом ряду, остальные по одной
        return kb.as_markup()

    @staticmethod 
    def create_channel_removal_keyboard(channels: List[str], page: int = 0, channel_info: Dict[str, str] = None, per_page: int = 5) -> Any:
        """Create keyboard for channel removal with pagination and real channel names"""
        kb = InlineKeyboardBuilder()
        
        if not channels:
            kb.button(text="Нет каналов для удаления", callback_data="dummy")
            kb.button(text="🔙 К каналам", callback_data="channels")
            return kb.as_markup()
        
        # Пагинация
        total_channels = len(channels)
        start_idx = page * per_page
        end_idx = min(start_idx + per_page, total_channels)
        current_channels = channels[start_idx:end_idx]
        
        # Добавляем кнопки для текущей страницы
        for channel in current_channels:
            # Получаем название канала
            if channel_info and channel in channel_info:
                channel_name = channel_info[channel]
            else:
                channel_name = channel
            
            # Красивое отображение названия канала
            if channel_name == channel and channel.startswith('-100'):
                # Если название не получено и это ID канала
                display_name = f"ID: {channel[-8:]}"
            else:
                # Используем реальное название канала
                display_name = channel_name if len(channel_name) <= 25 else channel_name[:22] + "..."
            
            kb.button(
                text=f"❌ {display_name}",
                callback_data=f"remove_channel_{channel}"
            )
        
        # Навигационные кнопки
        navigation_buttons = []
        
        # Кнопка "Предыдущая страница"
        if page > 0:
            navigation_buttons.append(("⬅️", f"remove_channel_page_{page - 1}"))
        
        # Показать текущую страницу
        total_pages = (total_channels + per_page - 1) // per_page
        if total_pages > 1:
            navigation_buttons.append((f"{page + 1}/{total_pages}", "dummy"))
        
        # Кнопка "Следующая страница"  
        if end_idx < total_channels:
            navigation_buttons.append(("➡️", f"remove_channel_page_{page + 1}"))
        
        # Добавляем навигационные кнопки, если их больше одной
        if len(navigation_buttons) > 1:
            for text, callback in navigation_buttons:
                kb.button(text=text, callback_data=callback)
        
        kb.button(text="🔙 К каналам", callback_data="channels")
        
        # Настройка раскладки
        kb.adjust(1)  # Каналы по одному в ряд
        if len(navigation_buttons) > 1:
            kb.adjust(1, len(navigation_buttons), 1)  # Каналы, навигация, кнопка назад
        
        return kb.as_markup()