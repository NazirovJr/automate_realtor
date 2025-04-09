import logging
from datetime import datetime
import re

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.sql import text
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes, CallbackQueryHandler, \
    ConversationHandler

from telegram.error import TelegramError

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация
TOKEN = "7866153858:AAFMpL-XejNmlJdkgc9D6ExC1H6hkQeBPvY"
DATABASE_URL = "postgresql://postgres:postgres@localhost/krisha"

# Инициализация базы данных
Base = declarative_base()
engine = create_engine(DATABASE_URL)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


# Определение моделей базы данных для пользовательских настроек
class User(Base):
    __tablename__ = "telegram_users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(Integer, unique=True, index=True)
    username = Column(String, nullable=True)
    first_name = Column(String)
    last_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    filters = relationship("UserFilter", back_populates="user", cascade="all, delete-orphan")
    notifications = relationship("NotificationSetting", back_populates="user", cascade="all, delete-orphan")


class UserFilter(Base):
    __tablename__ = "user_filters"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("telegram_users.id"))
    year_min = Column(Integer, default=1977)
    year_max = Column(Integer, default=2000)
    districts = Column(JSON, default=["Алмалинский", "Бостандыкский", "Медеуский", "Жетысуский"])
    not_first_floor = Column(Boolean, default=True)
    not_last_floor = Column(Boolean, default=True)
    min_floor = Column(Integer, nullable=True)
    max_floor = Column(Integer, nullable=True)
    rooms_min = Column(Integer, nullable=True)
    rooms_max = Column(Integer, nullable=True)
    price_min = Column(Integer, nullable=True)
    price_max = Column(Integer, nullable=True)
    area_min = Column(Integer, nullable=True)
    area_max = Column(Integer, nullable=True)
    max_market_price_percent = Column(Float, default=100.0)

    user = relationship("User", back_populates="filters")


class NotificationSetting(Base):
    __tablename__ = "notification_settings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("telegram_users.id"))
    frequency_type = Column(String, default="daily")  # daily, hourly, custom
    hour = Column(Integer, default=10)  # По умолчанию - 10 утра
    minute = Column(Integer, default=0)
    interval_hours = Column(Integer, default=1)  # Для hourly режима
    enabled = Column(Boolean, default=True)
    last_sent_at = Column(DateTime, nullable=True)

    user = relationship("User", back_populates="notifications")


# Создание таблиц
Base.metadata.create_all(bind=engine)

# Состояния для ConversationHandler
(
    MAIN_MENU,
    FILTER_MENU,
    YEAR_MIN,
    YEAR_MAX,
    DISTRICTS,
    FLOOR_SETTINGS,
    MIN_FLOOR,
    MAX_FLOOR,
    ROOMS,
    PRICE_RANGE,
    AREA_RANGE,
    MARKET_PERCENT,
    NOTIFICATION_MENU,
    NOTIFICATION_TYPE,
    NOTIFICATION_TIME,
    NOTIFICATION_INTERVAL
) = range(16)


# Клавиатуры
def get_main_keyboard():
    keyboard = [
        [KeyboardButton("⚙️ Настройка фильтров")],
        [KeyboardButton("🔔 Настройка уведомлений")],
        [KeyboardButton("🔍 Поиск объявлений")],
        [KeyboardButton("📊 Статистика")],
        [KeyboardButton("ℹ️ Помощь")]
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_filter_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🏢 Год постройки", callback_data="filter_year")],
        [InlineKeyboardButton("🏙️ Районы", callback_data="filter_districts")],
        [InlineKeyboardButton("🔢 Этажи", callback_data="filter_floors")],
        [InlineKeyboardButton("🚪 Комнаты", callback_data="filter_rooms")],
        [InlineKeyboardButton("💰 Диапазон цен", callback_data="filter_price")],
        [InlineKeyboardButton("📏 Площадь", callback_data="filter_area")],
        [InlineKeyboardButton("📉 % от рыночной цены", callback_data="filter_market_percent")],
        [InlineKeyboardButton("✅ Сохранить и выйти", callback_data="save_filters")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_notification_menu_keyboard():
    keyboard = [
        [InlineKeyboardButton("🕒 Тип уведомлений", callback_data="notif_type")],
        [InlineKeyboardButton("⏰ Время уведомлений", callback_data="notif_time")],
        [InlineKeyboardButton("⏱️ Интервал (для hourly)", callback_data="notif_interval")],
        [InlineKeyboardButton("✅ Сохранить и выйти", callback_data="save_notifications")]
    ]
    return InlineKeyboardMarkup(keyboard)


def get_notification_type_keyboard():
    keyboard = [
        [InlineKeyboardButton("📅 Ежедневно", callback_data="type_daily")],
        [InlineKeyboardButton("🕐 Каждый час", callback_data="type_hourly")],
        [InlineKeyboardButton("↩️ Назад", callback_data="back_to_notif_menu")]
    ]
    return InlineKeyboardMarkup(keyboard)


# Вспомогательные функции
def extract_year_from_description(description):
    """Извлекает год постройки из описания объявления."""
    if not description:
        return None

    year_patterns = [
        r'год\s+постройки[:\s]+(\d{4})',
        r'построен[а]?\s+в\s+(\d{4})',
        r'(\d{4})\s+год[а]?\s+постройки',
        r'дом\s+(\d{4})\s+год[а]?'
    ]

    for pattern in year_patterns:
        match = re.search(pattern, description.lower())
        if match:
            year = int(match.group(1))
            if 1900 <= year <= 2023:
                return year

    return None


def extract_floor_info(description):
    """Извлекает информацию об этаже и общем количестве этажей из описания."""
    if not description:
        return None, None

    floor_pattern = r'(\d+)(?:\s*[-\/из]\s*|\s+этаж\s+из\s+)(\d+)'
    match = re.search(floor_pattern, description.lower())

    if match:
        return int(match.group(1)), int(match.group(2))

    return None, None


def get_district_from_address(address):
    """Определяет район по адресу."""
    if not address:
        return None

    districts = {
        "Алмалинский": ["алмалинск", "алматы", "абая", "достык", "жибек жолы"],
        "Бостандыкский": ["бостандык", "тимирязев", "розыбакиев", "аль-фараби"],
        "Медеуский": ["медеу", "кок-тобе", "достык", "горный"],
        "Жетысуский": ["жетысу", "кульджинск", "палладиум"]
    }

    address_lower = address.lower()

    for district, keywords in districts.items():
        for keyword in keywords:
            if keyword in address_lower:
                return district

    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает команду /start."""
    user = update.effective_user

    # Сохраняем пользователя в базе данных, если его еще нет
    db = Session()
    try:
        db_user = db.query(User).filter(User.telegram_id == user.id).first()
        if not db_user:
            db_user = User(
                telegram_id=user.id,
                username=user.username,
                first_name=user.first_name,
                last_name=user.last_name
            )
            db.add(db_user)

            # Создаем настройки фильтров по умолчанию
            default_filter = UserFilter(user=db_user)
            db.add(default_filter)

            # Создаем настройки уведомлений по умолчанию
            default_notification = NotificationSetting(user=db_user)
            db.add(default_notification)

            db.commit()

        await update.message.reply_text(
            f"Привет, {user.first_name}! Я бот для поиска выгодных предложений недвижимости. "
            "Настрой фильтры под свои предпочтения, и я буду уведомлять тебя о новых объявлениях.",
            reply_markup=get_main_keyboard()
        )
        return MAIN_MENU
    finally:
        db.close()


async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор в главном меню."""
    text = update.message.text
    if text == "⚙️ Настройка фильтров":
        await update.message.reply_text(
            "Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    elif text == "🔔 Настройка уведомлений":
        # Показываем меню настройки уведомлений
        await update.message.reply_text(
            "Настройка уведомлений:",
            reply_markup=get_notification_menu_keyboard()
        )
        return NOTIFICATION_MENU
    elif text == "🔍 Поиск объявлений":
        # Выполняем поиск объявлений
        await search_properties(update, context)
        return MAIN_MENU
    elif text == "📊 Статистика":
        # Показываем статистику
        await show_statistics(update, context)
        return MAIN_MENU
    elif text == "ℹ️ Помощь":
        # Показываем помощь
        await update.message.reply_text(
            "Этот бот поможет вам найти выгодные предложения недвижимости по заданным параметрам.\n\n"
            "Как пользоваться:\n"
            "1. Настройте фильтры (год постройки, район, этажность, цена и т.д.)\n"
            "2. Настройте уведомления (ежедневно или каждый час)\n"
            "3. Получайте уведомления о новых объявлениях, соответствующих вашим критериям\n\n"
            "Используйте кнопки внизу для навигации."
        )
        return MAIN_MENU


# Обработчики для настройки фильтров
async def handle_filter_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор в меню фильтров."""
    query = update.callback_query
    await query.answer()

    print("sdjkasdlksad;asdks;aldkas;ldsa;dlasd;lskd;lsadka;ldsd;lkasd;askd;alsdsa;ldsa")
    data = query.data

    if data == "filter_year":
        await query.edit_message_text(
            "Введите минимальный год постройки (например, 1977):"
        )
        return YEAR_MIN
    elif data == "filter_districts":
        # Получаем текущие настройки пользователя
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            user_filter = user.filters[0] if user and user.filters else None

            current_districts = user_filter.districts if user_filter else ["Алмалинский", "Бостандыкский", "Медеуский",
                                                                           "Жетысуский"]

            # Создаем клавиатуру с выбором районов
            districts = ["Алмалинский", "Бостандыкский", "Медеуский", "Жетысуский"]
            keyboard = []

            for district in districts:
                status = "✅" if district in current_districts else "❌"
                keyboard.append([InlineKeyboardButton(f"{status} {district}", callback_data=f"district_{district}")])

            keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="districts_done")])

            await query.edit_message_text(
                "Выберите районы для поиска:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return DISTRICTS
        finally:
            db.close()
    elif data == "filter_floors":
        await query.edit_message_text(
            "Настройки этажности:\n\n"
            "Выберите опции:",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("Не первый этаж", callback_data="toggle_not_first")],
                [InlineKeyboardButton("Не последний этаж", callback_data="toggle_not_last")],
                [InlineKeyboardButton("Указать диапазон этажей", callback_data="set_floor_range")],
                [InlineKeyboardButton("✅ Готово", callback_data="floors_done")]
            ])
        )
        return FLOOR_SETTINGS
    elif data == "filter_rooms":
        await query.edit_message_text(
            "Введите количество комнат через дефис (например, 1-3 для поиска от 1 до 3 комнат):"
        )
        return ROOMS
    elif data == "filter_price":
        await query.edit_message_text(
            "Введите диапазон цен через дефис в миллионах тенге (например, 15-30 для поиска от 15 до 30 млн):"
        )
        return PRICE_RANGE
    elif data == "filter_area":
        await query.edit_message_text(
            "Введите диапазон площади в квадратных метрах через дефис (например, 40-80):"
        )
        return AREA_RANGE
    elif data == "filter_market_percent":
        await query.edit_message_text(
            "Введите максимальный процент от рыночной цены (например, 90 для поиска объявлений дешевле на 10% от рынка):"
        )
        return MARKET_PERCENT
    elif data == "save_filters":
        # Возвращаемся в главное меню
        await query.edit_message_text(
            "Настройки фильтров сохранены!"
        )
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return MAIN_MENU
    elif data.startswith("district_"):
        # Обрабатываем выбор района
        district = data.replace("district_", "")

        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            user_filter = user.filters[0] if user and user.filters else None

            if user_filter:
                current_districts = user_filter.districts or []

                if district in current_districts:
                    current_districts.remove(district)
                else:
                    current_districts.append(district)

                user_filter.districts = current_districts
                db.commit()

            # Обновляем клавиатуру
            districts = ["Алмалинский", "Бостандыкский", "Медеуский", "Жетысуский"]
            keyboard = []

            for d in districts:
                status = "✅" if d in current_districts else "❌"
                keyboard.append([InlineKeyboardButton(f"{status} {d}", callback_data=f"district_{d}")])

            keyboard.append([InlineKeyboardButton("✅ Готово", callback_data="districts_done")])

            await query.edit_message_text(
                "Выберите районы для поиска:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
            return DISTRICTS
        finally:
            db.close()
    elif data == "districts_done":
        await query.edit_message_text(
            "Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    elif data.startswith("toggle_"):
        option = data.replace("toggle_", "")

        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            user_filter = user.filters[0] if user and user.filters else None

            if user_filter:
                if option == "not_first":
                    user_filter.not_first_floor = not user_filter.not_first_floor
                elif option == "not_last":
                    user_filter.not_last_floor = not user_filter.not_last_floor

                db.commit()

            not_first = user_filter.not_first_floor if user_filter else True
            not_last = user_filter.not_last_floor if user_filter else True

            await query.edit_message_text(
                "Настройки этажности:\n\n"
                "Выберите опции:",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(f"{'✅' if not_first else '❌'} Не первый этаж",
                                          callback_data="toggle_not_first")],
                    [InlineKeyboardButton(f"{'✅' if not_last else '❌'} Не последний этаж",
                                          callback_data="toggle_not_last")],
                    [InlineKeyboardButton("Указать диапазон этажей", callback_data="set_floor_range")],
                    [InlineKeyboardButton("✅ Готово", callback_data="floors_done")]
                ])
            )
            return FLOOR_SETTINGS
        finally:
            db.close()
    elif data == "set_floor_range":
        await query.edit_message_text(
            "Введите минимальный этаж (или 0, если не важно):"
        )
        return MIN_FLOOR
    elif data == "floors_done":
        await query.edit_message_text(
            "Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU


async def handle_year_min(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод минимального года постройки."""
    try:
        year = int(update.message.text)
        if 1900 <= year <= 2025:
            # Сохраняем значение в контексте для последующего использования
            context.user_data["year_min"] = year

            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.filters:
                    user.filters[0].year_min = year
                    db.commit()
            finally:
                db.close()

            await update.message.reply_text(
                "Введите максимальный год постройки (например, 2000):"
            )
            return YEAR_MAX
        else:
            await update.message.reply_text(
                "Пожалуйста, введите корректный год от 1900 до 2025:"
            )
            return YEAR_MIN
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректный год в виде числа:"
        )
        return YEAR_MIN


async def handle_year_max(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод максимального года постройки."""
    try:
        year = int(update.message.text)
        year_min = context.user_data.get("year_min", 1977)

        if 1900 <= year <= 2025 and year >= year_min:
            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.filters:
                    user.filters[0].year_max = year
                    db.commit()
            finally:
                db.close()

            await update.message.reply_text(
                "Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()  # Добавьте клавиатуру
            )
            return FILTER_MENU  # Возвращаемся в меню фильтров
        else:
            await update.message.reply_text(
                f"Пожалуйста, введите корректный год от {year_min} до 2023:"
            )
            return YEAR_MAX
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректный год в виде числа:"
        )
        return YEAR_MAX


async def handle_min_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод минимального этажа."""
    try:
        floor = int(update.message.text)
        if floor >= 0:
            context.user_data["min_floor"] = floor

            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.filters:
                    user.filters[0].min_floor = floor if floor > 0 else None
                    db.commit()
            finally:
                db.close()

            await update.message.reply_text(
                "Введите максимальный этаж (или 0, если не важно):"
            )
            return MAX_FLOOR
        else:
            await update.message.reply_text(
                "Пожалуйста, введите корректное число от 0 и выше:"
            )
            return MIN_FLOOR
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректное число:"
        )
        return MIN_FLOOR


async def handle_max_floor(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод максимального этажа."""
    try:
        floor = int(update.message.text)
        min_floor = context.user_data.get("min_floor", 0)

        if floor >= 0 and (floor >= min_floor or floor == 0):
            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.filters:
                    user.filters[0].max_floor = floor if floor > 0 else None
                    db.commit()
            finally:
                db.close()

            await update.message.reply_text(
                "Настройки этажности обновлены. Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()
            )
            return FILTER_MENU
        else:
            await update.message.reply_text(
                f"Пожалуйста, введите корректное число от {min_floor} и выше (или 0, если не важно):"
            )
            return MAX_FLOOR
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректное число:"
        )
        return MAX_FLOOR


async def handle_rooms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод количества комнат."""
    text = update.message.text

    # Паттерн для диапазона (например, "1-3")
    range_pattern = r'^(\d+)[-–](\d+)$'
    # Паттерн для одного значения (например, "2")
    single_pattern = r'^(\d+)$'

    range_match = re.match(range_pattern, text)
    single_match = re.match(single_pattern, text)

    if range_match:
        min_rooms = int(range_match.group(1))
        max_rooms = int(range_match.group(2))

        if min_rooms > max_rooms:
            min_rooms, max_rooms = max_rooms, min_rooms

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.filters:
                user.filters[0].rooms_min = min_rooms
                user.filters[0].rooms_max = max_rooms
                db.commit()
        finally:
            db.close()

        await update.message.reply_text(
            f"Настройка количества комнат: от {min_rooms} до {max_rooms}. Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    elif single_match:
        rooms = int(single_match.group(1))

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.filters:
                user.filters[0].rooms_min = rooms
                user.filters[0].rooms_max = rooms
                db.commit()
        finally:
            db.close()

        await update.message.reply_text(
            f"Настройка количества комнат: ровно {rooms}. Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    else:
        await update.message.reply_text(
            "Пожалуйста, введите корректное количество комнат (например, '2' или '1-3'):"
        )
        return ROOMS


async def handle_price_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод диапазона цен."""
    text = update.message.text

    # Паттерн для диапазона (например, "15-30")
    range_pattern = r'^(\d+(?:\.\d+)?)[-–](\d+(?:\.\d+)?)$'
    match = re.match(range_pattern, text)

    if match:
        min_price = float(match.group(1))
        max_price = float(match.group(2))

        if min_price > max_price:
            min_price, max_price = max_price, min_price

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.filters:
                user.filters[0].price_min = int(min_price)
                user.filters[0].price_max = int(max_price)
                db.commit()
        finally:
            db.close()

        await update.message.reply_text(
            f"Настройка диапазона цен: от {int(min_price)} до {int(max_price)} млн тенге. Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    else:
        await update.message.reply_text(
            "Пожалуйста, введите корректный диапазон цен (например, '15000000-3000000'):"
        )
        return PRICE_RANGE


async def handle_area_range(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод диапазона площади."""
    text = update.message.text

    # Паттерн для диапазона (например, "40-80")
    range_pattern = r'^(\d+(?:\.\d+)?)[-–](\d+(?:\.\d+)?)$'
    match = re.match(range_pattern, text)

    if match:
        min_area = float(match.group(1))
        max_area = float(match.group(2))

        if min_area > max_area:
            min_area, max_area = max_area, min_area

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.filters:
                user.filters[0].area_min = int(min_area)
                user.filters[0].area_max = int(max_area)
                db.commit()
        finally:
            db.close()

        await update.message.reply_text(
            f"Настройка диапазона площади: от {int(min_area)} до {int(max_area)} кв.м. Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    else:
        await update.message.reply_text(
            "Пожалуйста, введите корректный диапазон площади (например, '40-80'):"
        )
        return AREA_RANGE


async def handle_market_percent(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод процента от рыночной цены."""
    try:
        percent = float(update.message.text)
        if 0 < percent <= 100:
            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.filters:
                    user.filters[0].max_market_price_percent = percent
                    db.commit()
            finally:
                db.close()

            await update.message.reply_text(
                f"Настройка процента от рыночной цены: {percent}%. Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()
            )
            return FILTER_MENU
        else:
            await update.message.reply_text(
                "Пожалуйста, введите корректный процент от 1 до 100:"
            )
            return MARKET_PERCENT
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректное число:"
        )
        return MARKET_PERCENT


# Обработчики для настройки уведомлений
async def handle_notification_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор в меню настроек уведомлений."""
    query = update.callback_query
    await query.answer()

    data = query.data

    if data == "notif_type":
        await query.edit_message_text(
            "Выберите тип уведомлений:",
            reply_markup=get_notification_type_keyboard()
        )
        return NOTIFICATION_TYPE
    elif data == "notif_time":
        await query.edit_message_text(
            "Введите время для ежедневных уведомлений в формате ЧЧ:ММ (например, 10:00):"
        )
        return NOTIFICATION_TIME
    elif data == "notif_interval":
        await query.edit_message_text(
            "Введите интервал в часах для периодических уведомлений (например, 1 для каждый час, 2 для каждые два часа):"
        )
        return NOTIFICATION_INTERVAL
    elif data == "save_notifications":
        # Возвращаемся в главное меню
        await query.edit_message_text(
            "Настройки уведомлений сохранены!"
        )
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return MAIN_MENU
    elif data == "back_to_notif_menu":
        await query.edit_message_text(
            "Настройка уведомлений:",
            reply_markup=get_notification_menu_keyboard()
        )
        return NOTIFICATION_MENU
    elif data.startswith("type_"):
        notification_type = data.replace("type_", "")

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.notifications:
                user.notifications[0].frequency_type = notification_type
                db.commit()

                # Перезапуск планировщика для этого пользователя
                restart_user_scheduler(user.telegram_id)
        finally:
            db.close()

        await query.edit_message_text(
            f"Тип уведомлений установлен: {notification_type}. Настройка уведомлений:",
            reply_markup=get_notification_menu_keyboard()
        )
        return NOTIFICATION_MENU


async def handle_notification_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод времени для уведомлений."""
    text = update.message.text

    # Паттерн для времени (например, "10:00")
    time_pattern = r'^([0-1]?[0-9]|2[0-3]):([0-5][0-9])$'
    match = re.match(time_pattern, text)

    if match:
        hour = int(match.group(1))
        minute = int(match.group(2))

        # Обновляем в базе данных
        db = Session()
        try:
            user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
            if user and user.notifications:
                user.notifications[0].hour = hour
                user.notifications[0].minute = minute
                db.commit()

                # Перезапуск планировщика для этого пользователя
                restart_user_scheduler(user.telegram_id)
        finally:
            db.close()

        await update.message.reply_text(
            f"Время уведомлений установлено: {hour:02d}:{minute:02d}. Настройка уведомлений:",
            reply_markup=get_notification_menu_keyboard()
        )
        return NOTIFICATION_MENU
    else:
        await update.message.reply_text(
            "Пожалуйста, введите корректное время в формате ЧЧ:ММ (например, 10:00):"
        )
        return NOTIFICATION_TIME


async def handle_notification_interval(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод интервала для периодических уведомлений."""
    try:
        interval = int(update.message.text)
        if 1 <= interval <= 24:
            # Обновляем в базе данных
            db = Session()
            try:
                user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
                if user and user.notifications:
                    user.notifications[0].interval_hours = interval
                    db.commit()

                    # Перезапуск планировщика для этого пользователя
                    restart_user_scheduler(user.telegram_id)
            finally:
                db.close()

            await update.message.reply_text(
                f"Интервал уведомлений установлен: каждые {interval} ч. Настройка уведомлений:",
                reply_markup=get_notification_menu_keyboard()
            )
            return NOTIFICATION_MENU
        else:
            await update.message.reply_text(
                "Пожалуйста, введите корректный интервал от 1 до 24 часов:"
            )
            return NOTIFICATION_INTERVAL
    except ValueError:
        await update.message.reply_text(
            "Пожалуйста, введите корректное число:"
        )
        return NOTIFICATION_INTERVAL


async def search_properties(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выполняет поиск объявлений по фильтрам пользователя."""
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
        if not user or not user.filters:
            await update.message.reply_text(
                "Сначала настройте фильтры поиска."
            )
            return

        user_filter = user.filters[0]

        # Строим SQL-запрос на основе фильтров пользователя
        query = """
                SELECT f.id, f.url, f.room, f.square, f.address, f.description, f.title,
                       p.price, p.green_percentage
                FROM flats f
                JOIN (
                    SELECT flat_id, price, green_percentage
                    FROM prices
                    WHERE (flat_id, date) IN (
                        SELECT flat_id, MAX(date) 
                        FROM prices 
                        GROUP BY flat_id
                    )
                ) p ON f.id = p.flat_id
                WHERE 1=1
                """

        params = {}

        # Фильтр по году постройки (извлекаем из описания)
        if user_filter.year_min is not None or user_filter.year_max is not None:
            # Это сложно сделать через прямой SQL-запрос, поэтому мы будем фильтровать результаты в Python
            pass

        # Фильтр по району (по адресу)
        if user_filter.districts and len(user_filter.districts) > 0:
            # Также сложно через SQL, фильтруем в Python
            pass

        # Фильтр по комнатам
        if user_filter.rooms_min is not None:
            query += " AND f.room >= :rooms_min"
            params["rooms_min"] = user_filter.rooms_min

        if user_filter.rooms_max is not None:
            query += " AND f.room <= :rooms_max"
            params["rooms_max"] = user_filter.rooms_max

        # Фильтр по цене
        if user_filter.price_min is not None:
            query += " AND p.price >= :price_min"
            params["price_min"] = user_filter.price_min

        if user_filter.price_max is not None:
            query += " AND p.price <= :price_max"
            params["price_max"] = user_filter.price_max

        # Фильтр по площади
        if user_filter.area_min is not None:
            query += " AND f.square >= :area_min"
            params["area_min"] = user_filter.area_min

        if user_filter.area_max is not None:
            query += " AND f.square <= :area_max"
            params["area_max"] = user_filter.area_max

        # Фильтр по проценту от рыночной цены
        if user_filter.max_market_price_percent is not None:
            query += " AND p.green_percentage <= :market_percent"
            params["market_percent"] = user_filter.max_market_price_percent

        # Сортировка по проценту от рыночной цены (от меньшего к большему)
        query += " ORDER BY p.green_percentage ASC LIMIT 10"

        # Выполняем запрос
        result = db.execute(text(query), params).fetchall()

        # Дополнительная фильтрация по году и району в Python
        filtered_results = []
        for row in result:
            # Извлекаем год постройки из описания
            year = extract_year_from_description(row.description)
            # Определяем район по адресу
            district = get_district_from_address(row.address)

            # Проверка года постройки
            year_matches = True
            if year is not None and user_filter.year_min is not None and year < user_filter.year_min:
                year_matches = False
            if year is not None and user_filter.year_max is not None and year > user_filter.year_max:
                year_matches = False

            # Проверка района
            district_matches = True
            if district is not None and user_filter.districts and len(user_filter.districts) > 0:
                if district not in user_filter.districts:
                    district_matches = False

            # Проверка этажа
            floor, total_floors = extract_floor_info(row.description)
            floor_matches = True

            if floor is not None and total_floors is not None:
                if user_filter.not_first_floor and floor == 1:
                    floor_matches = False
                if user_filter.not_last_floor and floor == total_floors:
                    floor_matches = False
                if user_filter.min_floor is not None and floor < user_filter.min_floor:
                    floor_matches = False
                if user_filter.max_floor is not None and floor > user_filter.max_floor:
                    floor_matches = False

            # Если все условия выполнены, добавляем объявление в результаты
            if year_matches and district_matches and floor_matches:
                filtered_results.append(row)

        # Отправляем результаты пользователю
        if filtered_results:
            await update.message.reply_text(
                f"Найдено {len(filtered_results)} объявлений, соответствующих вашим критериям:"
            )

            for row in filtered_results:
                # Вычисляем стоимость за кв.м
                price_per_sqm = row.price / row.square if row.square > 0 else 0

                # Извлекаем дополнительную информацию
                year = extract_year_from_description(row.description) or "Неизвестно"
                district = get_district_from_address(row.address) or "Неизвестно"
                floor, total_floors = extract_floor_info(row.description) or (None, None)
                floor_info = f"{floor}/{total_floors}" if floor and total_floors else "Неизвестно"

                message = (
                    f"🏠 *{row.title or 'Квартира'}*\n"
                    f"🏙️ Район: {district}\n"
                    f"🏢 Год постройки: {year}\n"
                    f"🔢 Этаж: {floor_info}\n"
                    f"🚪 Комнат: {row.room}\n"
                    f"📏 Площадь: {row.square} м²\n"
                    f"💰 Цена: {row.price:,} тенге\n"
                    f"📊 Цена за м²: {price_per_sqm:,.0f} тенге\n"
                    f"📉 На {100 - row.green_percentage:.1f}% ниже рыночной\n"
                    f"🔗 [Подробнее]({row.url})"
                )

                await update.message.reply_text(message, parse_mode="Markdown")
            else:
                await update.message.reply_text(
                    "По вашим критериям не найдено объявлений. Попробуйте изменить фильтры."
                )

    except Exception as e:
        logger.error(f"Ошибка при поиске объявлений: {e}")
        await update.message.reply_text(
            "Произошла ошибка при поиске. Пожалуйста, попробуйте позже или свяжитесь с администратором."
        )
    finally:
        db.close()


async def show_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статистику по объявлениям."""
    db = Session()
    try:
        # Получаем статистику по ценам
        avg_price = db.execute(text("""
                        SELECT AVG(p.price) as avg_price
                        FROM prices p
                        JOIN (
                            SELECT flat_id, MAX(date) as max_date
                            FROM prices
                            GROUP BY flat_id
                        ) latest ON p.flat_id = latest.flat_id AND p.date = latest.max_date
                    """)).scalar() or 0

        # Средняя цена за квадратный метр
        avg_price_per_sqm = db.execute(text("""
                        SELECT AVG(p.price / f.square) as avg_price_per_sqm
                        FROM prices p
                        JOIN flats f ON p.flat_id = f_id
                        JOIN (
                            SELECT flat_id, MAX(date) as max_date
                            FROM prices
                            GROUP BY flat_id
                        ) latest ON p.flat_id = latest.flat_id AND p.date = latest.max_date
                        WHERE f.square > 0
                    """)).scalar() or 0

        # Статистика по районам
        district_stats = {}
        properties = db.execute(text("""
                        SELECT f.address, f.square, p.price
                        FROM flats f
                        JOIN (
                            SELECT flat_id, price
                            FROM prices
                            WHERE (flat_id, date) IN (
                                SELECT flat_id, MAX(date)
                                FROM prices
                                GROUP BY flat_id
                            )
                        ) p ON f.id = p.flat_id
                    """)).fetchall()

        for property in properties:
            district = get_district_from_address(property.address)
            if district:
                if district not in district_stats:
                    district_stats[district] = {
                        'count': 0,
                        'total_price': 0,
                        'total_area': 0
                    }
                district_stats[district]['count'] += 1
                district_stats[district]['total_price'] += property.price
                district_stats[district]['total_area'] += property.square

        # Формируем сообщение со статистикой
        message = "📊 *Статистика по объявлениям*\n\n"

        message += f"💰 Средняя цена: {avg_price:,.0f} тенге\n"
        message += f"📊 Средняя цена за м²: {avg_price_per_sqm:,.0f} тенге\n\n"

        message += "*Статистика по районам:*\n"
        for district, stats in district_stats.items():
            avg_district_price = stats['total_price'] / stats['count'] if stats[
                                                                              'count'] > 0 else 0
            avg_district_price_per_sqm = stats['total_price'] / stats['total_area'] if stats[
                                                                                           'total_area'] > 0 else 0

            message += (
                f"🏙️ *{district}*\n"
                f"  • Объявлений: {stats['count']}\n"
                f"  • Средняя цена: {avg_district_price:,.0f} тенге\n"
                f"  • Средняя цена за м²: {avg_district_price_per_sqm:,.0f} тенге\n\n"
            )

        await update.message.reply_text(message, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"Ошибка при получении статистики: {e}")
        await update.message.reply_text(
            "Произошла ошибка при получении статистики. Пожалуйста, попробуйте позже или свяжитесь с администратором."
        )
    finally:
        db.close()


# Система планирования отправки уведомлений
scheduler = AsyncIOScheduler()


def setup_user_scheduler(user_id, bot):
    """Настраивает планировщик для конкретного пользователя."""
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.notifications or not user.notifications[0].enabled:
            return

        notification_settings = user.notifications[0]
        job_queue = Application.get_current().job_queue

        # Удаляем существующие задания
        current_jobs = job_queue.get_jobs_by_name(f"notification_{user_id}")
        for job in current_jobs:
            job.schedule_removal()

        # Создаем новое задание
        if notification_settings.frequency_type == "daily":
            # Ежедневное уведомление в указанное время
            job_queue.run_daily(
                send_notification,
                time=datetime.time(hour=notification_settings.hour, minute=notification_settings.minute),
                days=(0, 1, 2, 3, 4, 5, 6),
                context={"user_id": user_id, "bot": bot},
                name=f"notification_{user_id}"
            )
        elif notification_settings.frequency_type == "hourly":
            # Периодические уведомления с интервалом
            job_queue.run_repeating(
                send_notification,
                interval=datetime.timedelta(hours=notification_settings.interval_hours),
                first=0,
                context={"user_id": user_id, "bot": bot},
                name=f"notification_{user_id}"
            )

        logger.info(f"Scheduled notifications for user {user_id} - {notification_settings.frequency_type}")

    except Exception as e:
        logger.error(f"Ошибка в setup_user_scheduler для пользователя {user_id}: {e}")
    finally:
        db.close()


async def send_notification(user_id, bot):
    """Отправляет уведомление о новых объявлениях пользователю."""
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.filters:
            return

        user_filter = user.filters[0]

        # Определяем дату последнего уведомления
        last_notification = None
        if user.notifications and user.notifications[0].last_sent_at:
            last_notification = user.notifications[0].last_sent_at

        # Строим SQL-запрос на основе фильтров пользователя и даты последнего уведомления
        query = """
                    SELECT f.id, f.url, f.room, f.square, f.address, f.description, f.title,
                           p.price, p.green_percentage
                    FROM flats f
                    JOIN (
                        SELECT flat_id, price, green_percentage
                        FROM prices
                        WHERE (flat_id, date) IN (
                            SELECT flat_id, MAX(date)
                            FROM prices
                            GROUP BY flat_id
                        )
                    ) p ON f.id = p.flat_id
                    WHERE 1=1
                    """

        params = {}

        # Добавляем фильтр по дате добавления/обновления объявления
        if last_notification:
            query += " AND f.created_at > :last_notification"
            params["last_notification"] = last_notification

        # Фильтр по комнатам
        if user_filter.rooms_min is not None:
            query += " AND f.room >= :rooms_min"
            params["rooms_min"] = user_filter.rooms_min

            if user_filter.rooms_max is not None:
                query += " AND f.room <= :rooms_max"
                params["rooms_max"] = user_filter.rooms_max

            # Фильтр по цене
            if user_filter.price_min is not None:
                query += " AND p.price >= :price_min"
                params["price_min"] = user_filter.price_min

            if user_filter.price_max is not None:
                query += " AND p.price <= :price_max"
                params["price_max"] = user_filter.price_max

            # Фильтр по площади
            if user_filter.area_min is not None:
                query += " AND f.square >= :area_min"
                params["area_min"] = user_filter.area_min

            if user_filter.area_max is not None:
                query += " AND f.square <= :area_max"
                params["area_max"] = user_filter.area_max

            # Фильтр по проценту от рыночной цены
            if user_filter.max_market_price_percent is not None:
                query += " AND p.green_percentage <= :market_percent"
                params["market_percent"] = user_filter.max_market_price_percent

            # Сортировка по проценту от рыночной цены (от меньшего к большему)
            query += " ORDER BY p.green_percentage ASC LIMIT 10"

            # Выполняем запрос
            result = db.execute(text(query), params).fetchall()
            print("result" + str(result))
            # Дополнительная фильтрация по году и району в Python
            filtered_results = []
            for row in result:
                # Извлекаем год постройки из описания
                year = extract_year_from_description(row.description)
                # Определяем район по адресу
                district = get_district_from_address(row.address)

                # Проверка года постройки
                year_matches = True
                if year is not None and user_filter.year_min is not None and year < user_filter.year_min:
                    year_matches = False
                if year is not None and user_filter.year_max is not None and year > user_filter.year_max:
                    year_matches = False

                # Проверка района
                district_matches = True
                if district is not None and user_filter.districts and len(
                        user_filter.districts) > 0:
                    if district not in user_filter.districts:
                        district_matches = False

                # Проверка этажа
                floor, total_floors = extract_floor_info(row.description)
                floor_matches = True

                if floor is not None and total_floors is not None:
                    if user_filter.not_first_floor and floor == 1:
                        floor_matches = False
                    if user_filter.not_last_floor and floor == total_floors:
                        floor_matches = False
                    if user_filter.min_floor is not None and floor < user_filter.min_floor:
                        floor_matches = False
                    if user_filter.max_floor is not None and floor > user_filter.max_floor:
                        floor_matches = False

                # Если все условия выполнены, добавляем объявление в результаты
                if year_matches and district_matches and floor_matches:
                    filtered_results.append(row)

            # Отправляем найденные объявления пользователю
            if filtered_results:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🔔 Найдено {len(filtered_results)} новых объявлений, соответствующих вашим критериям!"
                )

                for row in filtered_results:
                    # Вычисляем стоимость за кв.м
                    price_per_sqm = row.price / row.square if row.square > 0 else 0

                    # Извлекаем дополнительную информацию
                    year = extract_year_from_description(row.description) or "Неизвестно"
                    district = get_district_from_address(row.address) or "Неизвестно"
                    floor, total_floors = extract_floor_info(row.description) or (None, None)
                    floor_info = f"{floor}/{total_floors}" if floor and total_floors else "Неизвестно"

                    message = (
                        f"🏠 *{row.title or 'Квартира'}*\n"
                        f"🏙️ Район: {district}\n"
                        f"🏢 Год постройки: {year}\n"
                        f"🔢 Этаж: {floor_info}\n"
                        f"🚪 Комнат: {row.room}\n"
                        f"📏 Площадь: {row.square} м²\n"
                        f"💰 Цена: {row.price:,} тенге\n"
                        f"📊 Цена за м²: {price_per_sqm:,.0f} тенге\n"
                        f"📉 На {100 - row.green_percentage:.1f}% ниже рыночной\n"
                        f"🔗 [Подробнее]({row.url})"
                    )

                    await bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode="Markdown"
                    )

            # Обновляем время последнего уведомления
            if user.notifications:
                user.notifications[0].last_sent_at = datetime.now()
                db.commit()


    except Exception as e:
        logger.error(f"Ошибка при отправке уведомления пользователю {user_id}: {e}")
    finally:
        db.close()


def setup_schedulers(bot):
    """Настраивает планировщики для всех пользователей."""
    db = Session()
    try:
        users = db.query(User).all()
        for user in users:
            setup_user_scheduler(user.telegram_id, bot)
    except Exception as e:
        logger.error(f"Ошибка в setup_schedulers: {e}")
    finally:
        db.close()

def setup_user_scheduler(user_id, bot):
    """Настраивает планировщик для конкретного пользователя."""
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.notifications or not user.notifications[0].enabled:
            return

        notification_settings = user.notifications[0]

        # Имя задачи для этого пользователя
        job_id = f"notification_{user_id}"

        # Удаляем существующую задачу, если есть
        if scheduler.get_job(job_id):
            scheduler.remove_job(job_id)

        # Настраиваем новую задачу в зависимости от типа уведомлений
        if notification_settings.frequency_type == "daily":
            # Ежедневное уведомление в указанное время
            hour = notification_settings.hour or 10
            minute = notification_settings.minute or 0

            scheduler.add_job(
                send_notification,
                CronTrigger(hour=hour, minute=minute),
                args=[user_id, bot],
                id=job_id,
                replace_existing=True
            )
        elif notification_settings.frequency_type == "hourly":
            # Периодическое уведомление с указанным интервалом
            # Периодическое уведомление с указанным интервалом
            interval_hours = notification_settings.interval_hours or 1

            scheduler.add_job(
                send_notification,
                IntervalTrigger(hours=interval_hours),
                args=[user_id, bot],
                id=job_id,
                replace_existing=True
            )
    finally:
        db.close()


def restart_user_scheduler(user_id):
    """Перезапускает планировщик для пользователя при изменении настроек."""
    if not scheduler.running:
        return

    # Получаем бота из текущего приложения
    bot = Application.get_current().bot

    # Перенастраиваем планировщик для пользователя
    setup_user_scheduler(user_id, bot)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает справку по командам."""
    help_text = (
        "🤖 *Помощь по боту поиска недвижимости*\n\n"
        "*Основные команды:*\n"
        "/start - Начать работу с ботом\n"
        "/help - Показать эту справку\n\n"

        "*Как пользоваться:*\n"
        "1. Настройте фильтры через меню 'Настройка фильтров'\n"
        "   • Год постройки\n"
        "   • Районы\n"
        "   • Этажность\n"
        "   • Количество комнат\n"
        "   • Диапазон цен\n"
        "   • Площадь\n"
        "   • Процент от рыночной цены\n\n"

        "2. Настройте уведомления через меню 'Настройка уведомлений'\n"
        "   • Тип уведомлений (ежедневно или каждый час)\n"
        "   • Время уведомлений (для ежедневных)\n"
        "   • Интервал уведомлений (для периодических)\n\n"

        "3. Используйте 'Поиск объявлений' для мгновенного поиска по вашим критериям\n\n"

        "4. Просматривайте статистику через меню 'Статистика'\n\n"

        "Бот будет автоматически присылать вам новые объявления, соответствующие вашим критериям, в соответствии с настройками уведомлений."
    )

    await update.message.reply_text(
        help_text,
        parse_mode="Markdown"
    )


async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отменяет текущую операцию и возвращается в главное меню."""
    await update.message.reply_text(
        "Операция отменена. Выберите действие:",
        reply_markup=get_main_keyboard()
    )
    return MAIN_MENU


async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(msg="Exception while handling update:", exc_info=context.error)

    if isinstance(context.error, TelegramError):
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="⚠️ Произошла ошибка. Пожалуйста, попробуйте еще раз."
        )


async def on_startup(application: Application):
    # Start the scheduler
    scheduler.start()

    # Setup user schedulers after application starts
    setup_schedulers(application.bot)

def main():
    """Запускает бота."""
    # Создаем приложение
    # Инициализация базы данных
    Base.metadata.create_all(bind=engine)

    application = Application.builder() \
        .token(TOKEN) \
        .post_init(on_startup) \
        .build()


    # Создаем конверсейшн хэндлер для основного меню
    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            MAIN_MENU: [
                CallbackQueryHandler(handle_main_menu),
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu)
            ],
            FILTER_MENU: [
                CallbackQueryHandler(handle_filter_menu)
            ],
            YEAR_MIN: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_year_min)
            ],
            YEAR_MAX: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_year_max)
            ],
            DISTRICTS: [
                CallbackQueryHandler(handle_filter_menu)
            ],
            FLOOR_SETTINGS: [
                CallbackQueryHandler(handle_filter_menu)
            ],
            MIN_FLOOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_min_floor)
            ],
            MAX_FLOOR: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_max_floor)
            ],
            ROOMS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_rooms)
            ],
            PRICE_RANGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_price_range)
            ],
            AREA_RANGE: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_area_range)
            ],
            MARKET_PERCENT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_market_percent)
            ],
            NOTIFICATION_MENU: [
                CallbackQueryHandler(handle_notification_menu)
            ],
            NOTIFICATION_TYPE: [
                CallbackQueryHandler(handle_notification_menu)
            ],
            NOTIFICATION_TIME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, handle_notification_time)
            ],
            NOTIFICATION_INTERVAL: [
                MessageHandler(filters.TEXT & ~filters.COMMAND,
                               handle_notification_interval)
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True
    )

    application.add_error_handler(error_handler)
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("help", help_command))

    # Настраиваем планировщики при запуске
    setup_schedulers(application.bot)
    # Запускаем бота
    application.run_polling()


if __name__ == "__main__":
    main()
