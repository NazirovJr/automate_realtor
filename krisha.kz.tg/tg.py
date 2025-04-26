import logging
from datetime import datetime, timedelta
import re
import os
from dotenv import load_dotenv
import atexit
import errno
import sys
import time
import json
import random

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, ForeignKey, JSON, UniqueConstraint, func, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, scoped_session
from sqlalchemy.sql import text
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, filters, ConversationHandler
from telegram.error import Conflict

# Загрузка переменных среды из .env файла
load_dotenv()

# Настройка логгирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Конфигурация из переменных среды
TOKEN = os.getenv("TELEGRAM_TOKEN", "7866153858:AAFMpL-XejNmlJdkgc9D6ExC1H6hkQeBPvY")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost/krisha")
ADMIN_TELEGRAM_ID = int(os.getenv("ADMIN_TELEGRAM_ID", "0"))  # ID администратора

logger.info(f"Using Telegram Token: {TOKEN[:5]}...{TOKEN[-5:]}")
logger.info(f"Admin Telegram ID: {ADMIN_TELEGRAM_ID}")
logger.info(f"Database URL: {DATABASE_URL}")

# Инициализация базы данных
Base = declarative_base()
engine = create_engine(DATABASE_URL)
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)


# Определение моделей базы данных для пользовательских настроек
class User(Base):
    __tablename__ = "telegram_users"

    id = Column(Integer, primary_key=True)
    telegram_id = Column(BigInteger, unique=True, index=True)
    username = Column(String, nullable=True)
    first_name = Column(String)
    last_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.now)

    filters = relationship("UserFilter", back_populates="user", cascade="all, delete-orphan")
    notifications = relationship("NotificationSetting", back_populates="user", cascade="all, delete-orphan")
    sent_properties = relationship("SentProperty", back_populates="user", cascade="all, delete-orphan")


class UserFilter(Base):
    __tablename__ = "user_filters"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("telegram_users.id"))
    year_min = Column(Integer, nullable=True)
    year_max = Column(Integer, nullable=True)
    districts = Column(JSON, default=lambda: ["Алмалинский", "Бостандыкский", "Медеуский", "Жетысуский"])
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
    max_market_price_percent = Column(Float, default=0.0)
    city = Column(String, nullable=True)
    address = Column(String, nullable=True)

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


class SentProperty(Base):
    __tablename__ = "sent_properties"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("telegram_users.id"))
    property_id = Column(Integer)  # ID объявления
    sent_at = Column(DateTime, default=datetime.now)

    user = relationship("User", back_populates="sent_properties")

    # Уникальный индекс для пары user_id и property_id
    __table_args__ = (
        # Не отправаем одно и то же объявление одному и тому же пользователю дважды
        UniqueConstraint('user_id', 'property_id', name='uq_user_property'),
    )


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
    CITY,
    ADDRESS,
    NOTIFICATION_MENU,
    NOTIFICATION_TYPE,
    NOTIFICATION_TIME,
    NOTIFICATION_INTERVAL,
    RESET_FILTERS,
    ADMIN_BROADCAST
) = range(20)


# Клавиатуры
def get_main_keyboard(is_admin=False):
    keyboard = [
        [KeyboardButton("⚙️ Настройка фильтров")],
        [KeyboardButton("👁️ Мои фильтры")],
        [KeyboardButton("🔔 Настройка уведомлений")],
        [KeyboardButton("🔍 Поиск объявлений")],
        [KeyboardButton("📊 Статистика")],
        [KeyboardButton("📩 Получить данные")],
        [KeyboardButton("🗑️ Сбросить историю")],
        [KeyboardButton("ℹ️ Помощь")]
    ]
    
    # Add admin-only options
    if is_admin:
        keyboard.append([KeyboardButton("🛠️ Админ: Отправить всем")])
        keyboard.append([KeyboardButton("📈 Админ: Общая статистика")])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)


def get_filter_menu_keyboard():
    """Возвращает клавиатуру для меню фильтров."""
    keyboard = [
        [
            InlineKeyboardButton("Год постройки", callback_data="filter_year"),
            InlineKeyboardButton("Районы", callback_data="filter_districts")
        ],
        [
            InlineKeyboardButton("Этажи", callback_data="filter_floors"),
            InlineKeyboardButton("Комнаты", callback_data="filter_rooms")
        ],
        [
            InlineKeyboardButton("Цена", callback_data="filter_price"),
            InlineKeyboardButton("Площадь", callback_data="filter_area")
        ],
        [
            InlineKeyboardButton("% от рыночной", callback_data="filter_market")
        ],
        [
            InlineKeyboardButton("Город", callback_data="filter_city"),
            InlineKeyboardButton("Адрес", callback_data="filter_address")
        ],
        [
            InlineKeyboardButton("Назад в меню", callback_data="back_to_menu")
        ]
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

    # Основной шаблон: числа разделенные / или "из" (например "5/9" или "5 этаж из 9")
    floor_patterns = [
        r'(\d+)(?:\s*[-/]\s*|\s+этаж\s+из\s+)(\d+)',  # 5/9, 5-9, 5 этаж из 9
        r'(\d+)\s*(?:этаж|эт)[.,]?\s+из\s+(\d+)',     # 5 этаж из 9, 5эт. из 9
        r'(\d+)\s*[-/]\s*(\d+)\s*эт',                 # 5/9 эт, 5-9 эт
        r'(\d+)\s*[-/]\s*(\d+)',                      # просто 5/9 или 5-9
        r'(\d+)\s+эт(?:аж|\.)\s+в\s+(\d+)-?этаж',     # 5 этаж в 9-этажном
        r'(\d+)\s+эт(?:аж|\.)[,]?\s+(\d+)-?эт',       # 5 этаж, 9-этажный
    ]

    for pattern in floor_patterns:
        match = re.search(pattern, description.lower())
        if match:
            try:
                floor = int(match.group(1))
                total = int(match.group(2))
                # Проверка на валидность
                if 0 < floor <= total:
                    return floor, total
            except (ValueError, IndexError):
                pass

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
    is_admin = user.id == ADMIN_TELEGRAM_ID
    
    # Log if admin
    if is_admin:
        logger.info(f"Admin user {user.id} ({user.username}) started the bot")

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
            reply_markup=get_main_keyboard(is_admin=is_admin)
        )
        return MAIN_MENU
    finally:
        db.close()


async def handle_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает выбор в главном меню."""
    user = update.effective_user
    is_admin = user.id == ADMIN_TELEGRAM_ID
    
    # Check if this is a message or callback query
    if update.message:
        text = update.message.text
        reply_method = update.message.reply_text
    elif update.callback_query:
        await update.callback_query.answer()
        text = update.callback_query.data
        reply_method = update.callback_query.message.reply_text
    else:
        # If neither, return to main menu
        logger.error("Received update is neither message nor callback query")
        return MAIN_MENU
        
    if text == "⚙️ Настройка фильтров":
        await reply_method(
            "Выберите параметр для настройки:",
            reply_markup=get_filter_menu_keyboard()
        )
        return FILTER_MENU
    elif text == "👁️ Мои фильтры":
        # Показываем текущие фильтры
        await show_current_filters(update, context)
        return MAIN_MENU
    elif text == "🔔 Настройка уведомлений":
        # Показываем меню настройки уведомлений
        await reply_method(
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
    elif text == "📩 Получить данные":
        # Запускаем тест уведомлений
        await test_notification(update, context)
        return MAIN_MENU
    elif text == "🗑️ Сбросить историю":
        # Сбрасываем историю отправленных объявлений
        await reset_sent_properties(update, context)
        return MAIN_MENU
    elif text == "ℹ️ Помощь":
        # Показываем помощь
        await reply_method(
            "Этот бот поможет вам найти выгодные предложения недвижимости по заданным параметрам.\n\n"
            "Как пользоваться:\n"
            "1. Настройте фильтры через меню 'Настройка фильтров'\n"
            "2. Просматривайте и сбрасывайте фильтры через меню 'Мои фильтры'\n"
            "3. Настройте уведомления через меню 'Настройка уведомлений'\n"
            "4. Используйте 'Поиск объявлений' для мгновенного поиска по вашим критериям\n"
            "5. Используйте 'Тест уведомлений' для проверки работы системы\n"
            "6. Если хотите получать объявления, которые уже видели, используйте 'Сбросить историю'\n\n"
            "Используйте кнопки внизу для навигации."
        )
        return MAIN_MENU
    # Admin-only options
    elif text == "🛠️ Админ: Отправить всем" and is_admin:
        await reply_method(
            "Введите сообщение, которое нужно отправить всем пользователям:"
        )
        return ADMIN_BROADCAST
    elif text == "📈 Админ: Общая статистика" and is_admin:
        await admin_overall_statistics(update, context)
        return MAIN_MENU
    else:
        # Неизвестная команда или попытка доступа к админ-функциям не-админом
        if text.startswith("🛠️ Админ:") or text.startswith("📈 Админ:"):
            if not is_admin:
                logger.warning(f"Non-admin user {user.id} tried to access admin function: {text}")
                await reply_method("У вас нет прав для выполнения этой команды.")
        else:
            await reply_method("Неизвестная команда. Используйте кнопки для навигации.")
        
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
        # Change from selection to text input
        await query.edit_message_text(
            "Введите названия районов через запятую (например, 'Алмалинский, Бостандыкский'):"
        )
        return DISTRICTS
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
            "Введите диапазон цен через дефис в миллионах тенге (например, 15000000-30000000 для поиска от 15000000 до 30000000):"
        )
        return PRICE_RANGE
    elif data == "filter_area":
        await query.edit_message_text(
            "Введите диапазон площади в квадратных метрах через дефис (например, 40-80):"
        )
        return AREA_RANGE
    elif data == "filter_market":
        await query.edit_message_text(
            "Введите максимальный процент от рыночной цены (например, 90 для поиска объявлений дешевле на 10% от рынка):"
        )
        return MARKET_PERCENT
    elif data == "filter_city":
        await query.edit_message_text(
            "Введите название города (например, 'Алматы', 'Астана'):"
        )
        return CITY
    elif data == "filter_address":
        await query.edit_message_text(
            "Введите часть адреса (улицу, район, микрорайон):"
        )
        return ADDRESS
    elif data == "back_to_menu":
        # Возвращаемся в главное меню
        await query.edit_message_text(
            "Настройки фильтров сохранены!"
        )
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return MAIN_MENU
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

        # Фильтрация по году постройки в описании
        if user_filter.year_min is not None and user_filter.year_max is not None:
            # Проверяем по шаблону "2020 г.п." и другим возможным форматам 
            # с учетом диапазона годов
            pattern_parts = []
            for year in range(user_filter.year_min, user_filter.year_max + 1):
                pattern_parts.append(f"{year} г.п.")
                pattern_parts.append(f"{year}г.п")
                pattern_parts.append(f"{year} года постройки")
                pattern_parts.append(f"{year}г. постройки")
                pattern_parts.append(f"построен в {year}")
            
            # Создаем LIKE условия для каждого шаблона
            year_conditions = " OR ".join([f"f.description ILIKE '%{pattern}%'" for pattern in pattern_parts])
            query += f" AND ({year_conditions})"
        
        # Фильтрация по району
        if user_filter.districts and len(user_filter.districts) > 0:
            district_conditions = []
            for district in user_filter.districts:
                district_conditions.append(f"f.description ILIKE '%{district}%'")
                district_conditions.append(f"f.address ILIKE '%{district}%'")
                district_conditions.append(f"f.title ILIKE '%{district}%'")
            
            if district_conditions:
                query += f" AND ({' OR '.join(district_conditions)})"
        
        # Фильтрация по этажу в заголовке
        if user_filter.min_floor is not None or user_filter.max_floor is not None or user_filter.not_first_floor or user_filter.not_last_floor:
            floor_conditions = []
            
            # Для диапазона этажей
            if user_filter.min_floor is not None and user_filter.max_floor is not None:
                for floor in range(user_filter.min_floor, user_filter.max_floor + 1):
                    floor_conditions.append(f"f.title ILIKE '%{floor}/%'")
                    floor_conditions.append(f"f.description ILIKE '%{floor} этаж%'")
                    floor_conditions.append(f"f.description ILIKE '%{floor}-й этаж%'")
            
            # Не первый этаж
            if user_filter.not_first_floor:
                query += " AND f.title NOT ILIKE '%1/%' AND f.description NOT ILIKE '%1 этаж%' AND f.description NOT ILIKE '%1-й этаж%'"
            
            # Не последний этаж (трудно реализовать через SQL, будем проверять это в Python)
            
            if floor_conditions:
                query += f" AND ({' OR '.join(floor_conditions)})"

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
            query += " AND p.green_percentage >= :market_percent"
            params["market_percent"] = user_filter.max_market_price_percent

        # Сортировка по проценту от рыночной цены (от меньшего к большему)
        query += " ORDER BY p.green_percentage DESC LIMIT 10"

        # Выполняем запрос
        try:
            # Переделываем сложный запрос в более безопасный вариант, без прямой вставки значений в SQL
            # Удаляем текстовые условия LIKE, которые мы добавили программно
            safe_query = query
            result = db.execute(text(safe_query), params).fetchall()
        except Exception as e:
            logger.error(f"SQL error: {e}")
            # Если возникла ошибка с SQL, делаем запрос без фильтрации
            basic_query = """
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
            params.pop("sent_property_ids", None)
            
            for key, value in params.items():
                if key in ["rooms_min", "price_min", "area_min", "market_percent"]:
                    basic_query += f" AND {key.replace(':', '')} >= :{key}"
                elif key in ["rooms_max", "price_max", "area_max"]:
                    basic_query += f" AND {key.replace(':', '')} <= :{key}"
            
            basic_query += " ORDER BY p.green_percentage ASC LIMIT 20"
            result = db.execute(text(basic_query), params).fetchall()
            
        # Дополнительная фильтрация по году и району в Python
        filtered_results = []
        for row in result:
            # Дополнительная проверка, не отправляли ли уже это объявление
            if row.id in sent_property_ids:
                continue
                
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

            # Проверка наличия года постройки в описании в виде "YYYY г.п."
            if user_filter.year_min is not None and user_filter.year_max is not None:
                # Если год не был извлечен, пробуем проверить шаблоны в описании
                if year is None:
                    year_matches_pattern = False
                    for year_check in range(user_filter.year_min, user_filter.year_max + 1):
                        if (f"{year_check} г.п." in row.description.lower() or 
                            f"{year_check}г.п" in row.description.lower() or
                            f"{year_check} года постройки" in row.description.lower() or
                            f"построен в {year_check}" in row.description.lower()):
                            year_matches_pattern = True
                            break
                    year_matches = year_matches_pattern

            # Проверка района напрямую в тексте
            district_matches = True
            if user_filter.districts and len(user_filter.districts) > 0:
                direct_district_match = False
                districts_lower = [d.lower() for d in user_filter.districts]
                
                # Проверяем упоминания района в тексте
                for d_lower in districts_lower:
                    if (d_lower in row.description.lower() or 
                        d_lower in row.address.lower() or 
                        d_lower in row.title.lower()):
                        direct_district_match = True
                        break
                
                # Проверяем, соответствует ли извлеченный район одному из фильтров
                if not direct_district_match and district:
                    district_lower = district.lower()
                    if district_lower not in districts_lower:
                        district_matches = False

            # Проверка этажа
            floor, total_floors = extract_floor_info(row.description)
            
            # Если не смогли извлечь из описания, пробуем проверить заголовок
            if floor is None:
                # Ищем шаблоны типа "3/5 этаж" в заголовке
                floor_pattern = r'(\d+)/(\d+)\s*этаж'
                floor_match = re.search(floor_pattern, row.title, re.IGNORECASE)
                if floor_match:
                    floor = int(floor_match.group(1))
                    total_floors = int(floor_match.group(2))
                
                # Если все еще не нашли, пробуем в объединенном тексте
                if floor is None or total_floors is None:
                    combined_text = row.title + " " + row.description
                    combined_match = re.search(r'(\d+)[/-](\d+)', combined_text)
                    if combined_match:
                        floor = int(combined_match.group(1))
                        total_floors = int(combined_match.group(2))
                        
                floor_info = f"{floor}/{total_floors}" if floor and total_floors else "Неизвестно"

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
                text=f"🔔 Найдено {len(filtered_results)} объявлений, соответствующих вашим критериям!"
            )

            for row in filtered_results:
                # Вычисляем стоимость за кв.м
                price_per_sqm = row.price / row.square if row.square > 0 else 0

                # Извлекаем дополнительную информацию
                year = extract_year_from_description(row.description) or "Неизвестно"
                district = get_district_from_address(row.address) or "Неизвестно"
                floor, total_floors = extract_floor_info(row.description) or (None, None)
                
                # Если не смогли извлечь из описания, пробуем проверить заголовок
                if floor is None:
                    # Ищем шаблоны типа "3/5 этаж" в заголовке
                    floor_pattern = r'(\d+)/(\d+)\s*этаж'
                    floor_match = re.search(floor_pattern, row.title, re.IGNORECASE)
                    if floor_match:
                        floor = int(floor_match.group(1))
                        total_floors = int(floor_match.group(2))
                    
                    # Если все еще не нашли, пробуем в объединенном тексте
                    if floor is None or total_floors is None:
                        combined_text = row.title + " " + row.description
                        combined_match = re.search(r'(\d+)[/-](\d+)', combined_text)
                        if combined_match:
                            floor = int(combined_match.group(1))
                            total_floors = int(combined_match.group(2))
                        
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
                
                # Отмечаем объявление как отправленное
                sent_property = SentProperty(user_id=user.id, property_id=row.id)
                db.add(sent_property)
                
            # Сохраняем отправленные объявления в базе
            db.commit()
        else:
            await bot.send_message(
                chat_id=user_id,
                text="По вашим критериям не найдено новых объявлений."
            )

        # Обновляем время последнего уведомления
        if user.notifications:
            user.notifications[0].last_sent_at = datetime.now()
            db.commit()

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


def setup_user_scheduler(user_id):
    """Настраивает планировщик для конкретного пользователя."""
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.notifications or not user.notifications[0].enabled:
            return

        notification_settings = user.notifications[0]
        app = Application.get_current()

        # Имя задачи для этого пользователя
        job_id = f"notification_{user_id}"

        # Получаем список текущих заданий
        current_jobs = app.job_queue.get_jobs_by_name(job_id)
        for job in current_jobs:
            job.schedule_removal()

        # Настраиваем новую задачу в зависимости от типа уведомлений
        if notification_settings.frequency_type == "daily":
            # Ежедневное уведомление в указанное время
            hour = notification_settings.hour or 10
            minute = notification_settings.minute or 0

            app.job_queue.run_daily(
                send_notification,
                time=datetime.time(hour=hour, minute=minute),
                days=(0, 1, 2, 3, 4, 5, 6),
                context={"user_id": user_id},
                name=job_id
            )
            logger.info(f"Настроено ежедневное уведомление для пользователя {user_id} на {hour}:{minute}")
            
        elif notification_settings.frequency_type == "hourly":
            # Периодическое уведомление с указанным интервалом
            interval_hours = notification_settings.interval_hours or 1

            app.job_queue.run_repeating(
                send_notification,
                interval=datetime.timedelta(hours=interval_hours),
                first=0,
                context={"user_id": user_id},
                name=job_id
            )
            logger.info(f"Настроено почасовое уведомление для пользователя {user_id} с интервалом {interval_hours} ч")
    except Exception as e:
        logger.error(f"Ошибка при настройке планировщика для пользователя {user_id}: {e}")
    finally:
        db.close()


def setup_schedulers():
    """Настраивает планировщики для всех пользователей."""
    db = Session()
    try:
        users = db.query(User).all()
        for user in users:
            setup_user_scheduler(user.telegram_id)
    except Exception as e:
        logger.error(f"Ошибка в setup_schedulers: {e}")
    finally:
        db.close()


def restart_user_scheduler(user_id):
    """Перезапускает планировщик для пользователя при изменении настроек."""
    setup_user_scheduler(user_id)


async def test_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Тестовая функция для немедленной отправки уведомлений."""
    user_id = update.effective_user.id
    await update.message.reply_text("Отправляю тестовое уведомление...")
    
    # Создаем контекст с нужными данными
    context.job = type('obj', (object,), {
        'context': {"user_id": user_id}
    })
    
    # Вызываем функцию отправки уведомлений напрямую
    await send_notification(context)
    
    return MAIN_MENU


# Добавим команду для сброса истории отправленных объявлений
async def reset_sent_properties(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сбрасывает историю отправленных объявлений для пользователя."""
    user_id = update.effective_user.id
    
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if user:
            # Получаем количество удаляемых записей для информационного сообщения
            count = db.query(SentProperty).filter(SentProperty.user_id == user.id).count()
            
            # Удаляем все записи о отправленных объявлениях для этого пользователя
            db.query(SentProperty).filter(SentProperty.user_id == user.id).delete()
            db.commit()
            
            await update.message.reply_text(
                f"История отправленных объявлений сброшена! Удалено {count} записей."
            )
        else:
            await update.message.reply_text(
                "Не удалось найти ваш профиль в базе данных."
            )
    except Exception as e:
        logger.error(f"Ошибка при сбросе истории отправленных объявлений: {e}")
        await update.message.reply_text(
            "Произошла ошибка при сбросе истории. Пожалуйста, попробуйте позже."
        )
    finally:
        db.close()
    
    return MAIN_MENU


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

        "2. Просматривайте и сбрасывайте фильтры через меню 'Мои фильтры'\n"
        "   • Просмотр всех активных фильтров\n"
        "   • Сброс всех фильтров\n"
        "   • Сброс отдельных фильтров\n\n"

        "3. Настройте уведомления через меню 'Настройка уведомлений'\n"
        "   • Тип уведомлений (ежедневно или каждый час)\n"
        "   • Время уведомлений (для ежедневных)\n"
        "   • Интервал уведомлений (для периодических)\n\n"

        "4. Используйте 'Поиск объявлений' для мгновенного поиска по вашим критериям\n\n"
        
        "5. Используйте 'Получить данные' для проверки работы системы\n\n"

        "6. Просматривайте статистику через меню 'Статистика'\n\n"

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
    logger.error(f"Exception while handling update: {context.error}")
    
    # Check if update exists and has a chat
    if update and update.effective_chat:
        await context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Произошла ошибка при обработке вашего запроса. Попробуйте позже или свяжитесь с администратором."
        )
    else:
        # Log additional details if update is None
        if isinstance(context.error, Conflict):
            logger.error("Multiple bot instances detected. Please ensure only one instance is running.")
        else:
            logger.error(f"Error occurred outside user interaction: {context.error}")


async def on_startup(application: Application):
    # Настраиваем планировщики для пользователей
    setup_schedulers()
    logger.info("Бот запущен и планировщики настроены")


async def send_notification(context):
    """Отправляет уведомление о новых объявлениях пользователю."""
    user_id = context.job.context["user_id"]
    bot = context.bot
    
    logger.info(f"Starting notification process for user {user_id}")
    
    db = Session()
    try:
        # Get user information
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user:
            logger.info(f"User not found for user_id {user_id}")
            return
            
        if not user.filters:
            logger.info(f"No filters found for user_id {user_id}")
            return

        # Store necessary information from the user
        user_id_db = user.id
        
        # Get all filter parameters and store locally before closing the session
        if user.filters:
            user_filter = user.filters[0]
            # Copy all filter values to local variables to avoid detached object errors
            year_min = user_filter.year_min
            year_max = user_filter.year_max
            districts = list(user_filter.districts) if user_filter.districts else []
            not_first_floor = user_filter.not_first_floor
            not_last_floor = user_filter.not_last_floor
            min_floor = user_filter.min_floor
            max_floor = user_filter.max_floor
            rooms_min = user_filter.rooms_min
            rooms_max = user_filter.rooms_max
            price_min = user_filter.price_min
            price_max = user_filter.price_max
            area_min = user_filter.area_min
            area_max = user_filter.area_max
            max_market_price_percent = user_filter.max_market_price_percent
            city = user_filter.city
            address = user_filter.address
        else:
            logger.info(f"No filter found for user {user_id}")
            return
            
        # Get sent property IDs in a separate query to avoid detached object issues
        sent_property_ids = [sp.property_id for sp in db.query(SentProperty).filter(SentProperty.user_id == user_id_db).all()]
        
        # Close the initial session to avoid detached object issues
        db.close()
        logger.info(f"Initial session closed for user {user_id}")

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

        # Исключаем уже отправленные объявления
        if sent_property_ids:
            query += " AND f.id NOT IN :sent_property_ids"
            params["sent_property_ids"] = tuple(sent_property_ids) if len(sent_property_ids) > 1 else f"({sent_property_ids[0]})"

        # Фильтр по городу (с использованием ILIKE для регистро-независимого поиска)
        if city:
            query += " AND f.address ILIKE :city_pattern"
            params["city_pattern"] = f"%{city}%"
            
        # Фильтр по адресу (с использованием ILIKE для регистро-независимого поиска)
        if address:
            query += " AND f.address ILIKE :address_pattern"
            params["address_pattern"] = f"%{address}%"

        # Фильтр по комнатам
        if rooms_min is not None:
            query += " AND f.room >= :rooms_min"
            params["rooms_min"] = rooms_min

        if rooms_max is not None:
            query += " AND f.room <= :rooms_max"
            params["rooms_max"] = rooms_max

        # Фильтр по цене
        if price_min is not None:
            query += " AND p.price >= :price_min"
            params["price_min"] = price_min

        if price_max is not None:
            query += " AND p.price <= :price_max"
            params["price_max"] = price_max

        # Фильтр по площади
        if area_min is not None:
            query += " AND f.square >= :area_min"
            params["area_min"] = area_min

        if area_max is not None:
            query += " AND f.square <= :area_max"
            params["area_max"] = area_max

        # Фильтр по проценту от рыночной цены
        if max_market_price_percent is not None and max_market_price_percent > 0:
            query += " AND p.green_percentage >= :market_percent"
            params["market_percent"] = max_market_price_percent

        # Сортировка по проценту от рыночной цены (от большего к меньшему)
        query += " ORDER BY p.green_percentage DESC LIMIT 10"

        # Выполняем запрос и получаем данные
        try:
            # Используем свежее соединение для запроса
            db_query = Session()
            try:
                logger.info(f"Executing main query for user {user_id}")
                result = db_query.execute(text(query), params).fetchall()
                db_query.commit()  # Завершаем транзакцию успешно
                logger.info(f"Query returned {len(result)} results for user {user_id}")
            except Exception as e:
                db_query.rollback()  # Откатываем транзакцию при ошибке
                logger.error(f"SQL error in main query for user {user_id}: {e}")
                
                # Пробуем более простой запрос без сложных условий
                try:
                    logger.info(f"Trying fallback query for user {user_id}")
                    # Создаем новый запрос с базовыми фильтрами
                    basic_query = """
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
                    
                    basic_params = {}
                    
                    # Добавляем базовые фильтры по комнатам, цене и площади
                    if rooms_min is not None:
                        basic_query += " AND f.room >= :rooms_min"
                        basic_params["rooms_min"] = rooms_min
                    
                    if rooms_max is not None:
                        basic_query += " AND f.room <= :rooms_max"
                        basic_params["rooms_max"] = rooms_max
                    
                    if price_min is not None:
                        basic_query += " AND p.price >= :price_min"
                        basic_params["price_min"] = price_min
                    
                    if price_max is not None:
                        basic_query += " AND p.price <= :price_max"
                        basic_params["price_max"] = price_max
                    
                    basic_query += " ORDER BY p.green_percentage DESC LIMIT 10"
                    
                    result = db_query.execute(text(basic_query), basic_params).fetchall()
                    db_query.commit()
                    logger.info(f"Fallback query returned {len(result)} results for user {user_id}")
                except Exception as e2:
                    db_query.rollback()
                    logger.error(f"SQL error in fallback query for user {user_id}: {e2}")
                    result = []
            finally:
                db_query.close()
                logger.info(f"Query session closed for user {user_id}")
                
            # Получили результаты, теперь обрабатываем их
            filtered_results = []
            for row in result:
                property_id = row[0]
                url = row[1]
                room = row[2]
                square = row[3]
                address = row[4]
                description = row[5] or ''
                title = row[6] or ''
                price = row[7]
                green_percentage = row[8]
                
                # Пропускаем уже отправленные объявления
                if property_id in sent_property_ids:
                    continue
                
                # Проверяем соответствие условиям фильтрации
                extracted_year = extract_year_from_description(description)
                district = get_district_from_address(address)
                floor, total_floors = extract_floor_info(description)
                
                # Проверка года постройки
                year_filter_passed = True
                # Если год извлечен и фильтр установлен - проверяем соответствие
                if extracted_year is not None:
                    if year_min is not None and extracted_year < year_min:
                        year_filter_passed = False
                    if year_max is not None and extracted_year > year_max:
                        year_filter_passed = False
                # Если год не извлечен, но фильтр установлен - проверяем текст
                elif year_min is not None or year_max is not None:
                    # Проверяем, есть ли в тексте явное указание года
                    if " г.п." in description.lower() or "год постройки" in description.lower() or "построен в" in description.lower():
                        # Если есть упоминание года, проверяем соответствие любому году из диапазона
                        year_range_start = year_min or 1900
                        year_range_end = year_max or 2025
                        year_mentions_found = False
                        
                        for check_year in range(year_range_start, year_range_end + 1):
                            year_patterns = [
                                f"{check_year} г.п.", 
                                f"{check_year}г.п", 
                                f"{check_year} год", 
                                f"{check_year} года", 
                                f"построен в {check_year}"
                            ]
                            if any(pattern in description.lower() for pattern in year_patterns):
                                year_mentions_found = True
                                break
                                
                        # Если упоминание года есть, но не соответствует фильтру
                        if not year_mentions_found:
                            year_filter_passed = False
                
                if not year_filter_passed:
                    continue
                
                # Проверка района
                district_filter_passed = True
                if districts and len(districts) > 0:
                    # Если район извлечен - проверяем соответствие
                    if district is not None:
                        # Case-insensitive comparison
                        district_lower = district.lower()
                        districts_lower = [d.lower() for d in districts]
                        if district_lower not in districts_lower:
                            # Район не соответствует, проверяем упоминания в тексте
                            district_mentioned = False
                            for d in districts:
                                d_lower = d.lower()
                                if (d_lower in description.lower() or 
                                    d_lower in address.lower() or 
                                    d_lower in title.lower()):
                                    district_mentioned = True
                                    break
                            if not district_mentioned:
                                district_filter_passed = False
                    else:
                        # Если район не извлечен, проверяем упоминания в тексте
                        district_mentioned = False
                        for d in districts:
                            d_lower = d.lower()
                            if (d_lower in description.lower() or 
                                d_lower in address.lower() or 
                                d_lower in title.lower()):
                                district_mentioned = True
                                break
                        
                        # Если упоминаний района нет, но в тексте есть слово "район", 
                        # это может означать, что район указан не в нашем списке
                        if not district_mentioned:
                            if "район" in description.lower() or "район" in address.lower():
                                district_filter_passed = False
                
                if not district_filter_passed:
                    continue
                
                # Проверка этажа
                floor_filter_passed = True
                if floor is not None and total_floors is not None:
                    # Проверка на первый этаж
                    if not_first_floor and floor == 1:
                        floor_filter_passed = False
                    # Проверка на последний этаж
                    if not_last_floor and floor == total_floors:
                        floor_filter_passed = False
                    # Проверка минимального этажа
                    if min_floor is not None and floor < min_floor:
                        floor_filter_passed = False
                    # Проверка максимального этажа
                    if max_floor is not None and floor > max_floor:
                        floor_filter_passed = False
                else:
                    # Если этаж не извлечен, но фильтр установлен, проверяем текст
                    has_floor_filters = (not_first_floor or 
                                        not_last_floor or 
                                        min_floor is not None or 
                                        max_floor is not None)
                    
                    if has_floor_filters:
                        # Ищем упоминания этажей в тексте
                        floor_match = re.search(r'(\d+)/(\d+)', title + " " + description)
                        if floor_match:
                            # Если нашли паттерн этажа, проверяем соответствие
                            try:
                                extracted_floor = int(floor_match.group(1))
                                extracted_total = int(floor_match.group(2))
                                
                                if not_first_floor and extracted_floor == 1:
                                    floor_filter_passed = False
                                if not_last_floor and extracted_floor == extracted_total:
                                    floor_filter_passed = False
                                if min_floor is not None and extracted_floor < min_floor:
                                    floor_filter_passed = False
                                if max_floor is not None and extracted_floor > max_floor:
                                    floor_filter_passed = False
                            except:
                                pass
                        else:
                            # Проверяем явные упоминания первого/последнего этажа
                            if not_first_floor:
                                if "первый этаж" in description.lower() or "1 этаж" in description.lower() or "1-й этаж" in description.lower():
                                    floor_filter_passed = False
                            if not_last_floor:
                                if "последний этаж" in description.lower() or "верхний этаж" in description.lower():
                                    floor_filter_passed = False
                
                if not floor_filter_passed:
                    continue
                
                # Объявление прошло все фильтры
                filtered_results.append({
                    'id': property_id,
                    'url': url,
                    'room': room,
                    'square': square,
                    'address': address,
                    'description': description,
                    'title': title,
                    'price': price,
                    'green_percentage': green_percentage
                })
            
            # Отправляем найденные объявления пользователю
            if filtered_results:
                logger.info(f"Found {len(filtered_results)} filtered results for user {user_id}")
                await bot.send_message(
                    chat_id=user_id,
                    text=f"🔔 Найдено {len(filtered_results)} новых объявлений по вашим критериям!"
                )
                
                sent_count = 0
                
                for property_data in filtered_results:
                    # Форматируем сообщение с информацией об объявлении
                    property_year = extract_year_from_description(property_data['description']) or "Неизвестно"
                    property_district = get_district_from_address(property_data['address']) or "Неизвестно"
                    
                    # Сначала пробуем извлечь этаж из описания
                    property_floor, property_total_floors = extract_floor_info(property_data['description']) or (None, None)
                    
                    # Если не получилось, пробуем извлечь из заголовка
                    if property_floor is None or property_total_floors is None:
                        title_floor_match = re.search(r'(\d+)/(\d+)\s*этаж', property_data['title'], re.IGNORECASE)
                        if title_floor_match:
                            property_floor = int(title_floor_match.group(1))
                            property_total_floors = int(title_floor_match.group(2))
                    
                    # Пробуем поискать в объединенном тексте (заголовок + описание)
                    if property_floor is None or property_total_floors is None:
                        combined_text = property_data['title'] + " " + property_data['description']
                        combined_match = re.search(r'(\d+)[/-](\d+)', combined_text)
                        if combined_match:
                            property_floor = int(combined_match.group(1))
                            property_total_floors = int(combined_match.group(2))
                    
                    floor_info = f"{property_floor}/{property_total_floors}" if property_floor and property_total_floors else "Неизвестно"
                    
                    message = (
                        f"🏠 *{property_data['title'] or 'Квартира'}*\n"
                        f"🏙️ Район: {property_district}\n"
                        f"🏢 Год постройки: {property_year}\n"
                        f"🔢 Этаж: {floor_info}\n"
                        f"🚪 Комнат: {property_data['room']}\n"
                        f"📏 Площадь: {property_data['square']} м²\n"
                        f"💰 Цена: {property_data['price']:,} тенге\n"
                        f"📊 Цена за м²: {int(property_data['price'] / property_data['square']) if property_data['square'] else 0:,} тенге/м²\n"
                        f"📉 От рыночной: {property_data['green_percentage']:.1f}%\n\n"
                        f"🔗 [Подробнее]({property_data['url']})"
                    )
                    
                    try:
                        await bot.send_message(
                            chat_id=user_id,
                            text=message,
                            parse_mode="Markdown",
                            disable_web_page_preview=True
                        )
                        
                        # Сохраняем запись об отправленном объявлении
                        # Используем новую сессию для каждой операции записи
                        db_update = Session()
                        try:
                            new_sent_property = SentProperty(
                                user_id=user_id_db,  # Using the ID we stored earlier
                                property_id=property_data['id']
                            )
                            db_update.add(new_sent_property)
                            db_update.commit()
                            sent_count += 1
                        except Exception as e:
                            db_update.rollback()
                            logger.error(f"Ошибка при сохранении отправленного объявления: {e}")
                        finally:
                            db_update.close()
                            
                    except Exception as e:
                        logger.error(f"Ошибка при отправке сообщения: {e}")
                
                # Обновляем время последнего уведомления - используем новую сессию
                db_notification = Session()
                try:
                    # Получаем свежие данные о настройках уведомлений
                    notification = db_notification.query(NotificationSetting).filter(NotificationSetting.user_id == user_id_db).first()
                    if notification:
                        notification.last_sent_at = datetime.now()
                        db_notification.commit()
                except Exception as e:
                    db_notification.rollback()
                    logger.error(f"Ошибка при обновлении времени последнего уведомления: {e}")
                finally:
                    db_notification.close()
                
                logger.info(f"Отправлено {sent_count} объявлений пользователю {user_id}")
            else:
                logger.info(f"No matching properties found for user {user_id}")
                try:
                    await bot.send_message(
                        chat_id=user_id,
                        text="На данный момент не найдено новых объявлений, соответствующих вашим критериям."
                    )
                except Exception as e:
                    logger.error(f"Ошибка при отправке сообщения об отсутствии объявлений: {e}")
        
        except Exception as e:
            logger.error(f"Необработанная ошибка в процессе отправки уведомлений для {user_id}: {e}", exc_info=True)
        
    except Exception as e:
        logger.error(f"Ошибка при отправке уведомлений для {user_id}: {e}", exc_info=True)
    finally:
        if 'db' in locals() and db:
            try:
                db.close()
                logger.info(f"Final database connection closed for user {user_id}")
            except:
                pass


async def handle_districts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод районов пользователем."""
    text = update.message.text
    
    # Split the input by commas and clean up spaces - make lowercase for case-insensitive comparison
    districts = [district.strip() for district in text.split(',') if district.strip()]
    
    if not districts:
        await update.message.reply_text(
            "Пожалуйста, введите хотя бы один район:"
        )
        return DISTRICTS
    
    # Update in database
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
        if user and user.filters:
            user.filters[0].districts = districts
            db.commit()
            
            await update.message.reply_text(
                f"Районы для поиска установлены: {', '.join(districts)}. Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()
            )
            return FILTER_MENU
    except Exception as e:
        logger.error(f"Ошибка при обновлении районов: {e}")
        await update.message.reply_text(
            "Произошла ошибка при сохранении районов. Пожалуйста, попробуйте еще раз."
        )
        return DISTRICTS
    finally:
        db.close()


# Функция для отображения текущих фильтров
async def show_current_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает текущие фильтры пользователя и предлагает их сбросить."""
    user_id = update.effective_user.id
    
    logger.info(f"Showing current filters for user {user_id}")
    
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.filters:
            await update.message.reply_text(
                "У вас еще не настроены фильтры. Используйте меню 'Настройка фильтров'."
            )
            return MAIN_MENU
        
        user_filter = user.filters[0]
        
        # Формируем сообщение со списком текущих фильтров
        message = "🔍 *Ваши текущие фильтры:*\n\n"
        
        # Год постройки
        if user_filter.year_min is not None or user_filter.year_max is not None:
            year_min = user_filter.year_min or "не указан"
            year_max = user_filter.year_max or "не указан"
            message += f"🏢 *Год постройки:* {year_min} - {year_max}\n"
        else:
            message += "🏢 *Год постройки:* не указан\n"
        
        # Районы
        if user_filter.districts and len(user_filter.districts) > 0:
            message += f"🏙️ *Районы:* {', '.join(user_filter.districts)}\n"
        else:
            message += "🏙️ *Районы:* не указаны\n"
        
        # Город
        if user_filter.city:
            message += f"🌃 *Город:* {user_filter.city}\n"
        else:
            message += "🌃 *Город:* не указан\n"
            
        # Адрес
        if user_filter.address:
            message += f"📍 *Адрес:* {user_filter.address}\n"
        else:
            message += "📍 *Адрес:* не указан\n"
        
        # Этажи
        floor_filters = []
        if user_filter.not_first_floor:
            floor_filters.append("не первый")
        if user_filter.not_last_floor:
            floor_filters.append("не последний")
        if user_filter.min_floor is not None:
            floor_filters.append(f"от {user_filter.min_floor}")
        if user_filter.max_floor is not None:
            floor_filters.append(f"до {user_filter.max_floor}")
        
        if floor_filters:
            message += f"🔢 *Этажи:* {', '.join(floor_filters)}\n"
        else:
            message += "🔢 *Этажи:* не указаны\n"
        
        # Комнаты
        if user_filter.rooms_min is not None or user_filter.rooms_max is not None:
            if user_filter.rooms_min == user_filter.rooms_max:
                message += f"🚪 *Комнаты:* {user_filter.rooms_min}\n"
            else:
                rooms_min = user_filter.rooms_min or "не указано"
                rooms_max = user_filter.rooms_max or "не указано"
                message += f"🚪 *Комнаты:* {rooms_min} - {rooms_max}\n"
        else:
            message += "🚪 *Комнаты:* не указаны\n"
        
        # Цена
        if user_filter.price_min is not None or user_filter.price_max is not None:
            price_min = f"{user_filter.price_min:,}" if user_filter.price_min else "не указана"
            price_max = f"{user_filter.price_max:,}" if user_filter.price_max else "не указана"
            message += f"💰 *Цена:* {price_min} - {price_max} тенге\n"
        else:
            message += "💰 *Цена:* не указана\n"
        
        # Площадь
        if user_filter.area_min is not None or user_filter.area_max is not None:
            area_min = user_filter.area_min or "не указана"
            area_max = user_filter.area_max or "не указана"
            message += f"📏 *Площадь:* {area_min} - {area_max} м²\n"
        else:
            message += "📏 *Площадь:* не указана\n"
        
        # Процент от рыночной цены
        if user_filter.max_market_price_percent is not None:
            message += f"📉 *От рыночной цены:* до {user_filter.max_market_price_percent}%\n"
        else:
            message += "📉 *От рыночной цены:* не указан\n"
        
        # Создаем клавиатуру для сброса фильтров
        keyboard = [
            [InlineKeyboardButton("🔄 Сбросить все фильтры", callback_data="reset_all_filters")],
            [InlineKeyboardButton("🏢 Сбросить год постройки", callback_data="reset_filter_year")],
            [InlineKeyboardButton("🏙️ Сбросить районы", callback_data="reset_filter_districts")],
            [InlineKeyboardButton("🌃 Сбросить город", callback_data="reset_filter_city")],
            [InlineKeyboardButton("📍 Сбросить адрес", callback_data="reset_filter_address")],
            [InlineKeyboardButton("🔢 Сбросить этажи", callback_data="reset_filter_floors")],
            [InlineKeyboardButton("🚪 Сбросить комнаты", callback_data="reset_filter_rooms")],
            [InlineKeyboardButton("💰 Сбросить цену", callback_data="reset_filter_price")],
            [InlineKeyboardButton("📏 Сбросить площадь", callback_data="reset_filter_area")],
            [InlineKeyboardButton("📉 Сбросить % от рынка", callback_data="reset_filter_market")]
        ]
        
        sent_message = await update.message.reply_text(
            message,
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )
        
        logger.info(f"Displayed filter information and reset buttons for user {user_id}")
        
        # State transition to RESET_FILTERS
        return RESET_FILTERS
        
    except Exception as e:
        logger.error(f"Ошибка при отображении фильтров: {e}")
        await update.message.reply_text(
            "Произошла ошибка при загрузке ваших фильтров. Пожалуйста, попробуйте позже."
        )
        return MAIN_MENU
    finally:
        db.close()


async def handle_reset_filters(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает сброс фильтров пользователя."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data
    
    logger.info(f"Reset filter request: {data} for user {user_id}")
    
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == user_id).first()
        if not user or not user.filters:
            await query.edit_message_text(
                "У вас еще не настроены фильтры. Используйте меню 'Настройка фильтров'."
            )
            await query.message.reply_text(
                "Выберите действие:",
                reply_markup=get_main_keyboard()
            )
            return MAIN_MENU
        
        user_filter = user.filters[0]
        
        if data == "reset_all_filters":
            # Log before update
            logger.info(f"Before reset - Year: {user_filter.year_min}-{user_filter.year_max}, Districts: {user_filter.districts}")
            
            # Сбрасываем все фильтры на значения по умолчанию
            user_filter.year_min = None
            user_filter.year_max = None
            user_filter.districts = []
            user_filter.not_first_floor = False
            user_filter.not_last_floor = False
            user_filter.min_floor = None
            user_filter.max_floor = None
            user_filter.rooms_min = None
            user_filter.rooms_max = None
            user_filter.price_min = None
            user_filter.price_max = None
            user_filter.area_min = None
            user_filter.area_max = None
            user_filter.max_market_price_percent = 0.0
            user_filter.city = None
            user_filter.address = None
            
            # Log after update but before commit
            logger.info(f"After reset, before commit - Year: {user_filter.year_min}-{user_filter.year_max}, Districts: {user_filter.districts}")
            
            db.commit()
            
            # Log after commit
            logger.info("Reset all filters - Commit successful")
            
            # Verify the changes were saved
            db.refresh(user_filter)
            logger.info(f"After commit and refresh - Year: {user_filter.year_min}-{user_filter.year_max}, Districts: {user_filter.districts}")
            
            await query.edit_message_text(
                "Все фильтры успешно сброшены!"
            )
        elif data == "reset_filter_year":
            # Сбрасываем фильтр года постройки
            logger.info(f"Before reset - Year: {user_filter.year_min}-{user_filter.year_max}")
            user_filter.year_min = None
            user_filter.year_max = None
            db.commit()
            logger.info("Reset year filter - Commit successful")
            
            # Verify
            db.refresh(user_filter)
            logger.info(f"After commit - Year: {user_filter.year_min}-{user_filter.year_max}")
            
            await query.edit_message_text(
                "Фильтр года постройки сброшен. Теперь будут показаны объявления с любым годом постройки."
            )
        elif data == "reset_filter_districts":
            # Сбрасываем фильтр районов
            logger.info(f"Before reset - Districts: {user_filter.districts}")
            user_filter.districts = []
            db.commit()
            logger.info("Reset districts filter - Commit successful")
            
            # Verify
            db.refresh(user_filter)
            logger.info(f"After commit - Districts: {user_filter.districts}")
            
            await query.edit_message_text(
                "Фильтр районов сброшен. Теперь будут показаны объявления из всех районов."
            )
        elif data == "reset_filter_city":
            # Сбрасываем фильтр города
            logger.info(f"Before reset - City: {user_filter.city}")
            user_filter.city = None
            db.commit()
            logger.info("Reset city filter - Commit successful")
            
            # Verify
            db.refresh(user_filter)
            logger.info(f"After commit - City: {user_filter.city}")
            
            await query.edit_message_text(
                "Фильтр города сброшен. Теперь будут показаны объявления из всех городов."
            )
        elif data == "reset_filter_address":
            # Сбрасываем фильтр адреса
            logger.info(f"Before reset - Address: {user_filter.address}")
            user_filter.address = None
            db.commit()
            logger.info("Reset address filter - Commit successful")
            
            # Verify
            db.refresh(user_filter)
            logger.info(f"After commit - Address: {user_filter.address}")
            
            await query.edit_message_text(
                "Фильтр адреса сброшен. Теперь будут показаны объявления с любыми адресами."
            )
        elif data == "reset_filter_floors":
            # Сбрасываем фильтр этажей
            logger.info(f"Before reset - Floors: min={user_filter.min_floor}, max={user_filter.max_floor}, not_first={user_filter.not_first_floor}, not_last={user_filter.not_last_floor}")
            user_filter.not_first_floor = False
            user_filter.not_last_floor = False
            user_filter.min_floor = None
            user_filter.max_floor = None
            db.commit()
            logger.info("Reset floors filter - Commit successful")
            
            await query.edit_message_text(
                "Фильтр этажей сброшен. Теперь будут показаны объявления с любым этажом."
            )
        elif data == "reset_filter_rooms":
            # Сбрасываем фильтр комнат
            logger.info(f"Before reset - Rooms: min={user_filter.rooms_min}, max={user_filter.rooms_max}")
            user_filter.rooms_min = None
            user_filter.rooms_max = None
            db.commit()
            logger.info("Reset rooms filter - Commit successful")
            
            await query.edit_message_text(
                "Фильтр комнат сброшен. Теперь будут показаны объявления с любым количеством комнат."
            )
        elif data == "reset_filter_price":
            # Сбрасываем фильтр цены
            logger.info(f"Before reset - Price: min={user_filter.price_min}, max={user_filter.price_max}")
            user_filter.price_min = None
            user_filter.price_max = None
            db.commit()
            logger.info("Reset price filter - Commit successful")
            
            await query.edit_message_text(
                "Фильтр цены сброшен. Теперь будут показаны объявления с любой ценой."
            )
        elif data == "reset_filter_area":
            # Сбрасываем фильтр площади
            logger.info(f"Before reset - Area: min={user_filter.area_min}, max={user_filter.area_max}")
            user_filter.area_min = None
            user_filter.area_max = None
            db.commit()
            logger.info("Reset area filter - Commit successful")
            
            await query.edit_message_text(
                "Фильтр площади сброшен. Теперь будут показаны объявления с любой площадью."
            )
        elif data == "reset_filter_market":
            # Сбрасываем фильтр процента от рыночной цены
            logger.info(f"Before reset - Market percent: {user_filter.max_market_price_percent}")
            user_filter.max_market_price_percent = 0.0
            db.commit()
            logger.info("Reset market percent filter - Commit successful")
            
            await query.edit_message_text(
                "Фильтр процента от рыночной цены сброшен. Теперь будут показаны объявления с любым процентом."
            )
        else:
            logger.warning(f"Unknown filter reset command: {data}")
            await query.edit_message_text(
                "Неизвестный тип фильтра. Пожалуйста, попробуйте еще раз."
            )
            await query.message.reply_text(
                "Выберите действие:",
                reply_markup=get_main_keyboard()
            )
            return MAIN_MENU
        
        # После сброса фильтров перезапускаем планировщик для обновления уведомлений
        try:
            restart_user_scheduler(user_id)
            logger.info(f"Restarted scheduler for user {user_id}")
        except Exception as e:
            logger.error(f"Error restarting scheduler: {e}")
        
        # Отправляем сообщение с предложением вернуться в главное меню
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        
        logger.info(f"Filter reset completed successfully for {data}, user {user_id}")
        return MAIN_MENU
        
    except Exception as e:
        logger.error(f"Ошибка при сбросе фильтров: {e}", exc_info=True)
        await query.edit_message_text(
            "Произошла ошибка при сбросе фильтров. Пожалуйста, попробуйте позже."
        )
        await query.message.reply_text(
            "Выберите действие:",
            reply_markup=get_main_keyboard()
        )
        return MAIN_MENU
    finally:
        if 'db' in locals() and db:
            db.close()
            logger.info("Database connection closed")


async def handle_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод города."""
    city = update.message.text.strip()
    
    if not city:
        await update.message.reply_text(
            "Пожалуйста, введите название города:"
        )
        return CITY
    
    # Update in database
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
        if user and user.filters:
            user.filters[0].city = city
            db.commit()
            
            await update.message.reply_text(
                f"Город для поиска установлен: {city}. Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()
            )
            return FILTER_MENU
    except Exception as e:
        logger.error(f"Ошибка при обновлении города: {e}")
        await update.message.reply_text(
            "Произошла ошибка при сохранении города. Пожалуйста, попробуйте еще раз."
        )
        return CITY
    finally:
        db.close()


async def handle_address(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает ввод адреса."""
    address = update.message.text.strip()
    
    if not address:
        await update.message.reply_text(
            "Пожалуйста, введите часть адреса:"
        )
        return ADDRESS
    
    # Update in database
    db = Session()
    try:
        user = db.query(User).filter(User.telegram_id == update.effective_user.id).first()
        if user and user.filters:
            user.filters[0].address = address
            db.commit()
            
            await update.message.reply_text(
                f"Часть адреса для поиска установлена: {address}. Выберите параметр для настройки:",
                reply_markup=get_filter_menu_keyboard()
            )
            return FILTER_MENU
    except Exception as e:
        logger.error(f"Ошибка при обновлении адреса: {e}")
        await update.message.reply_text(
            "Произошла ошибка при сохранении адреса. Пожалуйста, попробуйте еще раз."
        )
        return ADDRESS
    finally:
        db.close()


async def admin_overall_statistics(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает общую статистику для администратора."""
    user_id = update.effective_user.id
    
    # Проверка прав администратора
    if user_id != ADMIN_TELEGRAM_ID:
        logger.warning(f"Non-admin user {user_id} tried to access admin_overall_statistics")
        await update.message.reply_text(
            "У вас нет прав для выполнения этой команды.",
            reply_markup=get_main_keyboard(is_admin=False)
        )
        return MAIN_MENU
    
    db = Session()
    try:
        # Общая статистика пользователей
        total_users = db.query(User).count()
        active_users = db.query(User).join(NotificationSetting).filter(NotificationSetting.enabled == True).count()
        
        # Статистика по фильтрам
        filters_count = db.query(UserFilter).count()
        
        # Статистика по отправленным объявлениям
        total_sent = db.query(SentProperty).count()
        
        # Наиболее популярные районы
        district_counts = {}
        user_filters = db.query(UserFilter).all()
        for uf in user_filters:
            if uf.districts:
                for district in uf.districts:
                    district_counts[district] = district_counts.get(district, 0) + 1
        
        popular_districts = sorted(district_counts.items(), key=lambda x: x[1], reverse=True)
        
        # Статистика по годам постройки
        year_min_avg = db.query(func.avg(UserFilter.year_min)).filter(UserFilter.year_min != None).scalar() or 0
        year_max_avg = db.query(func.avg(UserFilter.year_max)).filter(UserFilter.year_max != None).scalar() or 0
        
        # Статистика по ценам
        price_min_avg = db.query(func.avg(UserFilter.price_min)).filter(UserFilter.price_min != None).scalar() or 0
        price_max_avg = db.query(func.avg(UserFilter.price_max)).filter(UserFilter.price_max != None).scalar() or 0
        
        # Формируем сообщение
        message = "📊 *Общая статистика бота*\n\n"
        message += f"👥 *Пользователи:*\n"
        message += f"  • Всего: {total_users}\n"
        message += f"  • С активными уведомлениями: {active_users}\n\n"
        
        message += f"🔍 *Фильтры:*\n"
        message += f"  • Всего настроено: {filters_count}\n"
        message += f"  • Средний диапазон годов: {int(year_min_avg)} - {int(year_max_avg)}\n"
        message += f"  • Средний диапазон цен: {int(price_min_avg):,} - {int(price_max_avg):,} тенге\n\n"
        
        message += f"📬 *Отправленные объявления:*\n"
        message += f"  • Всего отправлено: {total_sent}\n\n"
        
        message += f"🏙️ *Популярные районы:*\n"
        for district, count in popular_districts[:5]:
            message += f"  • {district}: {count} пользователей\n"
        
        await update.message.reply_text(
            message,
            parse_mode="Markdown",
            reply_markup=get_main_keyboard(is_admin=True)
        )
    except Exception as e:
        logger.error(f"Ошибка при получении общей статистики: {e}")
        await update.message.reply_text(
            "Произошла ошибка при загрузке статистики. Пожалуйста, попробуйте позже.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
    finally:
        db.close()


async def handle_admin_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает отправку сообщения всем пользователям."""
    user_id = update.effective_user.id
    
    # Проверка прав администратора
    if user_id != ADMIN_TELEGRAM_ID:
        logger.warning(f"Non-admin user {user_id} tried to access handle_admin_broadcast")
        await update.message.reply_text(
            "У вас нет прав для выполнения этой команды.",
            reply_markup=get_main_keyboard(is_admin=False)
        )
        return MAIN_MENU
    
    message_text = update.message.text
    if not message_text or message_text.strip() == "":
        await update.message.reply_text(
            "Пожалуйста, введите текст сообщения для отправки.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
        return ADMIN_BROADCAST
    
    # Начинаем отправку
    await update.message.reply_text(
        "Начинаю отправку сообщения всем пользователям..."
    )
    
    db = Session()
    try:
        # Получаем всех пользователей
        users = db.query(User).all()
        sent_count = 0
        failed_count = 0
        
        for user in users:
            try:
                await context.bot.send_message(
                    chat_id=user.telegram_id,
                    text=f"📢 *Сообщение от администратора:*\n\n{message_text}",
                    parse_mode="Markdown"
                )
                sent_count += 1
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения пользователю {user.telegram_id}: {e}")
                failed_count += 1
        
        await update.message.reply_text(
            f"✅ Отправка завершена!\n\n"
            f"Сообщение отправлено {sent_count} пользователям.\n"
            f"Не удалось отправить {failed_count} пользователям.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
    except Exception as e:
        logger.error(f"Ошибка при массовой отправке сообщений: {e}")
        await update.message.reply_text(
            "Произошла ошибка при отправке сообщений. Пожалуйста, попробуйте позже.",
            reply_markup=get_main_keyboard(is_admin=True)
        )
    finally:
        db.close()
    
    return MAIN_MENU


def main():
    """Запускает бота."""
    # Создаем lock-файл для предотвращения запуска нескольких экземпляров
    lock_file = "/tmp/krisha_tg_bot.lock"
    
    try:
        # Try to create a lock file with exclusive access
        lock_fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        
        # Write PID to lock file
        os.write(lock_fd, str(os.getpid()).encode())
        os.close(lock_fd)
        
        # Register cleanup to remove lock on exit
        import atexit
        atexit.register(lambda: os.unlink(lock_file) if os.path.exists(lock_file) else None)
        
        logger.info("Bot starting with lock file created")
        
        # Инициализация базы данных
        Base.metadata.create_all(bind=engine)

        # Configure the application
        application = Application.builder() \
            .token(TOKEN) \
            .post_init(on_startup) \
            .build()

        # Create a separate handler for reset filter callbacks
        reset_filter_handler = CallbackQueryHandler(handle_reset_filters, pattern='^(reset_all_filters|reset_filter_year|reset_filter_districts|reset_filter_city|reset_filter_address|reset_filter_floors|reset_filter_rooms|reset_filter_price|reset_filter_area|reset_filter_market)')
        
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
                    CallbackQueryHandler(handle_filter_menu),
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_districts)
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
                CITY: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_city)
                ],
                ADDRESS: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_address)
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
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_notification_interval)
                ],
                RESET_FILTERS: [
                    reset_filter_handler,
                ],
                ADMIN_BROADCAST: [
                    MessageHandler(filters.TEXT & ~filters.COMMAND, handle_admin_broadcast)
                ]
            },
            fallbacks=[CommandHandler("cancel", cancel)],
            per_message=False,  # Changed to False to allow mixed handler types
            name="main_conversation"
        )

        application.add_error_handler(error_handler)
        application.add_handler(reset_filter_handler)  # Add the handler outside the conversation too
        application.add_handler(conv_handler)
        
        # Add direct command handlers for essential commands
        application.add_handler(CommandHandler("start", start))
        application.add_handler(CommandHandler("help", help_command))
        application.add_handler(CommandHandler("reset", reset_sent_properties))
        
        # Add a general message handler with lower priority to catch any text messages outside conversation
        application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main_menu), group=1)

        # Настраиваем планировщики при запуске
        setup_schedulers()
        
        # Запускаем бота с простыми настройками
        logger.info("Starting bot polling")
        application.run_polling(drop_pending_updates=True)
        
    except OSError as e:
        import errno, sys
        if e.errno == errno.EEXIST:
            logger.error("Another instance of the bot is already running. Exiting.")
            try:
                # Try to read PID from existing lock file
                with open(lock_file, 'r') as f:
                    pid = f.read().strip()
                    logger.error(f"Bot is already running with PID: {pid}")
                    
                # Check if process is actually running
                try:
                    os.kill(int(pid), 0)  # Signal 0 doesn't kill but checks if process exists
                except (OSError, ValueError):
                    logger.warning("Stale lock file detected. The previous instance may have crashed.")
                    # You can choose to remove stale lock and retry
                    logger.info("Removing stale lock file...")
                    os.unlink(lock_file)
                    logger.info("Please restart the bot.")
            except (IOError, ValueError) as read_error:
                logger.error(f"Error reading lock file: {read_error}")
            
            sys.exit(1)
        else:
            # Other OS errors
            logger.error(f"OS error: {e}")
            sys.exit(1)


if __name__ == "__main__":
    main()
