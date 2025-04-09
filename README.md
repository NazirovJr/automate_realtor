# 🏠 Парсер недвижимости Krisha.kz + Telegram Bot

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)](https://www.python.org/)
[![Telegram Bot](https://img.shields.io/badge/Telegram-Bot-blue.svg)](https://core.telegram.org/bots)

Умный парсер для поиска квартир с расширенными возможностями:
- 🕵️‍♂️ **Гибкий поиск** по 15+ параметрам
- 📊 **Анализ цен** относительно рыночных
- 🔔 **Авто-уведомления** в Telegram
- 📈 **Мониторинг изменений** цен

![Bot Demo](https://i.imgur.com/5X6Jz9L.gif)

## 🌟 Особенности

### 🤖 Умные уведомления
```python
# Пример ежедневных уведомлений
await bot.send_message(
    chat_id=user_id,
    text="🔔 Новые предложения будут приходить каждый день в 10:00"
)
```
- **Гибкое расписание**: ежедневно/каждый N часов
- **Персонализированные фильтры**:
  ```json
  {
    "price": {"min": 150000, "max": 300000},
    "rooms": [1, 2],
    "floor": {"not_first": true}
  }
  ```

### ⚙️ Технологии
| Категория       | Стек                     |
|-----------------|--------------------------|
| **Парсинг**     | BeautifulSoup4, Requests |
| **БД**          | PostgreSQL, SQLite       |
| **Бот**         | python-telegram-bot 20+  |
| **Аналитика**   | Pandas, Matplotlib       |

## 🚀 Установка
```bash
git clone https://github.com/NazirovJr/automate_realtor.git
cd krisha.kz-main
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

## ⚡ Возможности бота

### 🎛 Главное меню
```python
KEYBOARD = [
    ["🔍 Поиск", "⚙️ Настройки"],
    ["📊 Статистика", "🔔 Уведомления"]
]
```

### 🔧 Фильтры
- 🏙 Районы
- 🏗 Год постройки
- 🪜 Этажность
- 💰 Цена/м²

## 🕒 Планировщик
```python
scheduler.add_job(
    send_notification,
    CronTrigger(hour=10, minute=30),
    args=[user_id, bot]
)
```

## 📄 Параметры поиска
```json
{
  "city": 1,
  "price_range": [150000, 350000],
  "rooms": [1, 2]
   .....
}
```

## 📈 Пример уведомления
```
🏠 *2-комн. квартира*  
🏙️ Район: Бостандыкский  
🏢 Год: 2018  
💰 Цена: 250,000 ₸ (-15% рынка)  
🔗 [Подробнее](https://krisha.kz/123)
```

## 📮 Контакты
- ✉️ [naziroffjr@gmail.com](mailto:naziroffjr@gmail.com)
- 📱 [Telegram](https://t.me/NJR_Ilhom)

---
