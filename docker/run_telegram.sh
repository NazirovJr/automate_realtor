#!/bin/bash

# Извлекаем параметры подключения к базе данных из переменной окружения
if [ -n "$DATABASE_URL" ]; then
  # Прямое назначение переменных для PostgreSQL без сложного парсинга
  DB_USER="postgres"
  DB_PASSWORD="postgres"
  DB_HOST="db"
  DB_PORT="5432"
  DB_NAME="krisha"
  
  echo "Using database connection parameters from environment"
else
  # Значения по умолчанию
  DB_USER="postgres"
  DB_PASSWORD="postgres"
  DB_HOST="db"
  DB_PORT="5432"
  DB_NAME="krisha"
  
  echo "Using default database connection parameters"
fi

# Ждем, пока база данных станет доступной
echo "Waiting for database to be ready..."
until PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -c '\q'; do
  >&2 echo "Database is unavailable - sleeping"
  sleep 1
done

echo "Database is up - applying migrations"

# Применяем миграцию
for file in /app/migrations/*.sql; do
  echo "Applying migration: $file"
  PGPASSWORD=$DB_PASSWORD psql -h $DB_HOST -U $DB_USER -d $DB_NAME -f "$file"
done

echo "Migrations applied - starting Telegram bot"

# Запускаем бот
exec python tg.py 