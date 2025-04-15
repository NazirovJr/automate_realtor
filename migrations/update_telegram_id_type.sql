-- Удаляем связанные записи, чтобы не было конфликтов с внешними ключами
DELETE FROM sent_properties;
DELETE FROM notification_settings;
DELETE FROM user_filters;
DELETE FROM telegram_users;

-- Изменяем тип столбца telegram_id на bigint
ALTER TABLE telegram_users ALTER COLUMN telegram_id TYPE bigint; 