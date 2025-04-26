#!/bin/bash
set -e

# Create the 'krisha' database if it doesn't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'CREATE DATABASE krisha'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'krisha')
    \gexec
EOSQL

# Connect to the 'krisha' database and create tables if they don't exist
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "krisha" <<-EOSQL
    -- Create flats table if it doesn't exist
    CREATE TABLE IF NOT EXISTS flats
    (
        id          SERIAL PRIMARY KEY,
        uuid        TEXT    NOT NULL UNIQUE,
        url         TEXT    NOT NULL,
        room        INTEGER,
        square      INTEGER,
        city        TEXT,
        lat         REAL,
        lon         REAL,
        description TEXT,
        address     TEXT,
        title       VARCHAR(255),
        star        INTEGER DEFAULT 0,
        focus       INTEGER DEFAULT 0
    );

    -- Create prices table if it doesn't exist
    CREATE TABLE IF NOT EXISTS prices
    (
        id       SERIAL PRIMARY KEY,
        date     DATE DEFAULT CURRENT_DATE,
        flat_id  INTEGER NOT NULL REFERENCES flats(id),
        price    INTEGER NOT NULL,
        green_percentage FLOAT,
        UNIQUE (date, flat_id)
    );
    
    -- Create indexes to improve query performance and reduce lock contention
    CREATE INDEX IF NOT EXISTS idx_prices_flat_id ON prices(flat_id);
    CREATE INDEX IF NOT EXISTS idx_prices_date ON prices(date);
    CREATE INDEX IF NOT EXISTS idx_prices_flat_id_date ON prices(flat_id, date);

    -- Create telegram_users table if it doesn't exist
    CREATE TABLE IF NOT EXISTS telegram_users
    (
        id          SERIAL PRIMARY KEY,
        telegram_id BIGINT UNIQUE NOT NULL,
        username    TEXT,
        first_name  TEXT NOT NULL,
        last_name   TEXT,
        created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create user_filters table if it doesn't exist
    CREATE TABLE IF NOT EXISTS user_filters
    (
        id                     SERIAL PRIMARY KEY,
        user_id                INTEGER REFERENCES telegram_users(id) ON DELETE CASCADE,
        year_min               INTEGER,
        year_max               INTEGER,
        districts              TEXT[],
        min_floor              INTEGER,
        max_floor              INTEGER,
        rooms_min              INTEGER,
        rooms_max              INTEGER,
        price_min              INTEGER,
        price_max              INTEGER,
        area_min               INTEGER,
        area_max               INTEGER,
        max_market_price_percent FLOAT,
        city                   TEXT,
        address                TEXT
    );

    -- Create notification_settings table if it doesn't exist
    CREATE TABLE IF NOT EXISTS notification_settings
    (
        id          SERIAL PRIMARY KEY,
        user_id     INTEGER REFERENCES telegram_users(id) ON DELETE CASCADE,
        enabled     BOOLEAN DEFAULT true,
        notify_time TIME DEFAULT '12:00:00',
        interval_hours INTEGER DEFAULT 24
    );
    
    -- Create sent_properties table if it doesn't exist
    CREATE TABLE IF NOT EXISTS sent_properties
    (
        id          SERIAL PRIMARY KEY,
        user_id     INTEGER REFERENCES telegram_users(id) ON DELETE CASCADE,
        property_id INTEGER NOT NULL,
        sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (user_id, property_id)
    );
EOSQL

echo "Database initialization completed successfully" 