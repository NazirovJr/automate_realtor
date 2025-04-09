import logging

import src.krisha.common.msg as msg
from src.krisha.config.path import AppPaths
from src.krisha.db.base import DBConnection

logger = logging.getLogger()


# Update the create_db function in db/service.py
def create_db(connector: DBConnection) -> None:
    """Create DB tables with PostgreSQL-compatible schema."""
    query = """
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
            photo       TEXT,
            address     TEXT,
            title       VARCHAR(255),
            star        INTEGER DEFAULT 0,
            focus       INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS prices
        (
            id       SERIAL PRIMARY KEY,
            date     DATE DEFAULT CURRENT_DATE,
            flat_id  INTEGER NOT NULL REFERENCES flats(id),
            price    INTEGER NOT NULL,
            green_percentage FLOAT,
            UNIQUE (date, flat_id)
        );
    """
    with connector as con:
        with con.cursor() as cursor:
            cursor.execute(query)
            con.commit()
    logger.info(msg.DB_CREATED)


def check_table_exists(connector: DBConnection) -> bool:
    """Check if flats table exists."""
    query = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name = 'flats';
    """
    try:
        cursor = connector.connection.cursor()
        cursor.execute(query)
        exists = cursor.fetchone() is not None
        cursor.close()
        if exists:
            logger.info(msg.DB_OK)
        return exists
    except Exception as e:
        logger.error(f"Error checking table existence: {e}")
        return False

def check_db(connector: DBConnection) -> None:
    """Check DB."""
    if not check_table_exists(connector):
        create_db(connector)


# Update db/service.py to use new connection parameters
def get_connection(path: AppPaths) -> DBConnection:
    """Get PostgreSQL DB connection."""
    # Check the database using a temporary connection
    with DBConnection(
            host=path.db_host,
            port=path.db_port,
            dbname=path.db_name,
            user=path.db_user,
            password=path.db_password
    ) as temp_conn:
        check_db(temp_conn)

    # Return a new connection for the application
    return DBConnection(
        host=path.db_host,
        port=path.db_port,
        dbname=path.db_name,
        user=path.db_user,
        password=path.db_password
    )