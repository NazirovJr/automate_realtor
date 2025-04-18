import logging

import src.krisha.common.msg as msg
from src.krisha.crawler.flat_parser import Flat
from src.krisha.db.base import DBConnection

logger = logging.getLogger()


def insert_flats_data_db(
        connector: DBConnection,
        flats_data: list[Flat],
) -> None:
    """Insert flats data to DB with proper connection handling."""
    insert_flats_query = """
        INSERT INTO flats(
            id,
            uuid,
            url,
            room,
            square,
            city,
            lat,
            lon,
            description,
            address,
            title
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            url = EXCLUDED.url,
            room = EXCLUDED.room,
            square = EXCLUDED.square,
            city = EXCLUDED.city,
            lat = EXCLUDED.lat,
            lon = EXCLUDED.lon,
            description = EXCLUDED.description,
            address = EXCLUDED.address,
            title = EXCLUDED.title;
    """

    insert_price_query = """
        INSERT INTO prices(
            flat_id,
            price,
            green_percentage
        )
        VALUES (%s, %s, %s)
        ON CONFLICT (date, flat_id) DO UPDATE SET
            price = EXCLUDED.price,
            green_percentage = EXCLUDED.green_percentage;
    """

    # Prepare data tuples
    flats_values = [
        (
            flat.id,
            flat.uuid,
            flat.url,
            flat.room or None,  # Handle optional fields
            flat.square or None,
            flat.city,
            flat.lat,
            flat.lon,
            flat.description,
            flat.address,
            flat.title
        )
        for flat in flats_data
    ]

    prices_values = [
        (
            flat.id,
            flat.price,
            getattr(flat, 'green_percentage', None)  # Handle optional field
        )
        for flat in flats_data
    ]

    cursor = None
    try:
        # Get cursor from existing connection
        cursor = connector.connection.cursor()

        # Execute batch inserts
        cursor.executemany(insert_flats_query, flats_values)
        cursor.executemany(insert_price_query, prices_values)

        # Commit transaction
        connector.connection.commit()
        logger.info(msg.DB_INSERT_OK)

    except Exception as e:
        # Rollback on error
        logger.error(f"Database error: {e}")
        connector.connection.rollback()
        raise

    finally:
        # Always close cursor but keep connection open
        if cursor:
            cursor.close()


def check_flat_exists(connector: DBConnection, flat_id: int) -> bool:
    """Check if a flat with the given ID exists in the database."""
    query = "SELECT EXISTS(SELECT 1 FROM flats WHERE id = %s);"
    cursor = None
    try:
        cursor = connector.connection.cursor()
        cursor.execute(query, (flat_id,))
        exists = cursor.fetchone()[0]
        return exists
    except Exception as e:
        logger.error(f"Error checking flat existence: {e}")
        return False
    finally:
        if cursor:
            cursor.close()


def get_flat_price(connector: DBConnection, flat_id: int) -> int:
    """Get the latest price for a flat."""
    query = """
        SELECT price
        FROM prices
        WHERE flat_id = %s
        ORDER BY date DESC
        LIMIT 1
    """
    cursor = None
    try:
        cursor = connector.connection.cursor()
        cursor.execute(query, (flat_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error getting flat price: {e}")
        return None
    finally:
        if cursor:
            cursor.close()