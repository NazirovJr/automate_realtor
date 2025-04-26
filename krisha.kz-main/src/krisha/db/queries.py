import logging
import time
import random
import psycopg2

import src.krisha.common.msg as msg
from src.krisha.crawler.flat_parser import Flat
from src.krisha.db.base import DBConnection

logger = logging.getLogger()


def insert_flats_data_db(
        connector: DBConnection,
        flats_data: list[Flat],
        max_retries: int = 5,
        initial_retry_delay: float = 1.0
) -> None:
    """Insert flats data to DB with enhanced deadlock handling and retry logic."""
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
            flat.room or None,
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
            getattr(flat, 'green_percentage', None)
        )
        for flat in flats_data
    ]

    # Use smaller batch size to reduce lock contention
    batch_size = 20  # Reduced from 50 to 20
    flats_batches = [flats_values[i:i + batch_size] for i in range(0, len(flats_values), batch_size)]
    prices_batches = [prices_values[i:i + batch_size] for i in range(0, len(prices_values), batch_size)]

    overall_success = True
    
    # Process each batch with enhanced retry logic
    for batch_idx in range(len(flats_batches)):
        retry_count = 0
        success = False
        
        # Add jitter to avoid retry storms when multiple processes retry simultaneously
        jitter = lambda retry: random.uniform(0.8, 1.2)

        while not success and retry_count < max_retries:
            cursor = None
            try:
                # Get cursor from existing connection
                cursor = connector.connection.cursor()
                
                # Set transaction isolation level to READ COMMITTED to reduce conflicts
                cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
                
                # Process flats and prices separately to reduce transaction time
                cursor.executemany(insert_flats_query, flats_batches[batch_idx])
                connector.connection.commit()
                
                cursor.executemany(insert_price_query, prices_batches[batch_idx])
                connector.connection.commit()
                
                logger.info(f"Database - Batch {batch_idx+1}/{len(flats_batches)} successfully inserted")
                success = True
                
            except psycopg2.errors.DeadlockDetected as e:
                retry_count += 1
                connector.connection.rollback()
                
                logger.warning(f"Deadlock detected on batch {batch_idx+1}, attempt {retry_count}/{max_retries}: {e}")
                
                if retry_count < max_retries:
                    # Exponential backoff with jitter
                    sleep_time = initial_retry_delay * (2 ** (retry_count - 1)) * jitter(retry_count)
                    logger.info(f"Retrying batch {batch_idx+1} in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Maximum retries reached for batch {batch_idx+1}. Continuing with next batch.")
                    overall_success = False
                    
            except Exception as e:
                logger.error(f"Database error in batch {batch_idx+1}: {e}")
                connector.connection.rollback()
                
                retry_count += 1
                if retry_count < max_retries and (
                    isinstance(e, psycopg2.OperationalError) or 
                    "deadlock" in str(e).lower() or 
                    "conflict" in str(e).lower()
                ):
                    # Retry certain types of errors
                    sleep_time = initial_retry_delay * (2 ** (retry_count - 1)) * jitter(retry_count)
                    logger.info(f"Retrying batch {batch_idx+1} after error in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Skipping batch {batch_idx+1} due to non-recoverable error")
                    overall_success = False
                    break

            finally:
                if cursor:
                    cursor.close()
    
    if overall_success:
        logger.info(msg.DB_INSERT_OK)
    else:
        logger.warning("Database insert completed with some errors. Some data may not have been saved.")


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