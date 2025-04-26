import logging
import sys
import time
import signal
import psycopg2

from src.krisha.config.config import load_config
from src.krisha.crawler.first_page import FirstPage
from src.krisha.crawler.spider import run_crawler
from src.krisha.db.service import get_connection

logger = logging.getLogger()

def handle_shutdown(signum, frame):
    """Handle shutdown signals gracefully."""
    logger.info("Received shutdown signal. Exiting gracefully...")
    sys.exit(0)

def main():
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    
    config = load_config()
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            with get_connection(config.path) as db_conn:
                first_page_url = FirstPage.get_url(config)
                
                # Log the search URL to help diagnose search parameter issues
                logger.info(f"Starting crawler with URL: {first_page_url}")
                
                # Run the crawler with the established database connection
                run_crawler(config, db_conn, first_page_url)
                
                # If crawler finishes successfully, break out of retry loop
                break
                
        except psycopg2.errors.DeadlockDetected as e:
            logger.error(f"Deadlock detected in main process (attempt {attempt}/{max_retries}): {e}")
            
            if attempt < max_retries:
                # Exponential backoff
                sleep_time = 5 * (2 ** (attempt - 1))
                logger.info(f"Retrying crawler in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.critical("Maximum retries reached after deadlocks. Giving up.")
                raise
                
        except psycopg2.OperationalError as e:
            logger.error(f"Database operational error (attempt {attempt}/{max_retries}): {e}")
            
            if attempt < max_retries:
                # Exponential backoff
                sleep_time = 10 * (2 ** (attempt - 1))
                logger.info(f"Retrying crawler in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logger.critical("Maximum retries reached after database errors. Giving up.")
                raise
                
        except Exception as e:
            # For other exceptions, log and exit immediately
            logger.critical(f"Unrecoverable error: {e}")
            raise
            
    logger.info("Crawler execution completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.critical(f"Critical error: {error}", exc_info=True)
        sys.exit(1)
