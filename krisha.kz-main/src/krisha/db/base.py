# src/krisha/db/base.py
import logging
import time
import psycopg2

logger = logging.getLogger()

class DBConnection:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        
        self.connection = self._connect()
        self._is_closed = False

    def _connect(self):
        """Create a new database connection."""
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        logger.debug("Created new database connection")
        return connection

    def reconnect(self, max_attempts=3, retry_delay=2.0):
        """Reconnect to database if the connection is closed or broken."""
        if not self._is_closed:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Error closing existing connection: {e}")
                # Continue with reconnect attempt even if close fails
        
        self._is_closed = True
        
        # Try to reconnect with exponential backoff
        for attempt in range(1, max_attempts + 1):
            try:
                logger.info(f"Attempting database reconnection (attempt {attempt}/{max_attempts})")
                self.connection = self._connect()
                self._is_closed = False
                logger.info("Database reconnection successful")
                return True
            except Exception as e:
                logger.error(f"Reconnection attempt {attempt} failed: {e}")
                if attempt < max_attempts:
                    sleep_time = retry_delay * (2 ** (attempt - 1))
                    logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
        
        logger.critical("All database reconnection attempts failed")
        raise ConnectionError("Could not reconnect to database after maximum attempts")

    def __enter__(self):
        return self  # Return the DBConnection instance instead of the raw connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self._is_closed:
            self.connection.close()
            self._is_closed = True

    def close(self):
        """Explicitly close connection if not using context manager"""
        if not self._is_closed:
            self.connection.close()
            self._is_closed = True
            logger.debug("Closed database connection")