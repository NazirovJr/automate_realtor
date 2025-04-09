# src/krisha/db/base.py
import psycopg2


class DBConnection:
    def __init__(self, host: str, port: int, dbname: str, user: str, password: str):
        self.connection = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
        )
        self._is_closed = False

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