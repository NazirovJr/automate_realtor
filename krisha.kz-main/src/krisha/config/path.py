# Modify the AppPaths dataclass in path.py
from dataclasses import dataclass
import os


@dataclass
class AppPaths:
    app_dir: str = "krisha"
    logs_dir: str = "logs"
    db_host: str = os.environ.get("DB_HOST", "localhost")
    db_port: int = int(os.environ.get("DB_PORT", "5432"))
    db_name: str = os.environ.get("DB_NAME", "krisha")
    db_user: str = os.environ.get("DB_USER", "postgres")
    db_password: str = os.environ.get("DB_PASSWORD", "postgres")
    logging_config_file: str = "logging.ini"
    search_params_file: str = "SEARCH_PARAMETERS.json"


def get_app_path() -> AppPaths:
    return AppPaths()
