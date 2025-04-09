# Modify the AppPaths dataclass in path.py
from dataclasses import dataclass


@dataclass
class AppPaths:
    app_dir: str = "krisha"
    logs_dir: str = "logs"
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "krisha"
    db_user: str = "postgres"
    db_password: str = "postgres"
    logging_config_file: str = "logging.ini"
    search_params_file: str = "SEARCH_PARAMETERS.json"


def get_app_path() -> AppPaths:
    return AppPaths()
