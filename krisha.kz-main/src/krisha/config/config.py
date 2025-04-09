from dataclasses import dataclass

from src.krisha.config.logs import setup_logs
from src.krisha.config.parser import ParserConfig, get_parser_config
from src.krisha.config.path import AppPaths, get_app_path
from src.krisha.config.search import SearchParameters, get_search_parameters


@dataclass
class Config:
    """Main configuration."""

    path: AppPaths
    parser_config: ParserConfig
    search_params: SearchParameters


def load_config() -> Config:
    path = get_app_path()
    setup_logs(path)
    parser_config = get_parser_config()
    search_params = get_search_parameters(
        path.search_params_file, parser_config
    )
    return Config(
        path=path,
        parser_config=parser_config,
        search_params=search_params,
    )
