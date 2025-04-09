import logging
import sys

from src.krisha.config.config import load_config
from src.krisha.crawler.first_page import FirstPage
from src.krisha.crawler.spider import run_crawler
from src.krisha.db.service import get_connection

logger = logging.getLogger()


def main():
    config = load_config()
    with get_connection(config.path) as db_conn:
        first_page_url = FirstPage.get_url(config)
        run_crawler(config, db_conn, first_page_url)

if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        logger.critical(msg=error, exc_info=True)
        sys.exit()
