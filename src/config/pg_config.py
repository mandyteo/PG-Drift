from __future__ import annotations
import logging
from typing import List
from ..utils import mask_string

import os

logger = logging.getLogger(__name__)

class PgConfig:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database

    @staticmethod
    def load_from_env() -> List[PgConfig]:
        configs = []

        db_count = int(os.getenv("DB_COUNT", "1"))
        if db_count == 1:
            logger.warning("DB_COUNT is set to 1. Ensure this is intentional.")
        if db_count < 1:
            raise ValueError("DB_COUNT must be at least 1")

        logger.debug("Loading %d database configurations from environment variables", db_count)
        for i in range(1, db_count + 1):
            host = os.getenv(f"PG_DRIFT_DB_HOST_{i}", "localhost")
            port = int(os.getenv(f"PG_DRIFT_DB_PORT_{i}", "5432"))
            user = os.getenv(f"PG_DRIFT_DB_USER_{i}", "postgres")
            password = os.getenv(f"PG_DRIFT_DB_PASSWORD_{i}", "password")
            database = os.getenv(f"PG_DRIFT_DB_NAME_{i}", "postgres")

            config = PgConfig(host, port, user, password, database)
            logger.debug("Loaded PgConfig: %s", config.config_info())
            configs.append(config)
        return configs
    
    def config_info(self) -> str:
        return f"Host: {mask_string(self.host)}, Port: {self.port}, User: {self.user}, Database: {self.database}"