from __future__ import annotations
import json
import logging
from datetime import datetime
from pathlib import Path
import psycopg
from config.pg_config import PgConfig

# Module logger configuration (verbose to console)
logger = logging.getLogger(__name__)

class PgMetadataExporter:
    def __init__(self, folder_path: str, prefix_name: str):
        self.folder_path = Path(folder_path)
        self.prefix_name = prefix_name
        self.folder_path.mkdir(parents=True, exist_ok=True)
        logger.debug("Initialized PgMetadataExporter: folder=%s prefix=%s", self.folder_path, self.prefix_name)

    def _extract_db_metadata(self, cursor: psycopg.rows.dict_row) -> list[dict]:
        query = """
        SELECT 
            t.table_name,
            c.column_name,
            c.data_type,
            c.is_nullable
        FROM information_schema.tables t
        JOIN information_schema.columns c 
            ON t.table_name = c.table_name 
            AND t.table_schema = c.table_schema
        WHERE t.table_schema = 'public'
        ORDER BY t.table_name, c.ordinal_position
        """

        cursor.execute(query)
        rows = cursor.fetchall()
        logger.debug("Executed metadata query; fetched %d rows", len(rows))
        return rows
    
    def _format_db_metadata_to_json(self, rows: list[dict]) -> dict:
        metadata = {}
        for row in rows:
            table_name = row['table_name']
            if table_name not in metadata:
                metadata[table_name] = []
            metadata[table_name].append({
                'column_name': row['column_name'],
                'data_type': row['data_type'],
                'is_nullable': row['is_nullable']
            })
        logger.debug("Formatted metadata for %d tables from %d rows", len(metadata), len(rows))
        return metadata

    def _generate_filepath(self, config: PgConfig) -> str:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{self.prefix_name}-{config.database}-{timestamp}.json"
        path = str(self.folder_path / filename)
        logger.debug("Generated export filepath: %s", path)
        return path

    def export(self, config: PgConfig) -> str:
        """Export PostgreSQL metadata to a JSON file. Returns the path to the exported file."""
        conn = None
        cursor = None
        logger.info("Starting metadata export for database: %s", config.config_info())
        try:
            
            conn = psycopg.connect(
                host=config.host,
                port=config.port,
                user=config.user,
                password=config.password,
                dbname=config.database
            )
            logger.debug("Database connection established; %s", config.config_info())

            cursor = conn.cursor(row_factory=psycopg.rows.dict_row)
            logger.debug("Created cursor with dict_row")

            rows = self._extract_db_metadata(cursor)
            metadata = self._format_db_metadata_to_json(rows)

            filepath = self._generate_filepath(config)

            with open(filepath, 'w') as f:
                json.dump(metadata, f, indent=2)
            logger.info("Successfully wrote metadata to %s", filepath)

            return str(filepath)

        except psycopg.Error as e:
            logger.exception("Failed to export metadata: database error")
            raise Exception(f"Failed to export metadata: {e}")
        except Exception:
            logger.exception("Unexpected error during metadata export")
            raise
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                    logger.debug("Cursor closed")
                except Exception:
                    logger.exception("Error closing cursor")
            if conn is not None:
                try:
                    conn.close()
                    logger.debug("Database connection closed")
                except Exception:
                    logger.exception("Error closing connection")