import datetime
import os
import logging
from pathlib import Path
from config.pg_config import PgConfig
from pg_metadata_exporter import PgMetadataExporter

logger = logging.getLogger(__name__)

def main():
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = Path("pgdrift-logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        filename=f"pgdrift-logs/pgdrift-{timestamp}.log",
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(name)s - %(message)s'
    )

    target_folder = os.getenv("PG_DRIFT_TARGET_FOLDER", "metadata_exports")
    configs = PgConfig.load_from_env()

    logger.info("Starting exporting of metadata for %d databases", len(configs))
    success, failures = 0, 0
    for idx, config in enumerate(configs, start=1):
        exporter = PgMetadataExporter(folder_path=target_folder, prefix_name=f"db_{idx}")
        try:
            filepath = exporter.export(config)
            success += 1
            logger.debug("Exported metadata for database.\n%s\nOutput Path: %s", config.config_info(), filepath)
        except Exception as e:
            failures += 1
            logger.error("Failed to export metadata for database.\n%s\n%s", config.config_info(), e)
    
    logger.info("Completed metadata export: %d successes, %d failures", success, failures)

if __name__ == "__main__":
    main()