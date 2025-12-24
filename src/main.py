import datetime
import os
import logging
import hashlib
from pathlib import Path
from config.pg_config import PgConfig
from pg_metadata_exporter import PgMetadataExporter
from pg_metadata_result import PgMetadataResult
from pg_metadata_diff_results import PgMetadataDiffResults

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

    target_folder = os.getenv("PG_DRIFT_TARGET_FOLDER", "pgdrift-output")
    configs = PgConfig.load_from_env()

    logger.info("Starting exporting of metadata for %d databases", len(configs))
    success, failures = 0, 0
    results = []
    for idx, config in enumerate(configs, start=1):
        exporter = PgMetadataExporter(folder_path=target_folder, prefix_name=f"db_{idx}", init_timestamp=timestamp)
        try:
            filepath = exporter.export(config)
            
            with open(filepath, 'rb') as f:
                file_hash = hashlib.sha256(f.read()).hexdigest()

            result = PgMetadataResult(metadata_filepath=filepath, checksum=file_hash, pg_config=config)
            results.append(result)
            
            success += 1
            logger.debug("Exported metadata for database.\n%s\nOutput Path: %s", config.config_info(), filepath)
            logger.info(f"db_{idx} exported to {filepath} with checksum {file_hash}")
            logger.info("Checksum for db_%d: %s", idx, file_hash)
        except Exception as e:
            failures += 1
            logger.error("Failed to export metadata for database.\n%s\n%s", config.config_info(), e)
    
    logger.info("Completed metadata export: %d successes, %d failures", success, failures)
    
    PgMetadataResult.output_tabulation_table(results, target_folder, timestamp)
    
    if len(results) > 1:
        checksums = [result.checksum for result in results]
        if len(set(checksums)) > 1:
            logger.info("Detected schema differences, generating detailed diff report")
            
            metadata_files = []
            for idx, result in enumerate(results, start=1):
                label = PgMetadataResult._db_label(result, idx)
                metadata_files.append((label, result.metadata_filepath, result.pg_config))
            
            diff_analyzer = PgMetadataDiffResults(metadata_files)
            diff_analyzer.generate_diff_report(target_folder, timestamp)
        else:
            logger.info("All databases have identical checksums - skipping diff report")
            print("\n✓ All databases are identical - no schema differences detected")


if __name__ == "__main__":
    main()