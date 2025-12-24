from __future__ import annotations
import csv
import json
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple
from config.pg_config import PgConfig

logger = logging.getLogger(__name__)


class PgMetadataDiffResults:
    """Handles comparison of database metadata and generation of diff reports."""

    def __init__(self, metadata_files: List[Tuple[str, str, PgConfig]]):
        """
        Args:
            metadata_files: List of tuples containing (database_label, json_filepath, pg_config)
        """
        self.metadata_files = metadata_files
        self.metadata_cache: Dict[str, Dict] = {}
        
    def _load_metadata(self, filepath: str) -> Dict:
        if filepath not in self.metadata_cache:
            with open(filepath, 'r') as f:
                self.metadata_cache[filepath] = json.load(f)
        return self.metadata_cache[filepath]
    
    def _get_all_tables(self, metadata: Dict) -> Set[str]:
        return set(metadata.keys())
    
    def _get_column_signature(self, column: Dict) -> str:
        return f"{column['column_name']}|{column['data_type']}|{column['is_nullable']}"
    
    def _compare_tables(self, label1: str, metadata1: Dict, label2: str, metadata2: Dict) -> List[Dict]:
        differences = []
        
        tables1 = self._get_all_tables(metadata1)
        tables2 = self._get_all_tables(metadata2)
        
        # Tables only in database 1
        only_in_db1 = tables1 - tables2
        for table in sorted(only_in_db1):
            differences.append({
                'diff_type': 'MISSING_TABLE',
                'table_name': table,
                'column_name': '',
                'detail': f"Table exists in {label1} but not in {label2}",
                'db1': label1,
                'db2': label2
            })
        
        # Tables only in database 2
        only_in_db2 = tables2 - tables1
        for table in sorted(only_in_db2):
            differences.append({
                'diff_type': 'EXTRA_TABLE',
                'table_name': table,
                'column_name': '',
                'detail': f"Table exists in {label2} but not in {label1}",
                'db1': label1,
                'db2': label2
            })
        
        # For common tables...
        common_tables = tables1 & tables2
        for table in sorted(common_tables):
            col_diffs = self._compare_columns(table, metadata1[table], metadata2[table], label1, label2)
            differences.extend(col_diffs)
        
        return differences
    
    def _compare_columns(self, table_name: str, columns1: List[Dict], columns2: List[Dict], 
                        label1: str, label2: str) -> List[Dict]:
        """Compare columns, return differences."""
        differences = []
        
        # Create dictionaries for easier lookup
        cols1_dict = {col['column_name']: col for col in columns1}
        cols2_dict = {col['column_name']: col for col in columns2}
        
        cols1_names = set(cols1_dict.keys())
        cols2_names = set(cols2_dict.keys())
        
        # Columns only in database 1
        only_in_db1 = cols1_names - cols2_names
        for col_name in sorted(only_in_db1):
            col = cols1_dict[col_name]
            differences.append({
                'diff_type': 'MISSING_COLUMN',
                'table_name': table_name,
                'column_name': col_name,
                'detail': f"Column exists in {label1} but not in {label2} (type: {col['data_type']}, nullable: {col['is_nullable']})",
                'db1': label1,
                'db2': label2
            })
        
        # Columns only in database 2
        only_in_db2 = cols2_names - cols1_names
        for col_name in sorted(only_in_db2):
            col = cols2_dict[col_name]
            differences.append({
                'diff_type': 'EXTRA_COLUMN',
                'table_name': table_name,
                'column_name': col_name,
                'detail': f"Column exists in {label2} but not in {label1} (type: {col['data_type']}, nullable: {col['is_nullable']})",
                'db1': label1,
                'db2': label2
            })
        
        # For type/nullable differences
        common_columns = cols1_names & cols2_names
        for col_name in sorted(common_columns):
            col1 = cols1_dict[col_name]
            col2 = cols2_dict[col_name]
            
            type_diff = col1['data_type'] != col2['data_type']
            nullable_diff = col1['is_nullable'] != col2['is_nullable']
            
            if type_diff or nullable_diff:
                detail_parts = []
                if type_diff:
                    detail_parts.append(f"data_type: {label1}={col1['data_type']} vs {label2}={col2['data_type']}")
                if nullable_diff:
                    detail_parts.append(f"nullable: {label1}={col1['is_nullable']} vs {label2}={col2['is_nullable']}")
                
                differences.append({
                    'diff_type': 'COLUMN_MISMATCH',
                    'table_name': table_name,
                    'column_name': col_name,
                    'detail': '; '.join(detail_parts),
                    'db1': label1,
                    'db2': label2
                })
        
        return differences
    
    def generate_diff_report(self, output_folder: str, timestamp: str) -> None:
        output_dir = Path(output_folder)
        output_dir.mkdir(parents=True, exist_ok=True)
        
        metadata_map = {}
        for label, filepath, _ in self.metadata_files:
            metadata_map[label] = self._load_metadata(filepath)
        
        logger.info("Generating diff report for %d databases", len(self.metadata_files))
        
        all_differences = []
        labels = list(metadata_map.keys())
        
        # Compare databases pair-by-pair
        for i in range(len(labels)):
            for j in range(i + 1, len(labels)):
                label1, label2 = labels[i], labels[j]
                logger.info("Comparing %s vs %s", label1, label2)
                
                diffs = self._compare_tables(label1, metadata_map[label1], label2, metadata_map[label2])
                all_differences.extend(diffs)
        
        if not all_differences:
            logger.info("No differences found between databases")
            print("\nAll databases are identical - no schema differences detected")
            return
        
        csv_path = output_dir / f"{timestamp}-schema_differences.csv"
        headers = ['Diff Type', 'Table Name', 'Column Name', 'Database 1', 'Database 2', 'Detail']
        
        with csv_path.open('w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            for diff in all_differences:
                writer.writerow([
                    diff['diff_type'],
                    diff['table_name'],
                    diff['column_name'],
                    diff['db1'],
                    diff['db2'],
                    diff['detail']
                ])
        
        logger.info("Schema differences report saved to %s", csv_path)
        self._print_diff_summary(all_differences, csv_path)
    
    def _print_diff_summary(self, differences: List[Dict], csv_path: Path) -> None:
        """Print a summary of differences to console."""
        print(f"\nSchema Differences Detected ({len(differences)} total)")
        print("=" * 80)
        
        by_type = {}
        for diff in differences:
            diff_type = diff['diff_type']
            if diff_type not in by_type:
                by_type[diff_type] = []
            by_type[diff_type].append(diff)
        
        for diff_type, diffs in sorted(by_type.items()):
            print(f"\n{diff_type}: {len(diffs)} occurrence(s)")

            # Show only first 5 differences as sample
            for diff in diffs[:5]:
                if diff['column_name']:
                    print(f"  • {diff['table_name']}.{diff['column_name']}: {diff['detail']}")
                else:
                    print(f"  • {diff['table_name']}: {diff['detail']}")
            if len(diffs) > 5:
                print(f"  ... and {len(diffs) - 5} more")
        
        print(f"\nFull Differences report saved to: {csv_path}")
        print("=" * 80)
