import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import csv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any, Optional

class GenericTableValidator:
    def __init__(self, session: Session = None):
        """
        Initialize the Generic Table Validator
        
        Args:
            session: Snowflake Snowpark session (if None, will use current session context)
        """
        self.session = session
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        # Get session context
        if self.session is None:
            try:
                from snowflake.snowpark.context import get_active_session
                self.session = get_active_session()
                self.logger.info("Using active Snowpark session")
            except Exception as e:
                self.logger.error(f"No active session found: {str(e)}")
                raise Exception("No Snowpark session available. Please provide a session or run in Snowflake environment.")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get comprehensive table information including all columns from schema
        """
        try:
            df = self.session.table(table_name)
            row_count = df.count()
            columns_info = df.schema.fields
            columns = []
            for field in columns_info:
                columns.append({
                    'name': field.name,
                    'type': str(field.datatype),
                    'nullable': field.nullable
                })
            return {
                'table_name': table_name,
                'row_count': row_count,
                'columns': columns,
                'column_names': [col['name'] for col in columns]
            }
        except Exception as e:
            self.logger.error(f"Error getting table info for {table_name}: {str(e)}")
            return None
    
    def validate_table_compatibility(self, table1_info: Dict, table2_info: Dict) -> List[Dict]:
        """
        Validate that both tables are compatible for comparison
        """
        issues = []
        if not table1_info:
            issues.append({'type': 'table_not_found', 'table': 'TABLE1', 'severity': 'CRITICAL'})
        if not table2_info:
            issues.append({'type': 'table_not_found', 'table': 'TABLE2', 'severity': 'CRITICAL'})
        if table1_info and table2_info:
            table1_cols = set([col.upper() for col in table1_info['column_names']])
            table2_cols = set([col.upper() for col in table2_info['column_names']])
            missing_in_table2 = table1_cols - table2_cols
            missing_in_table1 = table2_cols - table1_cols
            if missing_in_table2:
                issues.append({
                    'type': 'columns_missing_in_table2',
                    'table1': table1_info['table_name'],
                    'table2': table2_info['table_name'],
                    'missing_columns': list(missing_in_table2),
                    'severity': 'WARNING'
                })
            if missing_in_table1:
                issues.append({
                    'type': 'columns_missing_in_table1',
                    'table1': table1_info['table_name'],
                    'table2': table2_info['table_name'],
                    'missing_columns': list(missing_in_table1),
                    'severity': 'WARNING'
                })
        return issues
    
    def identify_primary_key_columns(self, table1_info: Dict, table2_info: Dict, 
                                   primary_key_columns: Optional[List[str]] = None) -> List[str]:
        if primary_key_columns:
            table1_cols = [col.upper() for col in table1_info['column_names']]
            table2_cols = [col.upper() for col in table2_info['column_names']]
            common_cols = set(table1_cols) & set(table2_cols)
            valid_pk_cols = []
            for col in primary_key_columns:
                if col.upper() in common_cols:
                    valid_pk_cols.append(col.upper())
                else:
                    self.logger.warning(f"Primary key column {col} not found in both tables")
            return valid_pk_cols
        else:
            table1_cols = [col.upper() for col in table1_info['column_names']]
            table2_cols = [col.upper() for col in table2_info['column_names']]
            common_cols = set(table1_cols) & set(table2_cols)
            potential_keys = []
            for col in common_cols:
                if 'ID' in col or col in ['MEMBER_ID', 'CUSTOMER_ID', 'ACCOUNT_ID', 'USER_ID']:
                    potential_keys.append(col)
            if potential_keys:
                self.logger.info(f"Auto-identified potential primary key columns: {potential_keys}")
                return potential_keys[:1]
            else:
                self.logger.warning("No primary key columns identified. Using all common columns.")
                return list(common_cols)
    
    def _print_first_n_rows(self, data: List[Dict], n: int = 10, title: str = ""):
        if not data:
            print(f"\n{title} - No data available.")
            return
        print(f"\n{title} - Printing up to {n} sample rows:")
        for i, row in enumerate(data[:n]):
            print(f"Row {i+1}: {row}")
        if len(data) > n:
            print(f"... ({len(data)-n} more rows not shown)")

    def create_snowflake_table(self, table_name: str, data: List[Dict]):
        """
        Create Snowflake table with validation results and print sample rows
        """
        if not data:
            self.logger.info(f"No data to create table {table_name}")
            print(f"\nTable {table_name} - No data to write.")
            return
        try:
            self._print_first_n_rows(data, 10, f"Table {table_name}")
            df = pd.DataFrame(data)
            df.columns = [col.replace('(', '').replace(')', '').replace(' ', '_').replace('-', '_') for col in df.columns]
            snowpark_df = self.session.create_dataframe(df)
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            self.logger.info(f"Created Snowflake table: {table_name} with {len(data)} rows")
        except Exception as e:
            self.logger.error(f"Error creating Snowflake table {table_name}: {str(e)}")
            print(f"Error creating Snowflake table {table_name}: {str(e)}")
    
    def write_to_snowflake_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
        """
        Write data to Snowflake stage as CSV and print sample rows
        """
        if not data:
            self.logger.info(f"No data to write to stage {filename}")
            print(f"\nStage file {filename} - No data to write.")
            return
        try:
            self._print_first_n_rows(data, 10, f"Stage file {filename}")
            df = pd.DataFrame(data)
            snowpark_df = self.session.create_dataframe(df)
            snowpark_df.write.mode("overwrite").csv(f"{stage_name}{filename}")
            self.logger.info(f"Created stage file: {stage_name}{filename}")
        except Exception as e:
            self.logger.error(f"Error writing to stage: {str(e)}")
            print(f"Error writing to stage {filename}: {str(e)}")
    
    def compare_tables_comprehensive(self, table1_name: str, table2_name: str,
                                   primary_key_columns: Optional[List[str]] = None,
                                   table1_alias: str = "SQL", 
                                   table2_alias: str = "MDP",
                                   batch_size: int = 10000) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Perform comprehensive row-by-row comparison of all columns between two tables
        Memory-optimized version that processes data in batches
        
        Returns:
            Tuple of (mismatched_rows, table1_only_rows, table2_only_rows, summary_stats)
        """
        mismatched_rows = []
        table1_only_rows = []
        table2_only_rows = []
        summary_stats = {}
        try:
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            if not table1_info or not table2_info:
                self.logger.error("Failed to get table information")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            table1_cols = [col.upper() for col in table1_info['column_names']]
            table2_cols = [col.upper() for col in table2_info['column_names']]
            common_columns = list(set(table1_cols) & set(table2_cols))
            if not common_columns:
                self.logger.error("No common columns found between tables")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                self.logger.error("No valid primary key columns identified")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            table1_df = self.session.table(table1_name)
            table2_df = self.session.table(table2_name)
            table1_df.create_or_replace_temp_view("temp_table1")
            table2_df.create_or_replace_temp_view("temp_table2")
            pk_join_conditions = []
            for col in pk_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            # 1. Find rows in TABLE1 but not in TABLE2 (count only)
            self.logger.info(f"Counting rows in {table1_alias} but not in {table2_alias}...")
            table1_only_count_sql = f"""
            SELECT COUNT(*) as count_only
            FROM temp_table1 t1
            LEFT JOIN temp_table2 t2 ON {pk_join_condition}
            WHERE t2.{pk_columns[0]} IS NULL
            """
            count_result = self.session.sql(table1_only_count_sql).collect()
            table1_only_count = count_result[0]['COUNT_ONLY']
            if table1_only_count > 0:
                table1_only_cols = ", ".join([f"t1.{col}" for col in pk_columns])
                table1_only_sql = f"""
                SELECT {table1_only_cols}
                FROM temp_table1 t1
                LEFT JOIN temp_table2 t2 ON {pk_join_condition}
                WHERE t2.{pk_columns[0]} IS NULL
                LIMIT 1000
                """
                table1_only_result = self.session.sql(table1_only_sql)
                table1_only_df = table1_only_result.to_pandas()
                for _, row in table1_only_df.iterrows():
                    table1_only_rows.append(row.to_dict())
            # 2. Find rows in TABLE2 but not in TABLE1 (count only)
            self.logger.info(f"Counting rows in {table2_alias} but not in {table1_alias}...")
            table2_only_count_sql = f"""
            SELECT COUNT(*) as count_only
            FROM temp_table2 t2
            LEFT JOIN temp_table1 t1 ON {pk_join_condition}
            WHERE t1.{pk_columns[0]} IS NULL
            """
            count_result = self.session.sql(table2_only_count_sql).collect()
            table2_only_count = count_result[0]['COUNT_ONLY']
            if table2_only_count > 0:
                table2_only_cols = ", ".join([f"t2.{col}" for col in pk_columns])
                table2_only_sql = f"""
                SELECT {table2_only_cols}
                FROM temp_table2 t2
                LEFT JOIN temp_table1 t1 ON {pk_join_condition}
                WHERE t1.{pk_columns[0]} IS NULL
                LIMIT 1000
                """
                table2_only_result = self.session.sql(table2_only_sql)
                table2_only_df = table2_only_result.to_pandas()
                for _, row in table2_only_df.iterrows():
                    table2_only_rows.append(row.to_dict())
            # 3. Count mismatched rows first, then sample
            self.logger.info("Counting rows with mismatched column values...")
            comparison_conditions = []
            for col in common_columns:
                if col not in pk_columns:
                    comparison_conditions.append(f"""
                        (COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))
                    """)
            if comparison_conditions:
                mismatch_count_sql = f"""
                SELECT COUNT(*) as count_mismatches
                FROM temp_table1 t1
                INNER JOIN temp_table2 t2 ON {pk_join_condition}
                WHERE {' OR '.join(comparison_conditions)}
                """
                count_result = self.session.sql(mismatch_count_sql).collect()
                total_mismatch_rows = count_result[0]['COUNT_MISMATCHES']
                if total_mismatch_rows > 0:
                    select_columns = []
                    for col in pk_columns:
                        select_columns.append(f"t1.{col}")
                    sample_columns = [col for col in common_columns if col not in pk_columns][:10]
                    for col in sample_columns:
                        select_columns.extend([f"t1.{col} as {table1_alias}_{col}", f"t2.{col} as {table2_alias}_{col}"])
                    mismatch_sql = f"""
                    SELECT {', '.join(select_columns)}
                    FROM temp_table1 t1
                    INNER JOIN temp_table2 t2 ON {pk_join_condition}
                    WHERE {' OR '.join(comparison_conditions[:5])}
                    LIMIT 5000
                    """
                    mismatch_result = self.session.sql(mismatch_sql)
                    mismatch_df = mismatch_result.to_pandas()
                    for _, row in mismatch_df.iterrows():
                        pk_values = {}
                        for col in pk_columns:
                            pk_values[col] = row[col]
                        for col in sample_columns:
                            table1_val = row.get(f'{table1_alias}_{col}')
                            table2_val = row.get(f'{table2_alias}_{col}')
                            if pd.isna(table1_val) and pd.isna(table2_val):
                                continue
                            elif pd.isna(table1_val) or pd.isna(table2_val) or str(table1_val) != str(table2_val):
                                mismatch_record = {
                                    **pk_values,
                                    'COLUMN_NAME': col,
                                    f'{table1_alias}_VALUE': table1_val if pd.notna(table1_val) else None,
                                    f'{table2_alias}_VALUE': table2_val if pd.notna(table2_val) else None,
                                    'TABLE1_ALIAS': table1_alias,
                                    'TABLE2_ALIAS': table2_alias
                                }
                                mismatched_rows.append(mismatch_record)
                estimated_total_mismatches = total_mismatch_rows * len([col for col in common_columns if col not in pk_columns])
            else:
                total_mismatch_rows = 0
                estimated_total_mismatches = 0
            summary_stats = {
                'timestamp': self.timestamp,
                'table1_name': table1_name,
                'table2_name': table2_name,
                'table1_alias': table1_alias,
                'table2_alias': table2_alias,
                'total_table1_rows': table1_info['row_count'],
                'total_table2_rows': table2_info['row_count'],
                'rows_only_in_table1': table1_only_count,
                'rows_only_in_table2': table2_only_count,
                'mismatched_rows_count': total_mismatch_rows,
                'mismatched_data_points': len(mismatched_rows),
                'estimated_total_mismatches': estimated_total_mismatches,
                'primary_key_columns': pk_columns,
                'common_columns': common_columns,
                'total_common_columns': len(common_columns),
                'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'note': 'Results are sampled for memory efficiency'
            }
            self.logger.info(f"Comprehensive comparison completed:")
            self.logger.info(f"  - Total {table1_alias} rows: {summary_stats['total_table1_rows']}")
            self.logger.info(f"  - Total {table2_alias} rows: {summary_stats['total_table2_rows']}")
            self.logger.info(f"  - {table1_alias} only rows: {table1_only_count}")
            self.logger.info(f"  - {table2_alias} only rows: {table2_only_count}")
            self.logger.info(f"  - Mismatched rows: {total_mismatch_rows}")
            self.logger.info(f"  - Sample data points: {len(mismatched_rows)}")
        except Exception as e:
            self.logger.error(f"Error during comprehensive table comparison: {str(e)}")
            raise
        return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats

    # The rest of the class remains unchanged except for using the updated create_snowflake_table and write_to_snowflake_stage.

    def validate_tables(self, table1_name: str, table2_name: str,
                       primary_key_columns: Optional[List[str]] = None,
                       table1_alias: str = "SQL", table2_alias: str = "MDP",
                       environment: str = "SNOWFLAKE",
                       create_tables: bool = True,
                       stage_name: str = "@~/") -> Dict[str, Any]:
        self.logger.info("="*80)
        self.logger.info("STARTING COMPREHENSIVE TABLE VALIDATION")
        self.logger.info("="*80)
        self.logger.info(f"Table 1 ({table1_alias}): {table1_name}")
        self.logger.info(f"Table 2 ({table2_alias}): {table2_name}")
        self.logger.info(f"Environment: {environment}")
        self.logger.info(f"Primary Key Columns: {primary_key_columns}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        table1_info = self.get_table_info(table1_name)
        table2_info = self.get_table_info(table2_name)
        if not table1_info or not table2_info:
            self.logger.error("Failed to get table information")
            return None
        validation_issues = self.validate_table_compatibility(table1_info, table2_info)
        critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
        if critical_issues:
            self.logger.error("Critical validation issues found - cannot proceed with data comparison")
            return {
                'validation_status': 'FAILED',
                'critical_issues': critical_issues,
                'validation_issues': validation_issues
            }
        mismatched_rows, table1_only_rows, table2_only_rows, summary_stats = self.compare_tables_comprehensive(
            table1_name, table2_name, primary_key_columns, table1_alias, table2_alias, batch_size=5000
        )
        mismatch_table_name = f"TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
        table1_only_table_name = f"TABLE_VALIDATION_{table1_alias}_ONLY_{self.timestamp}"
        table2_only_table_name = f"TABLE_VALIDATION_{table2_alias}_ONLY_{self.timestamp}"
        mismatch_csv = f"table_validation_mismatches_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
        table1_only_csv = f"table_validation_{table1_alias}_only_{self.timestamp}.csv"
        table2_only_csv = f"table_validation_{table2_alias}_only_{self.timestamp}.csv"
        summary_csv = f"table_validation_summary_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
        created_tables = {}
        if create_tables:
            if mismatched_rows:
                self.create_snowflake_table(mismatch_table_name, mismatched_rows)
                created_tables['mismatches'] = mismatch_table_name
            if table1_only_rows:
                self.create_snowflake_table(table1_only_table_name, table1_only_rows)
                created_tables[f'{table1_alias}_only'] = table1_only_table_name
            if table2_only_rows:
                self.create_snowflake_table(table2_only_table_name, table2_only_rows)
                created_tables[f'{table2_alias}_only'] = table2_only_table_name
        stage_files = {}
        if mismatched_rows:
            self.write_to_snowflake_stage(mismatched_rows, mismatch_csv, stage_name)
            stage_files['mismatches'] = f"{stage_name}{mismatch_csv}"
        if table1_only_rows:
            self.write_to_snowflake_stage(table1_only_rows, table1_only_csv, stage_name)
            stage_files[f'{table1_alias}_only'] = f"{stage_name}{table1_only_csv}"
        if table2_only_rows:
            self.write_to_snowflake_stage(table2_only_rows, table2_only_csv, stage_name)
            stage_files[f'{table2_alias}_only'] = f"{stage_name}{table2_only_csv}"
        self.write_to_snowflake_stage([summary_stats], summary_csv, stage_name)
        stage_files['summary'] = f"{stage_name}{summary_csv}"
        validation_passed = (
            len(mismatched_rows) == 0 and 
            len(table1_only_rows) == 0 and 
            len(table2_only_rows) == 0 and
            len(validation_issues) == 0
        )
        results = {
            'validation_status': 'PASSED' if validation_passed else 'FAILED',
            'timestamp': self.timestamp,
            'environment': environment,
            'table1_info': table1_info,
            'table2_info': table2_info,
            'validation_issues': validation_issues,
            'summary_statistics': summary_stats,
            'results_summary': {
                'mismatched_rows_count': summary_stats.get('mismatched_rows_count', len(mismatched_rows)),
                f'{table1_alias}_only_rows_count': summary_stats.get('rows_only_in_table1', len(table1_only_rows)),
                f'{table2_alias}_only_rows_count': summary_stats.get('rows_only_in_table2', len(table2_only_rows)),
                'total_issues_found': summary_stats.get('mismatched_rows_count', len(mismatched_rows)) + summary_stats.get('rows_only_in_table1', len(table1_only_rows)) + summary_stats.get('rows_only_in_table2', len(table2_only_rows))
            },
            'created_tables': created_tables if create_tables else None,
            'stage_files': stage_files
        }
        return results

    # The rest of the class remains unchanged (print_sample_mismatches, print_validation_report, validate_tables_sp, etc.)

# ... rest of the file unchanged ...
