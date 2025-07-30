# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
import pandas as pd
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
import csv
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any, Optional

class StreamingTableValidator:
    def __init__(self, session: Session = None):
        """
        Initialize the Streaming Table Validator with memory-efficient processing
        
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
    
    def identify_comparison_columns(self, table1_info: Dict, table2_info: Dict, 
                                  primary_key_columns: List[str],
                                  comparison_columns: Optional[List[str]] = None,
                                  max_auto_columns: int = 50) -> List[str]:
        """
        Identify columns to use for comparison
        """
        table1_cols = [col.upper() for col in table1_info['column_names']]
        table2_cols = [col.upper() for col in table2_info['column_names']]
        common_cols = set(table1_cols) & set(table2_cols)
        
        # Remove primary key columns from comparison
        pk_cols_upper = [col.upper() for col in primary_key_columns]
        available_comparison_cols = [col for col in common_cols if col not in pk_cols_upper]
        
        if comparison_columns:
            # User specified columns - validate they exist
            valid_comparison_cols = []
            for col in comparison_columns:
                col_upper = col.upper()
                if col_upper in available_comparison_cols:
                    valid_comparison_cols.append(col_upper)
                else:
                    self.logger.warning(f"Comparison column {col} not found in both tables or is a primary key")
            
            if valid_comparison_cols:
                self.logger.info(f"Using user-specified comparison columns ({len(valid_comparison_cols)} columns): {valid_comparison_cols}")
                return valid_comparison_cols
            else:
                self.logger.warning("No valid user-specified comparison columns found. Using auto-detection.")
        
        # Auto-select columns based on user-defined limit
        if len(available_comparison_cols) > max_auto_columns:
            selected_cols = list(available_comparison_cols)[:max_auto_columns]
            self.logger.info(f"Found {len(available_comparison_cols)} comparison columns. Auto-selected first {max_auto_columns} columns: {selected_cols}")
        else:
            selected_cols = list(available_comparison_cols)
            self.logger.info(f"Using all available comparison columns ({len(selected_cols)} columns): {selected_cols}")
        
        return selected_cols

    def create_result_tables_directly(self, table1_name: str, table2_name: str,
                                    primary_key_columns: List[str],
                                    comparison_columns: List[str],
                                    table1_alias: str = "SQL", 
                                    table2_alias: str = "MDP") -> Dict[str, Any]:
        """
        Create result tables directly in Snowflake using SQL without loading data into memory
        This is the most memory-efficient approach for getting ALL records
        """
        results = {}
        
        try:
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # Generate table names
            mismatch_table = f"TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
            table1_only_table = f"TABLE_VALIDATION_{table1_alias}_ONLY_{self.timestamp}"
            table2_only_table = f"TABLE_VALIDATION_{table2_alias}_ONLY_{self.timestamp}"
            
            # 1. Create table for rows only in TABLE1
            self.logger.info(f"Creating table for {table1_alias}-only rows...")
            table1_only_sql = f"""
            CREATE OR REPLACE TABLE {table1_only_table} AS
            SELECT t1.*
            FROM {table1_name} t1
            LEFT JOIN {table2_name} t2 ON {pk_join_condition}
            WHERE t2.{primary_key_columns[0]} IS NULL
            """
            self.session.sql(table1_only_sql).collect()
            
            # Get count
            count_result = self.session.sql(f"SELECT COUNT(*) as CNT FROM {table1_only_table}").collect()
            table1_only_count = count_result[0]['CNT']
            results['table1_only_count'] = table1_only_count
            results['table1_only_table'] = table1_only_table
            self.logger.info(f"Created {table1_only_table} with {table1_only_count:,} rows")
            
            # 2. Create table for rows only in TABLE2
            self.logger.info(f"Creating table for {table2_alias}-only rows...")
            table2_only_sql = f"""
            CREATE OR REPLACE TABLE {table2_only_table} AS
            SELECT t2.*
            FROM {table2_name} t2
            LEFT JOIN {table1_name} t1 ON {pk_join_condition}
            WHERE t1.{primary_key_columns[0]} IS NULL
            """
            self.session.sql(table2_only_sql).collect()
            
            # Get count
            count_result = self.session.sql(f"SELECT COUNT(*) as CNT FROM {table2_only_table}").collect()
            table2_only_count = count_result[0]['CNT']
            results['table2_only_count'] = table2_only_count
            results['table2_only_table'] = table2_only_table
            self.logger.info(f"Created {table2_only_table} with {table2_only_count:,} rows")
            
            # 3. Create mismatches table using UNPIVOT approach for ALL records
            self.logger.info("Creating comprehensive mismatches table...")
            
            # Build comparison conditions for filtering
            comparison_conditions = []
            for col in comparison_columns:
                comparison_conditions.append(f"""
                    (COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))
                """)
            
            # Create the mismatches table with unpivoted structure
            # This gives you one row per column mismatch, making it easier to analyze
            pk_select = ", ".join([f"t1.{col}" for col in primary_key_columns])
            
            mismatch_cases = []
            for col in comparison_columns:
                mismatch_cases.append(f"""
                SELECT {pk_select}, 
                       '{col}' as COLUMN_NAME,
                       COALESCE(CAST(t1.{col} AS STRING), 'NULL') as {table1_alias}_VALUE,
                       COALESCE(CAST(t2.{col} AS STRING), 'NULL') as {table2_alias}_VALUE,
                       '{table1_alias}' as TABLE1_ALIAS,
                       '{table2_alias}' as TABLE2_ALIAS
                FROM {table1_name} t1
                INNER JOIN {table2_name} t2 ON {pk_join_condition}
                WHERE COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL')
                """)
            
            mismatch_sql = f"""
            CREATE OR REPLACE TABLE {mismatch_table} AS
            {' UNION ALL '.join(mismatch_cases)}
            """
            
            self.session.sql(mismatch_sql).collect()
            
            # Get count
            count_result = self.session.sql(f"SELECT COUNT(*) as CNT FROM {mismatch_table}").collect()
            mismatch_count = count_result[0]['CNT']
            results['mismatch_count'] = mismatch_count
            results['mismatch_table'] = mismatch_table
            self.logger.info(f"Created {mismatch_table} with {mismatch_count:,} mismatch records")
            
            # 4. Create summary statistics table
            summary_table = f"TABLE_VALIDATION_SUMMARY_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
            
            # Get total row counts
            table1_count = self.session.sql(f"SELECT COUNT(*) as CNT FROM {table1_name}").collect()[0]['CNT']
            table2_count = self.session.sql(f"SELECT COUNT(*) as CNT FROM {table2_name}").collect()[0]['CNT']
            
            # Count unique mismatched rows (not individual column mismatches)
            unique_mismatch_rows_sql = f"""
            SELECT COUNT(DISTINCT CONCAT({', '.join([f"COALESCE(CAST({col} AS STRING), 'NULL')" for col in primary_key_columns])})) as CNT
            FROM {mismatch_table}
            """
            unique_mismatch_rows = self.session.sql(unique_mismatch_rows_sql).collect()[0]['CNT']
            
            summary_sql = f"""
            CREATE OR REPLACE TABLE {summary_table} AS
            SELECT 
                '{self.timestamp}' as TIMESTAMP,
                '{table1_name}' as TABLE1_NAME,
                '{table2_name}' as TABLE2_NAME,
                '{table1_alias}' as TABLE1_ALIAS,
                '{table2_alias}' as TABLE2_ALIAS,
                {table1_count} as TOTAL_TABLE1_ROWS,
                {table2_count} as TOTAL_TABLE2_ROWS,
                {table1_only_count} as ROWS_ONLY_IN_TABLE1,
                {table2_only_count} as ROWS_ONLY_IN_TABLE2,
                {unique_mismatch_rows} as MISMATCHED_ROWS_COUNT,
                {mismatch_count} as TOTAL_MISMATCH_DATA_POINTS,
                '{', '.join(primary_key_columns)}' as PRIMARY_KEY_COLUMNS,
                '{', '.join(comparison_columns)}' as COMPARISON_COLUMNS,
                {len(comparison_columns)} as TOTAL_COMPARISON_COLUMNS,
                'COMPLETE' as SAMPLE_TYPE,
                CURRENT_TIMESTAMP() as VALIDATION_DATE,
                'All records processed - no memory limits' as NOTE
            """
            
            self.session.sql(summary_sql).collect()
            results['summary_table'] = summary_table
            
            results.update({
                'table1_count': table1_count,
                'table2_count': table2_count,
                'unique_mismatch_rows': unique_mismatch_rows,
                'total_mismatch_data_points': mismatch_count,
                'primary_key_columns': primary_key_columns,
                'comparison_columns': comparison_columns,
                'validation_status': 'PASSED' if (table1_only_count == 0 and table2_only_count == 0 and unique_mismatch_rows == 0) else 'FAILED'
            })
            
            self.logger.info("✅ ALL TABLES CREATED SUCCESSFULLY - NO MEMORY LIMITS!")
            self.logger.info(f"📊 Summary: {table1_count:,} vs {table2_count:,} rows, {unique_mismatch_rows:,} mismatched rows, {mismatch_count:,} total mismatches")
            
        except Exception as e:
            self.logger.error(f"Error creating result tables: {str(e)}")
            raise
        
        return results

    def create_stage_files_streaming(self, table1_name: str, table2_name: str,
                                   primary_key_columns: List[str],
                                   comparison_columns: List[str],
                                   table1_alias: str = "SQL", 
                                   table2_alias: str = "MDP",
                                   stage_name: str = "@~/") -> Dict[str, str]:
        """
        Create CSV files in Snowflake stage directly from SQL without loading into memory
        """
        stage_files = {}
        
        try:
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # Create file format for consistent CSV output
            format_name = f"CSV_FORMAT_{self.timestamp}"
            self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
            self.session.sql(f"""
                CREATE FILE FORMAT {format_name}
                TYPE = 'CSV'
                FIELD_DELIMITER = ','
                RECORD_DELIMITER = '\\n'
                SKIP_HEADER = 0
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                NULL_IF = ('NULL', 'null')
                EMPTY_FIELD_AS_NULL = FALSE
                COMPRESSION = 'NONE'
            """).collect()
            
            # 1. Export TABLE1-only rows
            table1_only_file = f"table_validation_{table1_alias}_only_{self.timestamp}.csv"
            self.logger.info(f"Exporting {table1_alias}-only rows to {stage_name}{table1_only_file}...")
            
            table1_only_export_sql = f"""
            COPY INTO {stage_name}{table1_only_file}
            FROM (
                SELECT t1.*
                FROM {table1_name} t1
                LEFT JOIN {table2_name} t2 ON {pk_join_condition}
                WHERE t2.{primary_key_columns[0]} IS NULL
            )
            FILE_FORMAT = (FORMAT_NAME = '{format_name}')
            OVERWRITE = TRUE
            HEADER = TRUE
            """
            self.session.sql(table1_only_export_sql).collect()
            stage_files[f'{table1_alias}_only'] = f"{stage_name}{table1_only_file}"
            
            # 2. Export TABLE2-only rows
            table2_only_file = f"table_validation_{table2_alias}_only_{self.timestamp}.csv"
            self.logger.info(f"Exporting {table2_alias}-only rows to {stage_name}{table2_only_file}...")
            
            table2_only_export_sql = f"""
            COPY INTO {stage_name}{table2_only_file}
            FROM (
                SELECT t2.*
                FROM {table2_name} t2
                LEFT JOIN {table1_name} t1 ON {pk_join_condition}
                WHERE t1.{primary_key_columns[0]} IS NULL
            )
            FILE_FORMAT = (FORMAT_NAME = '{format_name}')
            OVERWRITE = TRUE
            HEADER = TRUE
            """
            self.session.sql(table2_only_export_sql).collect()
            stage_files[f'{table2_alias}_only'] = f"{stage_name}{table2_only_file}"
            
            # 3. Export mismatches (unpivoted format)
            mismatch_file = f"table_validation_mismatches_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
            self.logger.info(f"Exporting mismatches to {stage_name}{mismatch_file}...")
            
            pk_select = ", ".join([f"t1.{col}" for col in primary_key_columns])
            mismatch_cases = []
            for col in comparison_columns:
                mismatch_cases.append(f"""
                SELECT {pk_select}, 
                       '{col}' as COLUMN_NAME,
                       COALESCE(CAST(t1.{col} AS STRING), 'NULL') as {table1_alias}_VALUE,
                       COALESCE(CAST(t2.{col} AS STRING), 'NULL') as {table2_alias}_VALUE,
                       '{table1_alias}' as TABLE1_ALIAS,
                       '{table2_alias}' as TABLE2_ALIAS
                FROM {table1_name} t1
                INNER JOIN {table2_name} t2 ON {pk_join_condition}
                WHERE COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL')
                """)
            
            mismatch_export_sql = f"""
            COPY INTO {stage_name}{mismatch_file}
            FROM (
                {' UNION ALL '.join(mismatch_cases)}
            )
            FILE_FORMAT = (FORMAT_NAME = '{format_name}')
            OVERWRITE = TRUE
            HEADER = TRUE
            """
            self.session.sql(mismatch_export_sql).collect()
            stage_files['mismatches'] = f"{stage_name}{mismatch_file}"
            
            # Clean up file format
            self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
            
            self.logger.info("✅ ALL STAGE FILES CREATED SUCCESSFULLY!")
            
        except Exception as e:
            self.logger.error(f"Error creating stage files: {str(e)}")
            # Clean up file format on error
            try:
                self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
            except:
                pass
            raise
        
        return stage_files

    def validate_tables_streaming(self, table1_name: str, table2_name: str,
                                primary_key_columns: Optional[List[str]] = None,
                                comparison_columns: Optional[List[str]] = None,
                                table1_alias: str = "SQL", table2_alias: str = "MDP",
                                environment: str = "SNOWFLAKE",
                                create_tables: bool = True,
                                create_stage_files: bool = True,
                                stage_name: str = "@~/",
                                max_auto_columns: int = 50) -> Dict[str, Any]:
        """
        Memory-efficient streaming validation that processes ALL records without memory limits
        
        This approach:
        1. Uses pure SQL operations in Snowflake
        2. Never loads large datasets into Python memory
        3. Creates complete result tables with ALL records
        4. Provides comprehensive comparison results
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            primary_key_columns: List of columns to use as primary keys
            comparison_columns: List of specific columns to compare
            table1_alias: Alias for first table
            table2_alias: Alias for second table
            environment: Environment name
            create_tables: Whether to create result tables in Snowflake
            create_stage_files: Whether to create CSV files in stage
            stage_name: Snowflake stage for output files
            max_auto_columns: Maximum columns for auto-detection
            
        Returns:
            Dictionary with validation results including ALL records
        """
        self.logger.info("="*80)
        self.logger.info("STARTING STREAMING TABLE VALIDATION (NO MEMORY LIMITS)")
        self.logger.info("="*80)
        self.logger.info(f"Table 1 ({table1_alias}): {table1_name}")
        self.logger.info(f"Table 2 ({table2_alias}): {table2_name}")
        self.logger.info(f"Environment: {environment}")
        self.logger.info(f"Processing: ALL RECORDS (no sampling)")
        self.logger.info(f"Timestamp: {self.timestamp}")
        
        # Get table information
        table1_info = self.get_table_info(table1_name)
        table2_info = self.get_table_info(table2_name)
        if not table1_info or not table2_info:
            self.logger.error("Failed to get table information")
            return None
        
        # Validate table compatibility
        validation_issues = self.validate_table_compatibility(table1_info, table2_info)
        critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
        if critical_issues:
            self.logger.error("Critical validation issues found - cannot proceed with data comparison")
            return {
                'validation_status': 'FAILED',
                'critical_issues': critical_issues,
                'validation_issues': validation_issues
            }
        
        # Identify primary key columns
        pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
        if not pk_columns:
            self.logger.error("No valid primary key columns identified")
            return None
        
        # Identify comparison columns
        comp_columns = self.identify_comparison_columns(table1_info, table2_info, pk_columns, comparison_columns, max_auto_columns)
        if not comp_columns:
            self.logger.error("No valid comparison columns identified")
            return None
        
        self.logger.info(f"Primary Keys: {', '.join(pk_columns)}")
        self.logger.info(f"Comparison Columns ({len(comp_columns)}): {', '.join(comp_columns[:10])}{'...' if len(comp_columns) > 10 else ''}")
        
        results = {
            'validation_status': 'IN_PROGRESS',
            'timestamp': self.timestamp,
            'environment': environment,
            'table1_info': table1_info,
            'table2_info': table2_info,
            'validation_issues': validation_issues,
            'primary_key_columns': pk_columns,
            'comparison_columns': comp_columns,
            'created_tables': {},
            'stage_files': {}
        }
        
        try:
            # Create result tables directly in Snowflake (no memory usage)
            if create_tables:
                self.logger.info("Creating comprehensive result tables...")
                table_results = self.create_result_tables_directly(
                    table1_name, table2_name, pk_columns, comp_columns, table1_alias, table2_alias
                )
                results['created_tables'] = {
                    'mismatches': table_results['mismatch_table'],
                    f'{table1_alias}_only': table_results['table1_only_table'],
                    f'{table2_alias}_only': table_results['table2_only_table'],
                    'summary': table_results['summary_table']
                }
                results['summary_statistics'] = table_results
            
            # Create stage files directly from SQL (no memory usage)
            if create_stage_files:
                self.logger.info("Creating stage files...")
                stage_results = self.create_stage_files_streaming(
                    table1_name, table2_name, pk_columns, comp_columns, table1_alias, table2_alias, stage_name
                )
                results['stage_files'] = stage_results
            
            # Determine final validation status
            if create_tables:
                stats = results['summary_statistics']
                validation_passed = (
                    stats['table1_only_count'] == 0 and 
                    stats['table2_only_count'] == 0 and 
                    stats['unique_mismatch_rows'] == 0 and
                    len(validation_issues) == 0
                )
                results['validation_status'] = 'PASSED' if validation_passed else 'FAILED'
                
                results['results_summary'] = {
                    'mismatched_rows_count': stats['unique_mismatch_rows'],
                    f'{table1_alias}_only_rows_count': stats['table1_only_count'],
                    f'{table2_alias}_only_rows_count': stats['table2_only_count'],
                    'total_issues_found': stats['table1_only_count'] + stats['table2_only_count'] + stats['unique_mismatch_rows']
                }
            else:
                results['validation_status'] = 'COMPLETED_NO_TABLES'
            
            self.logger.info("="*80)
            self.logger.info("✅ STREAMING VALIDATION COMPLETED SUCCESSFULLY!")
            self.logger.info("✅ ALL RECORDS PROCESSED - NO MEMORY LIMITATIONS!")
            self.logger.info("="*80)
            
        except Exception as e:
            self.logger.error(f"Error during streaming validation: {str(e)}")
            results['validation_status'] = 'ERROR'
            results['error'] = str(e)
            raise
        
        return results

    def print_streaming_validation_report(self, results: Dict[str, Any]):
        """
        Print a comprehensive report for streaming validation results
        """
        print("\n" + "="*80)
        print("STREAMING TABLE VALIDATION REPORT (ALL RECORDS)")
        print("="*80)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Environment: {results['environment']}")
        print(f"Validation Status: {results['validation_status']}")
        
        if 'summary_statistics' in results:
            stats = results['summary_statistics']
            print(f"\nTable Information:")
            print(f"  Table 1: {results['table1_info']['table_name']}")
            print(f"    - Row Count: {stats['table1_count']:,}")
            print(f"  Table 2: {results['table2_info']['table_name']}")
            print(f"    - Row Count: {stats['table2_count']:,}")
            
            summary = results['results_summary']
            print(f"\nValidation Results (ALL RECORDS):")
            print(f"  Unique Mismatched Rows: {summary['mismatched_rows_count']:,}")
            print(f"  Total Mismatch Data Points: {stats['total_mismatch_data_points']:,}")
            print(f"  Rows only in {stats['table1_alias']}: {summary[f\"{stats['table1_alias']}_only_rows_count\"]:,}")
            print(f"  Rows only in {stats['table2_alias']}: {summary[f\"{stats['table2_alias']}_only_rows_count\"]:,}")
            print(f"  Total Issues Found: {summary['total_issues_found']:,}")
            
            print(f"\nProcessing Details:")
            print(f"  Processing Method: Pure SQL (No Memory Limits)")
            print(f"  Comparison Columns: {len(results['comparison_columns'])} columns")
            print(f"  Selected Columns: {', '.join(results['comparison_columns'][:10])}{'...' if len(results['comparison_columns']) > 10 else ''}")
            print(f"  Primary Key Columns: {', '.join(results['primary_key_columns'])}")
        
        if results.get('validation_issues'):
            print(f"\nValidation Issues:")
            for issue in results['validation_issues']:
                print(f"  - {issue['type']}: {issue.get('missing_columns', 'N/A')} ({issue['severity']})")
        
        if results.get('created_tables'):
            print(f"\nCreated Snowflake Tables (Complete Data):")
            for table_type, table_name in results['created_tables'].items():
                print(f"  {table_type}: {table_name}")
        
        if results.get('stage_files'):
            print(f"\nSnowflake Stage Files (Complete Data):")
            for file_type, filepath in results['stage_files'].items():
                print(f"  {file_type}: {filepath}")
        
        print(f"\n" + "="*80)
        print("KEY ADVANTAGES OF STREAMING APPROACH")
        print("="*80)
        print("✅ NO MEMORY LIMITATIONS - Processes tables of any size")
        print("✅ ALL RECORDS INCLUDED - No sampling or truncation") 
        print("✅ PURE SQL PROCESSING - Leverages Snowflake's compute power")
        print("✅ COMPLETE RESULTS - Full datasets in tables and files")
        print("✅ SCALABLE SOLUTION - Works with billions of rows")
        
        print(f"\n" + "="*80)
        print("NEXT STEPS")
        print("="*80)
        print("1. Query result tables directly for detailed analysis")
        print("2. Download CSV files from stage for external analysis") 
        print("3. Use result tables for further processing or reporting")
        print("4. All mismatches are in unpivoted format for easy analysis")


def validate_tables_streaming_sp(session: Session, 
                               table1_name: str, 
                               table2_name: str,
                               primary_key_columns: str = None,
                               comparison_columns: str = None,
                               table1_alias: str = "SQL",
                               table2_alias: str = "MDP", 
                               environment: str = "SNOWFLAKE",
                               create_tables: bool = True,
                               create_stage_files: bool = True,
                               stage_name: str = "@~/",
                               max_auto_columns: int = 50) -> str:
    """
    Stored procedure version for streaming validation with no memory limits
    
    Args:
        session: Snowpark session
        table1_name: Fully qualified name of first table
        table2_name: Fully qualified name of second table
        primary_key_columns: Comma-separated list of primary key column names
        comparison_columns: Comma-separated list of columns to compare
        table1_alias: Alias for first table
        table2_alias: Alias for second table
        environment: Database environment
        create_tables: Whether to create result tables
        create_stage_files: Whether to create stage files
        stage_name: Snowflake stage for output files
        max_auto_columns: Maximum columns for auto-detection
    
    Returns:
        Validation results summary as string
    """
    try:
        # Parse primary key columns
        pk_columns = None
        if primary_key_columns:
            pk_columns = [col.strip().upper() for col in primary_key_columns.split(',')]
        
        # Parse comparison columns
        comp_columns = None
        if comparison_columns:
            comp_columns = [col.strip().upper() for col in comparison_columns.split(',')]
        
        # Create streaming validator
        validator = StreamingTableValidator(session)
        
        # Run streaming validation (no memory limits)
        results = validator.validate_tables_streaming(
            table1_name=table1_name,
            table2_name=table2_name,
            primary_key_columns=pk_columns,
            comparison_columns=comp_columns,
            table1_alias=table1_alias,
            table2_alias=table2_alias,
            environment=environment,
            create_tables=create_tables,
            create_stage_files=create_stage_files,
            stage_name=stage_name,
            max_auto_columns=max_auto_columns
        )
        
        if results and 'summary_statistics' in results:
            summary = results['results_summary']
            stats = results['summary_statistics']
            
            return f"""
Streaming Table Validation {results['validation_status']} - ALL RECORDS PROCESSED
Environment: {results['environment']}
{stats['table1_alias']} ({results['table1_info']['table_name']}): {stats['table1_count']:,} rows
{stats['table2_alias']} ({results['table2_info']['table_name']}): {stats['table2_count']:,} rows
Unique Mismatched Rows: {summary['mismatched_rows_count']:,}
Total Mismatch Data Points: {stats['total_mismatch_data_points']:,}
{stats['table1_alias']} Only: {summary[f"{stats['table1_alias']}_only_rows_count"]:,}
{stats['table2_alias']} Only: {summary[f"{stats['table2_alias']}_only_rows_count"]:,}
Total Issues: {summary['total_issues_found']:,}
Primary Keys: {', '.join(results['primary_key_columns'])}
Comparison Columns: {len(results['comparison_columns'])}
Processing: Pure SQL - No Memory Limits - Complete Dataset
                    """.strip()
        else:
            return f"Streaming table validation {results.get('validation_status', 'failed')} to complete"
            
    except Exception as e:
        return f"Error during streaming table validation: {str(e)}"


def main_streaming():
    """
    Main function for testing the streaming table validator
    Processes ALL records without memory limitations
    """
    try:
        validator = StreamingTableValidator()
        
        print("🚀 TESTING STREAMING TABLE VALIDATOR (NO MEMORY LIMITS)")
        print("="*80)
        
        # Run comprehensive streaming validation
        results = validator.validate_tables_streaming(
            table1_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
            table2_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD',
            primary_key_columns=['MEMBER_ID'],
            comparison_columns=None,  # Auto-detect or specify ['COL1', 'COL2'] for specific columns
            table1_alias='SQL',
            table2_alias='MDP',
            environment='SNOWFLAKE_DEV',
            create_tables=True,        # Creates complete result tables
            create_stage_files=True,   # Creates complete CSV files
            stage_name='@~/',
            max_auto_columns=50        # Adjust as needed for auto-detection
        )
        
        if results:
            validator.print_streaming_validation_report(results)
            
            # Show how to query the results
            if results.get('created_tables'):
                print(f"\n" + "="*80)
                print("SAMPLE QUERIES FOR RESULT ANALYSIS")
                print("="*80)
                
                tables = results['created_tables']
                
                if 'mismatches' in tables:
                    print(f"\n-- Query mismatches by column:")
                    print(f"SELECT COLUMN_NAME, COUNT(*) as MISMATCH_COUNT")
                    print(f"FROM {tables['mismatches']}")
                    print(f"GROUP BY COLUMN_NAME")
                    print(f"ORDER BY MISMATCH_COUNT DESC;")
                    
                    print(f"\n-- Sample mismatches for specific column:")
                    print(f"SELECT * FROM {tables['mismatches']}")
                    print(f"WHERE COLUMN_NAME = 'YOUR_COLUMN_NAME'")
                    print(f"LIMIT 10;")
                
                if 'SQL_only' in tables:
                    print(f"\n-- Count of SQL-only records:")
                    print(f"SELECT COUNT(*) FROM {tables['SQL_only']};")
                
                if 'MDP_only' in tables:
                    print(f"\n-- Count of MDP-only records:")
                    print(f"SELECT COUNT(*) FROM {tables['MDP_only']};")
            
            return results
        else:
            print("❌ Streaming validation failed to complete")
            return None
            
    except Exception as e:
        print(f"❌ Error during streaming validation: {str(e)}")
        logging.error(f"Streaming validation error: {str(e)}")
        return None


# Example usage patterns:

def example_usage():
    """
    Examples of different usage patterns for the streaming validator
    """
    
    # Example 1: Complete validation with all columns
    print("# Example 1: Complete validation")
    print("""
validator = StreamingTableValidator()
results = validator.validate_tables_streaming(
    table1_name='DB.SCHEMA.TABLE1',
    table2_name='DB.SCHEMA.TABLE2',
    primary_key_columns=['ID'],
    comparison_columns=None,  # Auto-detect all columns
    create_tables=True,       # Get ALL results in tables
    create_stage_files=True   # Get ALL results in CSV files
)
    """)
    
    # Example 2: Specific columns only
    print("\n# Example 2: Specific columns validation")
    print("""
results = validator.validate_tables_streaming(
    table1_name='DB.SCHEMA.TABLE1',
    table2_name='DB.SCHEMA.TABLE2',
    primary_key_columns=['MEMBER_ID'],
    comparison_columns=['NAME', 'EMAIL', 'PHONE'],  # Only these columns
    table1_alias='DEV',
    table2_alias='PROD'
)
    """)
    
    # Example 3: Large table optimization
    print("\n# Example 3: Large table with column limit")
    print("""
results = validator.validate_tables_streaming(
    table1_name='DB.SCHEMA.HUGE_TABLE1',
    table2_name='DB.SCHEMA.HUGE_TABLE2',
    primary_key_columns=['ID1', 'ID2'],  # Composite key
    max_auto_columns=20,  # Limit auto-detection to first 20 columns
    create_tables=True    # Still gets ALL records, just fewer columns
)
    """)


if __name__ == "__main__":
    main_streaming()
