def validate_tables_sp(session: Session, 
                      table1_name: str, 
                      table2_name: str,
                      primary_key_columns: str = None,
                      comparison_columns: str = None,
                      table1_alias: str = "SQL",
                      table2_alias: str = "MDP", 
                      environment: str = "SNOWFLAKE",
                      create_tables: bool = True,
                      stage_name: str = "@~/",
                      target_schema: str = None,
                      max_sample_size = None,
                      max_auto_columns: int = None) -> str:# The Snowpark package is required for Python Worksheets. 
# You can add more packages by selecting them using the Packages control and then importing them.

import snowflake.snowpark as snowpark
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
        Initialize the Generic Table Validator with streaming capabilities
        
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
        
        Args:
            table1_info: Information about table 1
            table2_info: Information about table 2
            primary_key_columns: List of primary key columns (to exclude from comparison)
            comparison_columns: Optional list of specific columns to compare
            max_auto_columns: Maximum number of columns to auto-select (only used when comparison_columns is None)
            
        Returns:
            List of column names to compare
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
            # If too many columns, limit based on user preference
            selected_cols = list(available_comparison_cols)[:max_auto_columns]
            self.logger.info(f"Found {len(available_comparison_cols)} comparison columns. Auto-selected first {max_auto_columns} columns: {selected_cols}")
        else:
            selected_cols = list(available_comparison_cols)
            self.logger.info(f"Using all available comparison columns ({len(selected_cols)} columns): {selected_cols}")
        
        return selected_cols
    
    def _normalize_data_for_csv(self, data: List[Dict]) -> List[Dict]:
        """
        Normalize data to handle mixed data types and empty strings for CSV export
        """
        if not data:
            return data
        
        normalized_data = []
        for row in data:
            normalized_row = {}
            for key, value in row.items():
                # Convert all values to strings and handle None/NaN
                if pd.isna(value) or value is None:
                    normalized_row[key] = ""  # Empty string instead of None
                elif isinstance(value, (int, float)):
                    normalized_row[key] = str(value)
                elif isinstance(value, str):
                    # Handle empty strings - keep them as empty strings
                    normalized_row[key] = value if value else ""
                else:
                    # Convert other types to string
                    normalized_row[key] = str(value)
            normalized_data.append(normalized_row)
        
        return normalized_data
    
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
            
            # Normalize data for consistent data types
            normalized_data = self._normalize_data_for_csv(data)
            df = pd.DataFrame(normalized_data)
            
            # Clean column names
            df.columns = [col.replace('(', '').replace(')', '').replace(' ', '_').replace('-', '_') for col in df.columns]
            
            snowpark_df = self.session.create_dataframe(df)
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            self.logger.info(f"Created Snowflake table: {table_name} with {len(data)} rows")
        except Exception as e:
            self.logger.error(f"Error creating Snowflake table {table_name}: {str(e)}")
            print(f"Error creating Snowflake table {table_name}: {str(e)}")
    
    def write_to_snowflake_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
        """
        Write data to Snowflake stage as CSV with proper file format options
        """
        if not data:
            self.logger.info(f"No data to write to stage {filename}")
            print(f"\nStage file {filename} - No data to write.")
            return
        try:
            self._print_first_n_rows(data, 10, f"Stage file {filename}")
            
            # Normalize data to handle mixed data types and empty strings
            normalized_data = self._normalize_data_for_csv(data)
            df = pd.DataFrame(normalized_data)
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Create a file format that can handle empty strings
            format_name = f"CSV_FORMAT_{self.timestamp}"
            try:
                # Drop format if it exists
                self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
                
                # Create file format with proper options for empty strings
                self.session.sql(f"""
                    CREATE FILE FORMAT {format_name}
                    TYPE = 'CSV'
                    FIELD_DELIMITER = ','
                    RECORD_DELIMITER = '\\n'
                    SKIP_HEADER = 1
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ('NULL', 'null')
                    EMPTY_FIELD_AS_NULL = FALSE
                    COMPRESSION = 'NONE'
                """).collect()
                
                # Write to stage using the custom file format
                self.session.sql(f"""
                    COPY INTO {stage_name}{filename}
                    FROM (SELECT * FROM ({snowpark_df.select("*").queries['queries'][0]}))
                    FILE_FORMAT = (FORMAT_NAME = '{format_name}')
                    OVERWRITE = TRUE
                    HEADER = TRUE
                """).collect()
                
                # Clean up the file format
                self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
                
                self.logger.info(f"Created stage file: {stage_name}{filename}")
                
            except Exception as format_error:
                self.logger.warning(f"Custom file format approach failed: {str(format_error)}")
                # Fallback: try the original approach
                snowpark_df.write.mode("overwrite").csv(f"{stage_name}{filename}")
                self.logger.info(f"Created stage file using fallback method: {stage_name}{filename}")
                
        except Exception as e:
            self.logger.error(f"Error writing to stage: {str(e)}")
            print(f"Error writing to stage {filename}: {str(e)}")
    
    def compare_tables_memory_optimized(self, table1_name: str, table2_name: str,
                                       primary_key_columns: List[str],
                                       comparison_columns: List[str],
                                       table1_alias: str = "SQL", 
                                       table2_alias: str = "MDP",
                                       max_sample_size = 1000,
                                       user_specified_columns: bool = False) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Memory-optimized comparison using pure SQL - NO MEMORY LIMITS when streaming enabled
        
        Args:
            max_sample_size: int for auto-columns, False for unlimited when user specifies columns  
            user_specified_columns: True if user explicitly specified comparison columns
        
        Returns:
            Tuple of (mismatched_rows, table1_only_rows, table2_only_rows, summary_stats)
        """
        mismatched_rows = []
        table1_only_rows = []
        table2_only_rows = []
        summary_stats = {}
        
        # Calculate effective sample size and strategy based on user input
        if user_specified_columns:
            if max_sample_size is False:
                # User specified columns and wants unlimited - USE STREAMING APPROACH
                return self._compare_tables_streaming_all_records(
                    table1_name, table2_name, primary_key_columns, comparison_columns, 
                    table1_alias, table2_alias
                )
            else:
                # User specified columns but also set a sample limit
                effective_sample_size = max_sample_size
                sampling_note = f"limited to {max_sample_size} (user-override)"
                self.logger.info(f"User-specified columns mode: processing {len(comparison_columns)} columns with {effective_sample_size} max samples (user-override)")
        else:
            # Auto-detected columns - always use conservative sampling unless streaming requested
            if max_sample_size is False:
                self.logger.warning("max_sample_size=False with auto-detected columns - using streaming approach for ALL records")
                return self._compare_tables_streaming_all_records(
                    table1_name, table2_name, primary_key_columns, comparison_columns, 
                    table1_alias, table2_alias
                )
            else:
                effective_sample_size = max_sample_size
                sampling_note = f"limited to {effective_sample_size} (auto-conservative)"
                self.logger.info(f"Auto-detected columns mode: processing {len(comparison_columns)} columns with {effective_sample_size} max samples (memory-conservative)")
        
        # Continue with original sampling approach for limited cases
        try:
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            if not table1_info or not table2_info:
                self.logger.error("Failed to get table information")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats

            # Create temporary views
            table1_df = self.session.table(table1_name)
            table2_df = self.session.table(table2_name)
            table1_df.create_or_replace_temp_view("temp_table1")
            table2_df.create_or_replace_temp_view("temp_table2")
            
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # 1. Count and sample rows in TABLE1 but not in TABLE2
            self.logger.info(f"Processing rows in {table1_alias} but not in {table2_alias}...")
            table1_only_count_sql = f"""
            SELECT COUNT(*) as count_only
            FROM temp_table1 t1
            LEFT JOIN temp_table2 t2 ON {pk_join_condition}
            WHERE t2.{primary_key_columns[0]} IS NULL
            """
            count_result = self.session.sql(table1_only_count_sql).collect()
            table1_only_count = count_result[0]['COUNT_ONLY']
            
            if table1_only_count > 0:
                # Select columns based on user specification and sampling strategy
                if user_specified_columns:
                    sample_cols = primary_key_columns + comparison_columns  # Use all user-specified columns
                else:
                    sample_cols = primary_key_columns + comparison_columns[:5]  # Limit for auto-detected
                
                select_cols = ", ".join([f"t1.{col}" for col in sample_cols])
                limit_clause = f"LIMIT {effective_sample_size}"
                
                table1_only_sql = f"""
                SELECT {select_cols}
                FROM temp_table1 t1
                LEFT JOIN temp_table2 t2 ON {pk_join_condition}
                WHERE t2.{primary_key_columns[0]} IS NULL
                {limit_clause}
                """
                table1_only_result = self.session.sql(table1_only_sql)
                table1_only_df = table1_only_result.to_pandas()
                for _, row in table1_only_df.iterrows():
                    row_dict = {}
                    for key, value in row.to_dict().items():
                        if pd.isna(value):
                            row_dict[key] = None
                        else:
                            row_dict[key] = value
                    table1_only_rows.append(row_dict)
            
            # 2. Count and sample rows in TABLE2 but not in TABLE1
            self.logger.info(f"Processing rows in {table2_alias} but not in {table1_alias}...")
            table2_only_count_sql = f"""
            SELECT COUNT(*) as count_only
            FROM temp_table2 t2
            LEFT JOIN temp_table1 t1 ON {pk_join_condition}
            WHERE t1.{primary_key_columns[0]} IS NULL
            """
            count_result = self.session.sql(table2_only_count_sql).collect()
            table2_only_count = count_result[0]['COUNT_ONLY']
            
            if table2_only_count > 0:
                # Select columns based on user specification and sampling strategy
                if user_specified_columns:
                    sample_cols = primary_key_columns + comparison_columns  # Use all user-specified columns
                else:
                    sample_cols = primary_key_columns + comparison_columns[:5]  # Limit for auto-detected
                
                select_cols = ", ".join([f"t2.{col}" for col in sample_cols])
                limit_clause = f"LIMIT {effective_sample_size}"
                
                table2_only_sql = f"""
                SELECT {select_cols}
                FROM temp_table2 t2
                LEFT JOIN temp_table1 t1 ON {pk_join_condition}
                WHERE t1.{primary_key_columns[0]} IS NULL
                {limit_clause}
                """
                table2_only_result = self.session.sql(table2_only_sql)
                table2_only_df = table2_only_result.to_pandas()
                for _, row in table2_only_df.iterrows():
                    row_dict = {}
                    for key, value in row.to_dict().items():
                        if pd.isna(value):
                            row_dict[key] = None
                        else:
                            row_dict[key] = value
                    table2_only_rows.append(row_dict)
            
            # 3. Process mismatches using single-column approach
            self.logger.info("Processing column mismatches...")
            
            # Count total mismatches first
            comparison_conditions = []
            for col in comparison_columns:
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
                    # Process each column individually like original script
                    for col in comparison_columns:
                        single_col_sql = f"""
                        SELECT t1.{', '.join(primary_key_columns)}, 
                               t1.{col} as {table1_alias}_{col}, 
                               t2.{col} as {table2_alias}_{col}
                        FROM temp_table1 t1
                        INNER JOIN temp_table2 t2 ON {pk_join_condition}
                        WHERE COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL')
                        LIMIT {effective_sample_size}
                        """
                        
                        single_result = self.session.sql(single_col_sql)
                        single_df = single_result.to_pandas()
                        
                        # Process results for this single column
                        for _, row in single_df.iterrows():
                            pk_values = {}
                            for pk_col in primary_key_columns:
                                pk_values[pk_col] = row[pk_col]
                            
                            table1_val = row.get(f'{table1_alias}_{col}')
                            table2_val = row.get(f'{table2_alias}_{col}')
                            
                            mismatch_record = {
                                **pk_values,
                                'COLUMN_NAME': col,
                                f'{table1_alias}_VALUE': table1_val if pd.notna(table1_val) else None,
                                f'{table2_alias}_VALUE': table2_val if pd.notna(table2_val) else None,
                                'TABLE1_ALIAS': table1_alias,
                                'TABLE2_ALIAS': table2_alias
                            }
                            mismatched_rows.append(mismatch_record)
                            
                            # Apply sample limit across all columns
                            if len(mismatched_rows) >= effective_sample_size:
                                break
                        
                        # Break if we've reached the sample limit
                        if len(mismatched_rows) >= effective_sample_size:
                            break
                
                estimated_total_mismatches = total_mismatch_rows * len(comparison_columns)
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
                'primary_key_columns': primary_key_columns,
                'comparison_columns': comparison_columns,
                'total_comparison_columns': len(comparison_columns),
                'max_sample_size': effective_sample_size,
                'user_specified_columns': user_specified_columns,
                'memory_strategy': 'user-optimized' if user_specified_columns else 'auto-conservative',
                'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'note': f'Results are {sampling_note}'
            }
            
            self.logger.info(f"Memory-optimized comparison completed:")
            self.logger.info(f"  - Total {table1_alias} rows: {summary_stats['total_table1_rows']}")
            self.logger.info(f"  - Total {table2_alias} rows: {summary_stats['total_table2_rows']}")
            self.logger.info(f"  - {table1_alias} only rows: {table1_only_count} (sampled: {len(table1_only_rows)})")
            self.logger.info(f"  - {table2_alias} only rows: {table2_only_count} (sampled: {len(table2_only_rows)})")
            self.logger.info(f"  - Mismatched rows: {total_mismatch_rows} (sampled: {len(mismatched_rows)})")
            self.logger.info(f"  - Comparison columns: {len(comparison_columns)}")
            
        except Exception as e:
            self.logger.error(f"Error during memory-optimized comparison: {str(e)}")
            raise
        
        return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats

    def _compare_tables_streaming_all_records(self, table1_name: str, table2_name: str,
                                            primary_key_columns: List[str],
                                            comparison_columns: List[str],
                                            table1_alias: str = "SQL", 
                                            table2_alias: str = "MDP") -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Streaming comparison that gets ALL records using pure SQL - NO memory limits
        Creates temporary tables with results and returns counts only
        """
        self.logger.info("🚀 STREAMING MODE: Processing ALL records with no memory limits")
        
        # Generate temporary table names
        temp_mismatches = f"TEMP_MISMATCHES_{self.timestamp}"
        temp_table1_only = f"TEMP_{table1_alias}_ONLY_{self.timestamp}"
        temp_table2_only = f"TEMP_{table2_alias}_ONLY_{self.timestamp}"
        
        try:
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # 1. Create table for TABLE1-only rows (ALL records)
            self.logger.info(f"Creating complete {table1_alias}-only table...")
            table1_only_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table1_only} AS
            SELECT t1.*
            FROM {table1_name} t1
            LEFT JOIN {table2_name} t2 ON {pk_join_condition}
            WHERE t2.{primary_key_columns[0]} IS NULL
            """
            self.session.sql(table1_only_sql).collect()
            table1_only_count = self.session.sql(f"SELECT COUNT(*) as CNT FROM {temp_table1_only}").collect()[0]['CNT']
            
            # 2. Create table for TABLE2-only rows (ALL records)
            self.logger.info(f"Creating complete {table2_alias}-only table...")
            table2_only_sql = f"""
            CREATE OR REPLACE TEMPORARY TABLE {temp_table2_only} AS
            SELECT t2.*
            FROM {table2_name} t2
            LEFT JOIN {table1_name} t1 ON {pk_join_condition}
            WHERE t1.{primary_key_columns[0]} IS NULL
            """
            self.session.sql(table2_only_sql).collect()
            table2_only_count = self.session.sql(f"SELECT COUNT(*) as CNT FROM {temp_table2_only}").collect()[0]['CNT']
            
            # 3. Create mismatches table (ALL records) using unpivoted approach
            self.logger.info("Creating complete mismatches table...")
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
            
            if mismatch_cases:
                mismatch_sql = f"""
                CREATE OR REPLACE TEMPORARY TABLE {temp_mismatches} AS
                {' UNION ALL '.join(mismatch_cases)}
                """
                self.session.sql(mismatch_sql).collect()
                mismatch_count = self.session.sql(f"SELECT COUNT(*) as CNT FROM {temp_mismatches}").collect()[0]['CNT']
                
                # Count unique mismatched rows
                unique_mismatch_sql = f"""
                SELECT COUNT(DISTINCT CONCAT({', '.join([f"COALESCE(CAST({col} AS STRING), 'NULL')" for col in primary_key_columns])})) as CNT
                FROM {temp_mismatches}
                """
                unique_mismatch_rows = self.session.sql(unique_mismatch_sql).collect()[0]['CNT']
            else:
                mismatch_count = 0
                unique_mismatch_rows = 0
            
            # Store temp table names for later use
            self._temp_tables = {
                'mismatches': temp_mismatches,
                'table1_only': temp_table1_only,
                'table2_only': temp_table2_only
            }
            
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
                'mismatched_rows_count': unique_mismatch_rows,
                'mismatched_data_points': mismatch_count,
                'estimated_total_mismatches': mismatch_count,
                'primary_key_columns': primary_key_columns,
                'comparison_columns': comparison_columns,
                'total_comparison_columns': len(comparison_columns),
                'max_sample_size': "ALL RECORDS",
                'user_specified_columns': True,
                'memory_strategy': 'streaming-all-records',
                'validation_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                'note': 'ALL RECORDS processed - no sampling limits',
                'temp_tables': self._temp_tables
            }
            
            self.logger.info("✅ STREAMING COMPARISON COMPLETED:")
            self.logger.info(f"  - Total {table1_alias} rows: {table1_info['row_count']:,}")
            self.logger.info(f"  - Total {table2_alias} rows: {table2_info['row_count']:,}")
            self.logger.info(f"  - {table1_alias} only rows: {table1_only_count:,}")
            self.logger.info(f"  - {table2_alias} only rows: {table2_only_count:,}")
            self.logger.info(f"  - Unique mismatched rows: {unique_mismatch_rows:,}")
            self.logger.info(f"  - Total mismatch data points: {mismatch_count:,}")
            
            # Return empty lists since data is in temp tables
            return [], [], [], summary_stats
            
        except Exception as e:
            self.logger.error(f"Error during streaming comparison: {str(e)}")
            raise

    def validate_tables(self, table1_name: str, table2_name: str,
                       primary_key_columns: Optional[List[str]] = None,
                       comparison_columns: Optional[List[str]] = None,
                       table1_alias: str = "SQL", table2_alias: str = "MDP",
                       environment: str = "SNOWFLAKE",
                       create_tables: bool = True,
                       stage_name: str = "@~/",
                       target_schema: str = None,
                       max_sample_size = None,
                       max_auto_columns: int = None) -> Dict[str, Any]:
        """
        Enhanced table validation with smart sampling based on column specification
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            primary_key_columns: List of columns to use as primary keys
            comparison_columns: List of specific columns to compare (if None, uses auto-detection)
            table1_alias: Alias for first table
            table2_alias: Alias for second table
            environment: Environment name
            create_tables: Whether to create result tables
            stage_name: Snowflake stage for output files
            target_schema: Schema where result tables should be created (if None, uses same schema as table1)
            max_sample_size: None=auto-decide, int=specific limit, False=unlimited (user-controllable)
            max_auto_columns: None=default(50), int=user-specified limit for auto-detection
            
        Returns:
            Dictionary with validation results
        """
        # Smart defaults based on user input
        if max_sample_size is None:
            # Auto-decide based on whether user specified columns
            if comparison_columns:
                final_max_sample_size = False  # Unlimited when user specifies columns
            else:
                final_max_sample_size = 1000   # Conservative default for auto-detection
        else:
            # User explicitly set it
            final_max_sample_size = max_sample_size
        
        if max_auto_columns is None:
            final_max_auto_columns = 50  # Default
        else:
            final_max_auto_columns = max_auto_columns
        
        # Determine target schema for result tables
        if target_schema is None:
            # Extract schema from table1_name (e.g., "DB.SCHEMA.TABLE" -> "DB.SCHEMA")
            table_parts = table1_name.split('.')
            if len(table_parts) >= 2:
                target_schema = '.'.join(table_parts[:-1])
            else:
                target_schema = "PUBLIC"  # Default fallback
        
        self.logger.info("="*80)
        self.logger.info("STARTING MEMORY-OPTIMIZED TABLE VALIDATION")
        self.logger.info("="*80)
        self.logger.info(f"Table 1 ({table1_alias}): {table1_name}")
        self.logger.info(f"Table 2 ({table2_alias}): {table2_name}")
        self.logger.info(f"Environment: {environment}")
        self.logger.info(f"Target Schema: {target_schema}")
        self.logger.info(f"Primary Key Columns: {primary_key_columns}")
        self.logger.info(f"Comparison Columns: {comparison_columns if comparison_columns else f'Auto-detect (max {final_max_auto_columns})'}")
        self.logger.info(f"Max Sample Size: {'Auto-decided' if max_sample_size is None else ('False (unlimited)' if final_max_sample_size is False else final_max_sample_size)}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        
        # Initialize results structure early to ensure we always return something
        results = {
            'validation_status': 'IN_PROGRESS',
            'timestamp': self.timestamp,
            'environment': environment,
            'table1_info': None,
            'table2_info': None,
            'validation_issues': [],
            'summary_statistics': {},
            'results_summary': {},
            'created_tables': {},
            'stage_files': {},
            'error_details': None
        }
        
        try:
            # Determine if user specified columns
            user_specified_columns = comparison_columns is not None
            
            # Get table information
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            if not table1_info or not table2_info:
                self.logger.error("Failed to get table information")
                results.update({
                    'validation_status': 'FAILED',
                    'error_details': 'Failed to get table information - check table names and permissions'
                })
                return results
            
            results['table1_info'] = table1_info
            results['table2_info'] = table2_info
            
            # Validate table compatibility
            validation_issues = self.validate_table_compatibility(table1_info, table2_info)
            results['validation_issues'] = validation_issues
            critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
            if critical_issues:
                self.logger.error("Critical validation issues found - cannot proceed with data comparison")
                results.update({
                    'validation_status': 'FAILED',
                    'critical_issues': critical_issues,
                    'error_details': f'Critical validation issues: {[issue["type"] for issue in critical_issues]}'
                })
                return results
            
            # Identify primary key columns
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                self.logger.error("No valid primary key columns identified")
                results.update({
                    'validation_status': 'FAILED',
                    'error_details': 'No valid primary key columns identified'
                })
                return results
            
            # Identify comparison columns
            comp_columns = self.identify_comparison_columns(table1_info, table2_info, pk_columns, comparison_columns, final_max_auto_columns)
            if not comp_columns:
                self.logger.error("No valid comparison columns identified")
                results.update({
                    'validation_status': 'FAILED',
                    'error_details': 'No valid comparison columns identified'
                })
                return results
            
            # Run memory-optimized comparison
            mismatched_rows, table1_only_rows, table2_only_rows, summary_stats = self.compare_tables_memory_optimized(
                table1_name, table2_name, pk_columns, comp_columns, table1_alias, table2_alias, final_max_sample_size, user_specified_columns
            )
            
            results['summary_statistics'] = summary_stats
            
            # Handle streaming mode results
            is_streaming_mode = summary_stats.get('memory_strategy') == 'streaming-all-records'
            
            # Generate output file names with target schema
            mismatch_table_name = f"{target_schema}.TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
            table1_only_table_name = f"{target_schema}.TABLE_VALIDATION_{table1_alias}_ONLY_{self.timestamp}"
            table2_only_table_name = f"{target_schema}.TABLE_VALIDATION_{table2_alias}_ONLY_{self.timestamp}"
            mismatch_csv = f"table_validation_mismatches_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
            table1_only_csv = f"table_validation_{table1_alias}_only_{self.timestamp}.csv"
            table2_only_csv = f"table_validation_{table2_alias}_only_{self.timestamp}.csv"
            summary_csv = f"table_validation_summary_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
            
            # Create tables if requested
            created_tables = {}
            table_creation_errors = []
            
            if create_tables:
                try:
                    if is_streaming_mode:
                        # In streaming mode, copy from temp tables to permanent tables
                        temp_tables = summary_stats.get('temp_tables', {})
                        
                        if temp_tables.get('mismatches') and summary_stats['mismatched_data_points'] > 0:
                            try:
                                self.session.sql(f"""
                                    CREATE OR REPLACE TABLE {mismatch_table_name} AS
                                    SELECT * FROM {temp_tables['mismatches']}
                                """).collect()
                                created_tables['mismatches'] = mismatch_table_name
                                self.logger.info(f"Created permanent table: {mismatch_table_name} with {summary_stats['mismatched_data_points']:,} rows")
                            except Exception as e:
                                table_creation_errors.append(f"mismatches table: {str(e)}")
                                self.logger.warning(f"Failed to create mismatches table: {str(e)}")
                        
                        if temp_tables.get('table1_only') and summary_stats['rows_only_in_table1'] > 0:
                            try:
                                self.session.sql(f"""
                                    CREATE OR REPLACE TABLE {table1_only_table_name} AS
                                    SELECT * FROM {temp_tables['table1_only']}
                                """).collect()
                                created_tables[f'{table1_alias}_only'] = table1_only_table_name
                                self.logger.info(f"Created permanent table: {table1_only_table_name} with {summary_stats['rows_only_in_table1']:,} rows")
                            except Exception as e:
                                table_creation_errors.append(f"{table1_alias}_only table: {str(e)}")
                                self.logger.warning(f"Failed to create {table1_alias}_only table: {str(e)}")
                        
                        if temp_tables.get('table2_only') and summary_stats['rows_only_in_table2'] > 0:
                            try:
                                self.session.sql(f"""
                                    CREATE OR REPLACE TABLE {table2_only_table_name} AS
                                    SELECT * FROM {temp_tables['table2_only']}
                                """).collect()
                                created_tables[f'{table2_alias}_only'] = table2_only_table_name
                                self.logger.info(f"Created permanent table: {table2_only_table_name} with {summary_stats['rows_only_in_table2']:,} rows")
                            except Exception as e:
                                table_creation_errors.append(f"{table2_alias}_only table: {str(e)}")
                                self.logger.warning(f"Failed to create {table2_alias}_only table: {str(e)}")
                    else:
                        # Original approach for sampled data
                        if mismatched_rows:
                            try:
                                self.create_snowflake_table(mismatch_table_name, mismatched_rows)
                                created_tables['mismatches'] = mismatch_table_name
                            except Exception as e:
                                table_creation_errors.append(f"mismatches table: {str(e)}")
                                self.logger.warning(f"Failed to create mismatches table: {str(e)}")
                        if table1_only_rows:
                            try:
                                self.create_snowflake_table(table1_only_table_name, table1_only_rows)
                                created_tables[f'{table1_alias}_only'] = table1_only_table_name
                            except Exception as e:
                                table_creation_errors.append(f"{table1_alias}_only table: {str(e)}")
                                self.logger.warning(f"Failed to create {table1_alias}_only table: {str(e)}")
                        if table2_only_rows:
                            try:
                                self.create_snowflake_table(table2_only_table_name, table2_only_rows)
                                created_tables[f'{table2_alias}_only'] = table2_only_table_name
                            except Exception as e:
                                table_creation_errors.append(f"{table2_alias}_only table: {str(e)}")
                                self.logger.warning(f"Failed to create {table2_alias}_only table: {str(e)}")
                                
                except Exception as e:
                    self.logger.error(f"Error during table creation: {str(e)}")
                    table_creation_errors.append(f"General table creation error: {str(e)}")
            
            results['created_tables'] = created_tables
            if table_creation_errors:
                results['table_creation_errors'] = table_creation_errors
            
            # Write to stage files
            stage_files = {}
            stage_creation_errors = []
            
            try:
                if is_streaming_mode:
                    # In streaming mode, export directly from temp tables
                    temp_tables = summary_stats.get('temp_tables', {})
                    self._write_streaming_stage_files(temp_tables, stage_name, table1_alias, table2_alias, summary_stats)
                    stage_files = {
                        'mismatches': f"{stage_name}{mismatch_csv}",
                        f'{table1_alias}_only': f"{stage_name}{table1_only_csv}",
                        f'{table2_alias}_only': f"{stage_name}{table2_only_csv}",
                        'summary': f"{stage_name}{summary_csv}"
                    }
                else:
                    # Original approach for sampled data
                    try:
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
                    except Exception as e:
                        stage_creation_errors.append(f"Stage file creation error: {str(e)}")
                        self.logger.warning(f"Error creating stage files: {str(e)}")
                        
            except Exception as e:
                stage_creation_errors.append(f"General stage error: {str(e)}")
                self.logger.warning(f"Error during stage file creation: {str(e)}")
            
            results['stage_files'] = stage_files
            if stage_creation_errors:
                results['stage_creation_errors'] = stage_creation_errors
            
            # Determine validation status based on data, not just table creation
            mismatched_count = summary_stats.get('mismatched_rows_count', len(mismatched_rows))
            table1_only_count = summary_stats.get('rows_only_in_table1', len(table1_only_rows))
            table2_only_count = summary_stats.get('rows_only_in_table2', len(table2_only_rows))
            
            validation_passed = (
                mismatched_count == 0 and 
                table1_only_count == 0 and 
                table2_only_count == 0 and
                len(validation_issues) == 0
            )
            
            # Update results with final status
            results.update({
                'validation_status': 'PASSED' if validation_passed else 'FAILED',
                'results_summary': {
                    'mismatched_rows_count': mismatched_count,
                    f'{table1_alias}_only_rows_count': table1_only_count,
                    f'{table2_alias}_only_rows_count': table2_only_count,
                    'total_issues_found': mismatched_count + table1_only_count + table2_only_count
                }
            })
            
            # Add success details
            if validation_passed:
                results['success_details'] = 'Tables match perfectly - no differences found'
            else:
                issues_found = []
                if mismatched_count > 0:
                    issues_found.append(f"{mismatched_count:,} mismatched rows")
                if table1_only_count > 0:
                    issues_found.append(f"{table1_only_count:,} {table1_alias}-only rows")
                if table2_only_count > 0:
                    issues_found.append(f"{table2_only_count:,} {table2_alias}-only rows")
                results['failure_details'] = f'Differences found: {", ".join(issues_found)}'
            
            self.logger.info(f"✅ VALIDATION COMPLETED: {results['validation_status']}")
            if results['validation_status'] == 'FAILED':
                self.logger.info(f"   Reason: {results.get('failure_details', 'See results for details')}")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error during table validation: {str(e)}")
            results.update({
                'validation_status': 'ERROR',
                'error_details': f'Unexpected error during validation: {str(e)}'
            })
            return results

    def _write_streaming_stage_files(self, temp_tables: Dict, stage_name: str, table1_alias: str, table2_alias: str, summary_stats: Dict):
        """
        Write stage files directly from temp tables for streaming mode
        """
        try:
            # Create file format
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
            
            # Export mismatches
            if temp_tables.get('mismatches') and summary_stats['mismatched_data_points'] > 0:
                mismatch_file = f"table_validation_mismatches_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
                self.session.sql(f"""
                    COPY INTO {stage_name}{mismatch_file}
                    FROM {temp_tables['mismatches']}
                    FILE_FORMAT = (FORMAT_NAME = '{format_name}')
                    OVERWRITE = TRUE
                    HEADER = TRUE
                """).collect()
                self.logger.info(f"Exported streaming mismatches to {stage_name}{mismatch_file}")
            
            # Export table1-only
            if temp_tables.get('table1_only') and summary_stats['rows_only_in_table1'] > 0:
                table1_file = f"table_validation_{table1_alias}_only_{self.timestamp}.csv"
                self.session.sql(f"""
                    COPY INTO {stage_name}{table1_file}
                    FROM {temp_tables['table1_only']}
                    FILE_FORMAT = (FORMAT_NAME = '{format_name}')
                    OVERWRITE = TRUE
                    HEADER = TRUE
                """).collect()
                self.logger.info(f"Exported streaming {table1_alias}-only to {stage_name}{table1_file}")
            
            # Export table2-only
            if temp_tables.get('table2_only') and summary_stats['rows_only_in_table2'] > 0:
                table2_file = f"table_validation_{table2_alias}_only_{self.timestamp}.csv"
                self.session.sql(f"""
                    COPY INTO {stage_name}{table2_file}
                    FROM {temp_tables['table2_only']}
                    FILE_FORMAT = (FORMAT_NAME = '{format_name}')
                    OVERWRITE = TRUE
                    HEADER = TRUE
                """).collect()
                self.logger.info(f"Exported streaming {table2_alias}-only to {stage_name}{table2_file}")
            
            # Export summary
            summary_file = f"table_validation_summary_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
            self.write_to_snowflake_stage([summary_stats], summary_file, stage_name)
            
            # Clean up format
            self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
            
        except Exception as e:
            self.logger.error(f"Error writing streaming stage files: {str(e)}")
            try:
                self.session.sql(f"DROP FILE FORMAT IF EXISTS {format_name}").collect()
            except:
                pass

    def print_sample_mismatches(self, table1_name: str, table2_name: str,
                               primary_key_columns: Optional[List[str]] = None,
                               comparison_columns: Optional[List[str]] = None,
                               table1_alias: str = "SQL", 
                               table2_alias: str = "MDP",
                               sample_size: int = 10,
                               max_auto_columns: int = 50) -> List[Dict]:
        """
        Print sample mismatches for verification with column filtering
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            primary_key_columns: List of columns to use as primary keys
            comparison_columns: List of specific columns to compare
            table1_alias: Alias for first table
            table2_alias: Alias for second table
            sample_size: Number of sample mismatches to show
            max_auto_columns: Maximum columns for auto-detection (when comparison_columns is None)
            
        Returns:
            List of sample mismatch records
        """
        print(f"\n SAMPLE MISMATCHES VERIFICATION ({sample_size} samples)")
        print("="*70)
        
        sample_mismatches = []
        
        try:
            # Get table information
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            
            if not table1_info or not table2_info:
                print(" Error: Could not get table information")
                return sample_mismatches
            
            # Identify primary key columns
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                print(" Error: No valid primary key columns identified")
                return sample_mismatches
            
            # Identify comparison columns
            comp_columns = self.identify_comparison_columns(table1_info, table2_info, pk_columns, comparison_columns, max_auto_columns)
            if not comp_columns:
                print(" Error: No valid comparison columns identified")
                return sample_mismatches
            
            # Limit comparison columns for sample verification (only if auto-detected)
            if comparison_columns:
                limited_comp_columns = comp_columns  # Use all user-specified columns
                print(f" Using user-specified columns: {', '.join(limited_comp_columns)}")
            else:
                limited_comp_columns = comp_columns[:5]  # Only check first 5 columns for auto-detected samples
                print(f" Using auto-detected columns (limited for sample): {', '.join(limited_comp_columns)}")
            
            print(f" Primary Keys: {', '.join(pk_columns)}")
            print()
            
            # Create temporary views
            table1_df = self.session.table(table1_name)
            table2_df = self.session.table(table2_name)
            table1_df.create_or_replace_temp_view("temp_sample_table1")
            table2_df.create_or_replace_temp_view("temp_sample_table2")
            
            # Build primary key join condition
            pk_join_conditions = []
            for col in pk_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # Build select and comparison conditions
            select_columns = []
            comparison_conditions = []
            
            # Add primary key columns
            for col in pk_columns:
                select_columns.append(f"t1.{col}")
            
            # Add comparison columns with both table values
            for col in limited_comp_columns:
                select_columns.extend([f"t1.{col} as {table1_alias}_{col}", f"t2.{col} as {table2_alias}_{col}"])
                comparison_conditions.append(f"(COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))")
            
            # Build sample mismatch query
            sample_sql = f"""
            SELECT {', '.join(select_columns)}
            FROM temp_sample_table1 t1
            INNER JOIN temp_sample_table2 t2 ON {pk_join_condition}
            WHERE {' OR '.join(comparison_conditions)}
            LIMIT {sample_size}
            """
            
            result = self.session.sql(sample_sql)
            sample_df = result.to_pandas()
            
            if len(sample_df) == 0:
                print(" No mismatches found in sample data - tables appear to match!")
                return sample_mismatches
            
            print(f"Found {len(sample_df)} sample mismatches:\n")
            
            # Process and display each sample
            for idx, row in sample_df.iterrows():
                # Extract primary key values
                pk_values = {}
                for col in pk_columns:
                    pk_values[col] = row[col]
                
                print(f"Sample {idx + 1}:")
                print(f"  Primary Key: {', '.join([f'{k}={v}' for k, v in pk_values.items()])}")
                
                # Check each comparison column for differences
                differences_found = []
                for col in limited_comp_columns:
                    table1_val = row.get(f'{table1_alias}_{col}')
                    table2_val = row.get(f'{table2_alias}_{col}')
                    
                    # Check if values are different
                    if pd.isna(table1_val) and pd.isna(table2_val):
                        continue  # Both are null, no difference
                    elif pd.isna(table1_val) or pd.isna(table2_val) or str(table1_val) != str(table2_val):
                        differences_found.append({
                            'column': col,
                            'table1_value': table1_val if pd.notna(table1_val) else 'NULL',
                            'table2_value': table2_val if pd.notna(table2_val) else 'NULL'
                        })
                        print(f"    ✓ {col}: {table1_alias}='{table1_val}' vs {table2_alias}='{table2_val}'")
                
                # Store sample record
                sample_record = {
                    **pk_values,
                    'differences': differences_found,
                    'total_differences': len(differences_found)
                }
                sample_mismatches.append(sample_record)
                print()  # Add blank line between samples
            
            print(f" Verification complete! Found {len(sample_mismatches)} records with differences.")
            print(f" Columns compared: {', '.join(limited_comp_columns)}")
            print(f" Primary keys used: {', '.join(pk_columns)}")
            
        except Exception as e:
            self.logger.error(f"Error during sample verification: {str(e)}")
            print(f" Error during sample verification: {str(e)}")
        
        return sample_mismatches

    def print_validation_report(self, results: Dict[str, Any]):
        """
        Print a comprehensive validation report
        
        Args:
            results: Results dictionary from validation
        """
        print("\n" + "="*80)
        print("MEMORY-OPTIMIZED TABLE VALIDATION REPORT")
        print("="*80)
        print(f"Timestamp: {results['timestamp']}")
        print(f"Environment: {results['environment']}")
        print(f"Validation Status: {results['validation_status']}")
        
        stats = results['summary_statistics']
        print(f"\nTable Information:")
        print(f"  {stats['table1_alias']} Table: {stats['table1_name']}")
        print(f"    - Row Count: {stats['total_table1_rows']:,}")
        print(f"  {stats['table2_alias']} Table: {stats['table2_name']}")
        print(f"    - Row Count: {stats['total_table2_rows']:,}")
        
        summary = results['results_summary']
        table1_alias = stats['table1_alias']
        table2_alias = stats['table2_alias']
        
        print(f"\nValidation Results:")
        if stats.get('memory_strategy') == 'streaming-all-records':
            print(f"  ✅ STREAMING MODE: ALL RECORDS PROCESSED")
            print(f"  Unique Mismatched Rows: {summary['mismatched_rows_count']:,}")
            print(f"  Total Mismatch Data Points: {stats['mismatched_data_points']:,}")
        else:
            print(f"  Data Mismatches: {summary['mismatched_rows_count']:,} (sampled)")
        
        print(f"  Rows only in {table1_alias}: {summary[f'{table1_alias}_only_rows_count']:,}")
        print(f"  Rows only in {table2_alias}: {summary[f'{table2_alias}_only_rows_count']:,}")
        print(f"  Total Issues Found: {summary['total_issues_found']:,}")
        
        print(f"\nMemory Optimization Settings:")
        print(f"  Max Sample Size: {stats['max_sample_size']}")
        print(f"  Memory Strategy: {stats.get('memory_strategy', 'default')}")
        print(f"  User Specified Columns: {stats.get('user_specified_columns', False)}")
        print(f"  Comparison Columns: {stats['total_comparison_columns']} columns")
        print(f"  Selected Columns: {', '.join(stats['comparison_columns'][:10])}{'...' if len(stats['comparison_columns']) > 10 else ''}")
        
        if results.get('validation_issues'):
            print(f"\nValidation Issues:")
            for issue in results['validation_issues']:
                print(f"  - {issue['type']}: {issue.get('missing_columns', 'N/A')} ({issue['severity']})")
        
        if results.get('created_tables'):
            print(f"\nCreated Snowflake Tables:")
            for table_type, table_name in results['created_tables'].items():
                print(f"  {table_type}: {table_name}")
        
        print(f"\nSnowflake Stage Files:")
        for file_type, filepath in results['stage_files'].items():
            print(f"  {file_type}: {filepath}")
        
        print(f"\nComparison Details:")
        print(f"  Primary Key Columns: {', '.join(stats['primary_key_columns'])}")
        print(f"  Validation Date: {stats['validation_date']}")
        
        # Add note about sampling
        if 'note' in stats:
            print(f"\n Note: {stats['note']}")
        
        # Show verification option with enhanced guidance
        print(f"\n" + "="*80)
        print("VERIFICATION & CUSTOMIZATION OPTIONS")
        print("="*80)
        print("To verify results or customize comparison:")
        print("1. Verify: validator.print_sample_mismatches(table1, table2, pk_cols, comp_cols, alias1, alias2, 10)")
        
        if stats.get('memory_strategy') == 'streaming-all-records':
            print("2. ✅ STREAMING MODE - ALL records processed with no memory limits")
        elif stats.get('user_specified_columns'):
            print("2. ✅ You used specific columns - memory usage is optimized")
        else:
            print("2. Specify columns: Use comparison_columns=['COL1', 'COL2'] for better memory control")
            print("3. Adjust auto-limit: Use max_auto_columns=100 to process more columns automatically")
        
        print(f"4. Memory control: Current max_sample_size={stats['max_sample_size']} ({'unlimited when columns specified' if stats.get('user_specified_columns') else 'adjust as needed'})")
        print("5. For ALL records: Set max_sample_size=False to enable streaming mode")


def validate_tables_sp(session: Session, 
                      table1_name: str, 
                      table2_name: str,
                      primary_key_columns: str = None,
                      comparison_columns: str = None,
                      table1_alias: str = "SQL",
                      table2_alias: str = "MDP", 
                      environment: str = "SNOWFLAKE",
                      create_tables: bool = True,
                      stage_name: str = "@~/",
                      max_sample_size = None,
    """
    Stored procedure version with smart sampling logic and target schema support
    
    Args:
        session: Snowpark session
        table1_name: Fully qualified name of first table
        table2_name: Fully qualified name of second table
        primary_key_columns: Comma-separated list of primary key column names
        comparison_columns: Comma-separated list of columns to compare (when provided, defaults max_sample_size to False)
        table1_alias: Alias for first table (default: SQL for dev)
        table2_alias: Alias for second table (default: MDP for prod)
        environment: Database environment
        create_tables: Whether to create result tables
        stage_name: Snowflake stage for output files
        target_schema: Schema where result tables should be created (e.g., 'PUBLIC', 'DEV_SCHEMA')
        max_sample_size: None=auto-decide, int=specific limit, False=unlimited (user-controllable)
        max_auto_columns: None=default(50), int=user-specified limit for auto-detection
    
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
        
        # Smart defaults based on user input
        if max_sample_size is None:
            # Auto-decide based on whether user specified columns
            if comp_columns:
                final_max_sample_size = False  # Unlimited when user specifies columns
            else:
                final_max_sample_size = 1000   # Conservative default for auto-detection
        else:
            # User explicitly set it
            final_max_sample_size = max_sample_size
        
        if max_auto_columns is None:
            final_max_auto_columns = 50  # Default
        else:
            final_max_auto_columns = max_auto_columns
        
        # Create validator
        validator = GenericTableValidator(session)
        
        # Run memory-optimized validation with target schema
        results = validator.validate_tables(
            table1_name=table1_name,
            table2_name=table2_name,
            primary_key_columns=pk_columns,
            comparison_columns=comp_columns,
            table1_alias=table1_alias,
            table2_alias=table2_alias,
            environment=environment,
            create_tables=create_tables,
            stage_name=stage_name,
            target_schema=target_schema,
            max_sample_size=final_max_sample_size,
            max_auto_columns=final_max_auto_columns
        )
        
        if results and results.get('validation_status') != 'ERROR':
            summary = results['results_summary']
            stats = results['summary_statistics']
            table1_alias = stats['table1_alias']
            table2_alias = stats['table2_alias']
            
            # Format the response based on validation status
            status_line = f"Table Validation {results['validation_status']}"
            if results['validation_status'] == 'FAILED':
                if 'failure_details' in results:
                    status_line += f" - {results['failure_details']}"
            elif results['validation_status'] == 'PASSED':
                if 'success_details' in results:
                    status_line += f" - {results['success_details']}"
            
            # Include any errors that occurred but didn't fail the validation
            error_notes = []
            if 'table_creation_errors' in results:
                error_notes.append(f"Table creation issues: {len(results['table_creation_errors'])}")
            if 'stage_creation_errors' in results:
                error_notes.append(f"Stage file issues: {len(results['stage_creation_errors'])}")
            
            error_summary = f"\nNotes: {', '.join(error_notes)}" if error_notes else ""
            
            return f"""
{status_line}
Environment: {results['environment']}
{table1_alias} ({stats['table1_name']}): {stats['total_table1_rows']:,} rows
{table2_alias} ({stats['table2_name']}): {stats['total_table2_rows']:,} rows
Mismatched Rows: {summary['mismatched_rows_count']:,}
{table1_alias} Only: {summary[f'{table1_alias}_only_rows_count']:,}
{table2_alias} Only: {summary[f'{table2_alias}_only_rows_count']:,}
Total Issues: {summary['total_issues_found']:,}
Primary Keys: {', '.join(stats['primary_key_columns'])}
Comparison Columns: {stats['total_comparison_columns']}
Memory Strategy: {stats.get('memory_strategy', 'default')}
Sample Size: {stats['max_sample_size']}
Created Tables: {len(results.get('created_tables', {}))}
Stage Files: {len(results.get('stage_files', {}))}{error_summary}
                    """.strip()
        else:
            error_msg = results.get('error_details', 'Unknown error') if results else 'Validation returned no results'
            return f"Table validation FAILED - {error_msg}"
            
    except Exception as e:
        return f"Error during table validation: {str(e)}"


def main():
    """
    Main function for testing the memory-optimized table validator
    """
    try:
        validator = GenericTableValidator()
        
        # Sample mismatches with enhanced column control
        print(" STEP 1: Verifying script functionality with sample mismatches...")
        sample_mismatches = validator.print_sample_mismatches(
            table1_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
            table2_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD',
            primary_key_columns=['MEMBER_ID'],
            comparison_columns=None,  # Auto-detect or specify ['COL1', 'COL2'] for specific columns
            table1_alias='SQL',
            table2_alias='MDP',
            sample_size=15,
            max_auto_columns=50  # User can control auto-detection limit
        )
        
        # If found sample mismatches, proceed with full validation
        if sample_mismatches:
            print(f"\n STEP 2: Running memory-optimized validation (found {len(sample_mismatches)} sample mismatches)...")
            
            results = validator.validate_tables(
                table1_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
                table2_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD',
                primary_key_columns=['MEMBER_ID'],
                comparison_columns=None,  # Specify columns like ['COLUMN1', 'COLUMN2'] or None for auto-detect
                table1_alias='SQL',
                table2_alias='MDP',
                environment='SNOWFLAKE',
                create_tables=True,      # Set to True to create tables with ALL results
                stage_name='@~/',
                target_schema='PUBLIC',  # Use PUBLIC schema or specify your writable schema
                max_sample_size=False,   # False=ALL RECORDS (streaming), None=auto-decide, int=specific limit
                max_auto_columns=77      # Control auto-detection limit
            )
            
            if results:
                validator.print_validation_report(results)
                return results
            else:
                print(" Full validation failed to complete")
                return None
        else:
            print(" No mismatches found in sample - tables appear to match perfectly!")
            print("You can still run the full validation if needed.")
            return {'status': 'no_mismatches_in_sample'}
            
    except Exception as e:
        print(f" Error during validation: {str(e)}")
        logging.error(f"Validation error: {str(e)}")
        return None


if __name__ == "__main__":
    main()
