CREATE OR REPLACE PROCEDURE MDP_SQL_COMPARE_PY(
    DATABASE VARCHAR,
    TABLE1_NAME VARCHAR,
    TABLE2_NAME VARCHAR,
    PRIMARY_KEY_COLUMNS VARCHAR DEFAULT NULL,
    COMPARISON_COLUMNS VARCHAR DEFAULT NULL,
    TABLE1_ALIAS VARCHAR DEFAULT 'SQL',
    TABLE2_ALIAS VARCHAR DEFAULT 'MDP',
    ENVIRONMENT VARCHAR DEFAULT 'SNOWFLAKE',
    CREATE_TABLES BOOLEAN DEFAULT TRUE,
    STAGE_NAME VARCHAR DEFAULT '@~/',
    TARGET_SCHEMA VARCHAR DEFAULT NULL,
    MAX_SAMPLE_SIZE VARIANT DEFAULT NULL,
    MAX_AUTO_COLUMNS INTEGER DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'pandas')
HANDLER = 'main'
AS
$$
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
    
    def _build_full_table_name(self, database: str, table_name: str) -> str:
        """
        Build full table name by concatenating database and table name
        
        Args:
            database: Database name to prepend
            table_name: Table name (can be schema.table or just table)
            
        Returns:
            Full table name in format: database.schema.table
        """
        if not database:
            return table_name
        
        # If table_name already starts with the database, return as-is
        if table_name.upper().startswith(database.upper() + "."):
            return table_name
            
        # If table_name has schema.table format, prepend database
        if "." in table_name:
            return f"{database}.{table_name}"
        else:
            # If just table name, assume PUBLIC schema
            return f"{database}.PUBLIC.{table_name}"
    
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
    
    def validate_tables(self, database: str, table1_name: str, table2_name: str,
                       primary_key_columns: Optional[List[str]] = None,
                       comparison_columns: Optional[List[str]] = None,
                       table1_alias: str = "SQL", table2_alias: str = "MDP",
                       environment: str = "SNOWFLAKE",
                       create_tables: bool = True,
                       stage_name: str = "@~/",
                       target_schema: str = None,
                       max_sample_size = None,
                       max_auto_columns: int = None) -> str:
        """
        Simplified validation that returns a summary string for stored procedure
        """
        try:
            # Build full table names with database
            full_table1_name = self._build_full_table_name(database, table1_name)
            full_table2_name = self._build_full_table_name(database, table2_name)
            
            # Smart defaults based on user input
            if max_sample_size is None:
                if comparison_columns:
                    final_max_sample_size = False  # Unlimited when user specifies columns
                else:
                    final_max_sample_size = 1000   # Conservative default for auto-detection
            else:
                final_max_sample_size = max_sample_size
            
            if max_auto_columns is None:
                final_max_auto_columns = 50
            else:
                final_max_auto_columns = max_auto_columns
            
            # Determine target schema for result tables
            if target_schema is None:
                table_parts = full_table1_name.split('.')
                if len(table_parts) >= 2:
                    target_schema = '.'.join(table_parts[:-1])
                else:
                    target_schema = f"{database}.PUBLIC" if database else "PUBLIC"
            
            # Get table information using full table names
            table1_info = self.get_table_info(full_table1_name)
            table2_info = self.get_table_info(full_table2_name)
            if not table1_info or not table2_info:
                return f"ERROR: Failed to get table information - check table names and permissions"
            
            # Validate table compatibility
            validation_issues = self.validate_table_compatibility(table1_info, table2_info)
            critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
            if critical_issues:
                return f"ERROR: Critical validation issues found - {[issue['type'] for issue in critical_issues]}"
            
            # Identify primary key columns
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                return f"ERROR: No valid primary key columns identified"
            
            # Identify comparison columns
            comp_columns = self.identify_comparison_columns(table1_info, table2_info, pk_columns, comparison_columns, final_max_auto_columns)
            if not comp_columns:
                return f"ERROR: No valid comparison columns identified"
            
            # Simplified comparison for stored procedure - count only approach
            return self._perform_comparison_counts(
                full_table1_name, full_table2_name, pk_columns, comp_columns, 
                table1_alias, table2_alias, environment, target_schema,
                create_tables, final_max_sample_size
            )
            
        except Exception as e:
            return f"ERROR: Unexpected error during validation: {str(e)}"
    
    def _perform_comparison_counts(self, table1_name: str, table2_name: str,
                                 primary_key_columns: List[str], comparison_columns: List[str],
                                 table1_alias: str, table2_alias: str, environment: str, 
                                 target_schema: str, create_tables: bool, max_sample_size) -> str:
        """
        Perform simplified comparison that focuses on counts and creates summary tables
        """
        try:
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # Count rows only in table1
            table1_only_sql = f"""
            SELECT COUNT(*) as count_only
            FROM {table1_name} t1
            LEFT JOIN {table2_name} t2 ON {pk_join_condition}
            WHERE t2.{primary_key_columns[0]} IS NULL
            """
            table1_only_count = self.session.sql(table1_only_sql).collect()[0]['COUNT_ONLY']
            
            # Count rows only in table2
            table2_only_sql = f"""
            SELECT COUNT(*) as count_only
            FROM {table2_name} t2
            LEFT JOIN {table1_name} t1 ON {pk_join_condition}
            WHERE t1.{primary_key_columns[0]} IS NULL
            """
            table2_only_count = self.session.sql(table2_only_sql).collect()[0]['COUNT_ONLY']
            
            # Count mismatched rows
            comparison_conditions = []
            for col in comparison_columns:
                comparison_conditions.append(f"""
                    (COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))
                """)
            
            mismatch_count = 0
            if comparison_conditions:
                mismatch_count_sql = f"""
                SELECT COUNT(*) as count_mismatches
                FROM {table1_name} t1
                INNER JOIN {table2_name} t2 ON {pk_join_condition}
                WHERE {' OR '.join(comparison_conditions)}
                """
                mismatch_count = self.session.sql(mismatch_count_sql).collect()[0]['COUNT_MISMATCHES']
            
            # Get row counts
            table1_rows = self.session.sql(f"SELECT COUNT(*) as cnt FROM {table1_name}").collect()[0]['CNT']
            table2_rows = self.session.sql(f"SELECT COUNT(*) as cnt FROM {table2_name}").collect()[0]['CNT']
            
            # Create result tables if requested
            tables_created = 0
            table_creation_errors = []
            
            if create_tables and (table1_only_count > 0 or table2_only_count > 0 or mismatch_count > 0):
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                # Create table1-only table
                if table1_only_count > 0:
                    try:
                        table1_only_table = f"{target_schema}.TABLE_VALIDATION_{table1_alias}_ONLY_{timestamp}"
                        self.session.sql(f"""
                            CREATE OR REPLACE TABLE {table1_only_table} AS
                            SELECT t1.*
                            FROM {table1_name} t1
                            LEFT JOIN {table2_name} t2 ON {pk_join_condition}
                            WHERE t2.{primary_key_columns[0]} IS NULL
                        """).collect()
                        tables_created += 1
                    except Exception as e:
                        table_creation_errors.append(f"{table1_alias}_only: {str(e)}")
                
                # Create table2-only table
                if table2_only_count > 0:
                    try:
                        table2_only_table = f"{target_schema}.TABLE_VALIDATION_{table2_alias}_ONLY_{timestamp}"
                        self.session.sql(f"""
                            CREATE OR REPLACE TABLE {table2_only_table} AS
                            SELECT t2.*
                            FROM {table2_name} t2
                            LEFT JOIN {table1_name} t1 ON {pk_join_condition}
                            WHERE t1.{primary_key_columns[0]} IS NULL
                        """).collect()
                        tables_created += 1
                    except Exception as e:
                        table_creation_errors.append(f"{table2_alias}_only: {str(e)}")
                
                # Create mismatches table
                if mismatch_count > 0:
                    try:
                        mismatches_table = f"{target_schema}.TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{timestamp}"
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
                            self.session.sql(f"""
                                CREATE OR REPLACE TABLE {mismatches_table} AS
                                {' UNION ALL '.join(mismatch_cases)}
                            """).collect()
                            tables_created += 1
                    except Exception as e:
                        table_creation_errors.append(f"mismatches: {str(e)}")
            
            # Determine status
            total_issues = table1_only_count + table2_only_count + mismatch_count
            status = "PASSED" if total_issues == 0 else "FAILED"
            
            # Format response
            error_notes = f"\nTable Creation Errors: {', '.join(table_creation_errors)}" if table_creation_errors else ""
            
            return f"""Table Validation {status}
Database: {table1_name.split('.')[0] if '.' in table1_name else 'N/A'}
Environment: {environment}
{table1_alias} ({table1_name}): {table1_rows:,} rows
{table2_alias} ({table2_name}): {table2_rows:,} rows
Mismatched Rows: {mismatch_count:,}
{table1_alias} Only: {table1_only_count:,}
{table2_alias} Only: {table2_only_count:,}
Total Issues: {total_issues:,}
Primary Keys: {', '.join(primary_key_columns)}
Comparison Columns: {len(comparison_columns)}
Tables Created: {tables_created}
Target Schema: {target_schema}{error_notes}""".strip()
            
        except Exception as e:
            return f"ERROR: Comparison failed - {str(e)}"


def main(session, database, table1_name, table2_name, primary_key_columns=None, 
         comparison_columns=None, table1_alias="SQL", table2_alias="MDP", 
         environment="SNOWFLAKE", create_tables=True, stage_name="@~/", 
         target_schema=None, max_sample_size=None, max_auto_columns=None):
    """
    Main function for the stored procedure
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
        
        # Handle max_sample_size conversion from VARIANT
        final_max_sample_size = max_sample_size
        if max_sample_size is not None:
            # Convert VARIANT to appropriate type
            if isinstance(max_sample_size, str):
                if max_sample_size.lower() == 'false':
                    final_max_sample_size = False
                elif max_sample_size.lower() == 'null' or max_sample_size.lower() == 'none':
                    final_max_sample_size = None
                else:
                    try:
                        final_max_sample_size = int(max_sample_size)
                    except:
                        final_max_sample_size = None
        
        # Create validator
        validator = GenericTableValidator(session)
        
        # Run validation
        result = validator.validate_tables(
            database=database,
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
            max_auto_columns=max_auto_columns
        )
        
        return result
        
    except Exception as e:
        return f"ERROR: Stored procedure execution failed - {str(e)}"
$$;
