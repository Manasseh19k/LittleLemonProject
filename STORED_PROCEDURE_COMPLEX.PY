-- =====================================================================================
-- COMPREHENSIVE MDP_SQL_COMPARE_PY STORED PROCEDURE
-- Replicates most functionality of the original script within Snowflake constraints
-- =====================================================================================

CREATE OR REPLACE PROCEDURE MDP_SQL_COMPARE_PY_COMPREHENSIVE(
    DATABASE VARCHAR,
    TABLE1_NAME VARCHAR,
    TABLE2_NAME VARCHAR,
    PRIMARY_KEY_COLUMNS VARCHAR DEFAULT NULL,
    COMPARISON_COLUMNS VARCHAR DEFAULT NULL,
    TABLE1_ALIAS VARCHAR DEFAULT 'SQL',
    TABLE2_ALIAS VARCHAR DEFAULT 'MDP',
    ENVIRONMENT VARCHAR DEFAULT 'SNOWFLAKE',
    CREATE_TABLES BOOLEAN DEFAULT TRUE,
    CREATE_SAMPLE_TABLES BOOLEAN DEFAULT TRUE,
    TARGET_SCHEMA VARCHAR DEFAULT NULL,
    MAX_SAMPLE_SIZE INTEGER DEFAULT 1000,
    MAX_AUTO_COLUMNS INTEGER DEFAULT 50,
    DETAILED_OUTPUT BOOLEAN DEFAULT TRUE
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
from datetime import datetime
import logging
from typing import Dict, List, Tuple, Any, Optional
import json

class ComprehensiveTableValidator:
    def __init__(self, session: Session = None):
        self.session = session
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.logger = logging.getLogger(__name__)
        
        if self.session is None:
            from snowflake.snowpark.context import get_active_session
            self.session = get_active_session()
    
    def _build_full_table_name(self, database: str, table_name: str) -> str:
        if not database:
            return table_name
        if table_name.upper().startswith(database.upper() + "."):
            return table_name
        if "." in table_name:
            return f"{database}.{table_name}"
        else:
            return f"{database}.PUBLIC.{table_name}"
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
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
            return None
    
    def validate_table_compatibility(self, table1_info: Dict, table2_info: Dict) -> List[Dict]:
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
                    'missing_columns': list(missing_in_table2),
                    'severity': 'WARNING'
                })
            if missing_in_table1:
                issues.append({
                    'type': 'columns_missing_in_table1',
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
                return potential_keys[:1]
            else:
                return list(common_cols)[:1] if common_cols else []
    
    def identify_comparison_columns(self, table1_info: Dict, table2_info: Dict, 
                                  primary_key_columns: List[str],
                                  comparison_columns: Optional[List[str]] = None,
                                  max_auto_columns: int = 50) -> List[str]:
        table1_cols = [col.upper() for col in table1_info['column_names']]
        table2_cols = [col.upper() for col in table2_info['column_names']]
        common_cols = set(table1_cols) & set(table2_cols)
        
        pk_cols_upper = [col.upper() for col in primary_key_columns]
        available_comparison_cols = [col for col in common_cols if col not in pk_cols_upper]
        
        if comparison_columns:
            valid_comparison_cols = []
            for col in comparison_columns:
                col_upper = col.upper()
                if col_upper in available_comparison_cols:
                    valid_comparison_cols.append(col_upper)
            return valid_comparison_cols if valid_comparison_cols else list(available_comparison_cols)[:max_auto_columns]
        
        if len(available_comparison_cols) > max_auto_columns:
            return list(available_comparison_cols)[:max_auto_columns]
        else:
            return list(available_comparison_cols)
    
    def create_comprehensive_comparison_tables(self, table1_name: str, table2_name: str,
                                             primary_key_columns: List[str], comparison_columns: List[str],
                                             table1_alias: str, table2_alias: str, target_schema: str,
                                             max_sample_size: int, create_sample_tables: bool) -> Dict:
        
        results = {
            'tables_created': [],
            'sample_tables_created': [],
            'counts': {},
            'errors': []
        }
        
        try:
            # Build primary key join condition
            pk_join_conditions = []
            for col in primary_key_columns:
                pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
            pk_join_condition = " AND ".join(pk_join_conditions)
            
            # Count all differences
            table1_only_sql = f"""
            SELECT COUNT(*) as count_only
            FROM {table1_name} t1
            LEFT JOIN {table2_name} t2 ON {pk_join_condition}
            WHERE t2.{primary_key_columns[0]} IS NULL
            """
            table1_only_count = self.session.sql(table1_only_sql).collect()[0]['COUNT_ONLY']
            results['counts']['table1_only'] = table1_only_count
            
            table2_only_sql = f"""
            SELECT COUNT(*) as count_only
            FROM {table2_name} t2
            LEFT JOIN {table1_name} t1 ON {pk_join_condition}
            WHERE t1.{primary_key_columns[0]} IS NULL
            """
            table2_only_count = self.session.sql(table2_only_sql).collect()[0]['COUNT_ONLY']
            results['counts']['table2_only'] = table2_only_count
            
            # Count mismatches
            comparison_conditions = []
            for col in comparison_columns:
                comparison_conditions.append(f"(COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))")
            
            mismatch_count = 0
            if comparison_conditions:
                mismatch_count_sql = f"""
                SELECT COUNT(*) as count_mismatches
                FROM {table1_name} t1
                INNER JOIN {table2_name} t2 ON {pk_join_condition}
                WHERE {' OR '.join(comparison_conditions)}
                """
                mismatch_count = self.session.sql(mismatch_count_sql).collect()[0]['COUNT_MISMATCHES']
            results['counts']['mismatches'] = mismatch_count
            
            # Create comprehensive result tables
            if table1_only_count > 0:
                try:
                    table1_only_table = f"{target_schema}.TABLE_VALIDATION_{table1_alias}_ONLY_{self.timestamp}"
                    self.session.sql(f"""
                        CREATE OR REPLACE TABLE {table1_only_table} AS
                        SELECT t1.*, 'ONLY_IN_{table1_alias}' as DIFFERENCE_TYPE
                        FROM {table1_name} t1
                        LEFT JOIN {table2_name} t2 ON {pk_join_condition}
                        WHERE t2.{primary_key_columns[0]} IS NULL
                    """).collect()
                    results['tables_created'].append(table1_only_table)
                except Exception as e:
                    results['errors'].append(f"Failed to create {table1_alias}_only table: {str(e)}")
            
            if table2_only_count > 0:
                try:
                    table2_only_table = f"{target_schema}.TABLE_VALIDATION_{table2_alias}_ONLY_{self.timestamp}"
                    self.session.sql(f"""
                        CREATE OR REPLACE TABLE {table2_only_table} AS
                        SELECT t2.*, 'ONLY_IN_{table2_alias}' as DIFFERENCE_TYPE
                        FROM {table2_name} t2
                        LEFT JOIN {table1_name} t1 ON {pk_join_condition}
                        WHERE t1.{primary_key_columns[0]} IS NULL
                    """).collect()
                    results['tables_created'].append(table2_only_table)
                except Exception as e:
                    results['errors'].append(f"Failed to create {table2_alias}_only table: {str(e)}")
            
            # Create comprehensive mismatches table
            if mismatch_count > 0:
                try:
                    mismatches_table = f"{target_schema}.TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
                    pk_select = ", ".join([f"t1.{col}" for col in primary_key_columns])
                    
                    mismatch_cases = []
                    for col in comparison_columns:
                        mismatch_cases.append(f"""
                        SELECT {pk_select}, 
                               '{col}' as COLUMN_NAME,
                               COALESCE(CAST(t1.{col} AS STRING), 'NULL') as {table1_alias}_VALUE,
                               COALESCE(CAST(t2.{col} AS STRING), 'NULL') as {table2_alias}_VALUE,
                               '{table1_alias}' as TABLE1_ALIAS,
                               '{table2_alias}' as TABLE2_ALIAS,
                               CURRENT_TIMESTAMP as VALIDATION_TIMESTAMP
                        FROM {table1_name} t1
                        INNER JOIN {table2_name} t2 ON {pk_join_condition}
                        WHERE COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL')
                        """)
                    
                    if mismatch_cases:
                        self.session.sql(f"""
                            CREATE OR REPLACE TABLE {mismatches_table} AS
                            {' UNION ALL '.join(mismatch_cases)}
                        """).collect()
                        results['tables_created'].append(mismatches_table)
                except Exception as e:
                    results['errors'].append(f"Failed to create mismatches table: {str(e)}")
            
            # Create sample tables for verification (equivalent to print_sample_mismatches)
            if create_sample_tables and mismatch_count > 0:
                try:
                    sample_table = f"{target_schema}.TABLE_VALIDATION_SAMPLES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
                    
                    # Create sample with side-by-side comparison
                    pk_cols_str = ", ".join([f"t1.{col}" for col in primary_key_columns])
                    comp_cols_t1 = ", ".join([f"t1.{col} as {table1_alias}_{col}" for col in comparison_columns[:10]])  # Limit for readability
                    comp_cols_t2 = ", ".join([f"t2.{col} as {table2_alias}_{col}" for col in comparison_columns[:10]])
                    
                    comparison_where = " OR ".join([f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL')" for col in comparison_columns[:10]])
                    
                    self.session.sql(f"""
                        CREATE OR REPLACE TABLE {sample_table} AS
                        SELECT {pk_cols_str}, {comp_cols_t1}, {comp_cols_t2},
                               CURRENT_TIMESTAMP as SAMPLE_TIMESTAMP
                        FROM {table1_name} t1
                        INNER JOIN {table2_name} t2 ON {pk_join_condition}
                        WHERE {comparison_where}
                        LIMIT {max_sample_size}
                    """).collect()
                    results['sample_tables_created'].append(sample_table)
                except Exception as e:
                    results['errors'].append(f"Failed to create sample table: {str(e)}")
            
            # Create summary table (equivalent to validation report)
            try:
                summary_table = f"{target_schema}.TABLE_VALIDATION_SUMMARY_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
                
                table1_rows = self.session.sql(f"SELECT COUNT(*) as cnt FROM {table1_name}").collect()[0]['CNT']
                table2_rows = self.session.sql(f"SELECT COUNT(*) as cnt FROM {table2_name}").collect()[0]['CNT']
                
                summary_data = [{
                    'VALIDATION_TIMESTAMP': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    'TABLE1_NAME': table1_name,
                    'TABLE2_NAME': table2_name,
                    'TABLE1_ALIAS': table1_alias,
                    'TABLE2_ALIAS': table2_alias,
                    'TABLE1_ROWS': table1_rows,
                    'TABLE2_ROWS': table2_rows,
                    'ROWS_ONLY_IN_TABLE1': table1_only_count,
                    'ROWS_ONLY_IN_TABLE2': table2_only_count,
                    'MISMATCHED_ROWS': mismatch_count,
                    'TOTAL_ISSUES': table1_only_count + table2_only_count + mismatch_count,
                    'PRIMARY_KEY_COLUMNS': ', '.join(primary_key_columns),
                    'COMPARISON_COLUMNS_COUNT': len(comparison_columns),
                    'COMPARISON_COLUMNS': ', '.join(comparison_columns),
                    'VALIDATION_STATUS': 'PASSED' if (table1_only_count + table2_only_count + mismatch_count) == 0 else 'FAILED',
                    'ENVIRONMENT': 'SNOWFLAKE'
                }]
                
                summary_df = pd.DataFrame(summary_data)
                snowpark_df = self.session.create_dataframe(summary_df)
                snowpark_df.write.mode("overwrite").save_as_table(summary_table)
                results['tables_created'].append(summary_table)
                
            except Exception as e:
                results['errors'].append(f"Failed to create summary table: {str(e)}")
            
            return results
            
        except Exception as e:
            results['errors'].append(f"General comparison error: {str(e)}")
            return results
    
    def validate_tables_comprehensive(self, database: str, table1_name: str, table2_name: str,
                                    primary_key_columns: Optional[List[str]] = None,
                                    comparison_columns: Optional[List[str]] = None,
                                    table1_alias: str = "SQL", table2_alias: str = "MDP",
                                    environment: str = "SNOWFLAKE",
                                    create_tables: bool = True,
                                    create_sample_tables: bool = True,
                                    target_schema: str = None,
                                    max_sample_size: int = 1000,
                                    max_auto_columns: int = 50,
                                    detailed_output: bool = True) -> str:
        
        # Build full table names
        full_table1_name = self._build_full_table_name(database, table1_name)
        full_table2_name = self._build_full_table_name(database, table2_name)
        
        # Determine target schema
        if target_schema is None:
            table_parts = full_table1_name.split('.')
            if len(table_parts) >= 2:
                target_schema = '.'.join(table_parts[:-1])
            else:
                target_schema = f"{database}.PUBLIC" if database else "PUBLIC"
        
        try:
            # Get table information
            table1_info = self.get_table_info(full_table1_name)
            table2_info = self.get_table_info(full_table2_name)
            
            if not table1_info or not table2_info:
                return "ERROR: Failed to get table information - check table names and permissions"
            
            # Validate compatibility
            validation_issues = self.validate_table_compatibility(table1_info, table2_info)
            critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
            if critical_issues:
                return f"ERROR: Critical validation issues - {[issue['type'] for issue in critical_issues]}"
            
            # Identify columns
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                return "ERROR: No valid primary key columns identified"
            
            comp_columns = self.identify_comparison_columns(table1_info, table2_info, pk_columns, comparison_columns, max_auto_columns)
            if not comp_columns:
                return "ERROR: No valid comparison columns identified"
            
            # Perform comprehensive comparison
            if create_tables:
                results = self.create_comprehensive_comparison_tables(
                    full_table1_name, full_table2_name, pk_columns, comp_columns,
                    table1_alias, table2_alias, target_schema, max_sample_size, create_sample_tables
                )
            else:
                # Just count without creating tables
                results = {'counts': {}, 'tables_created': [], 'sample_tables_created': [], 'errors': []}
                # Add counting logic here if needed
            
            # Format comprehensive output
            status = "PASSED" if results['counts'].get('mismatches', 0) + results['counts'].get('table1_only', 0) + results['counts'].get('table2_only', 0) == 0 else "FAILED"
            
            output_lines = [
                f"=== COMPREHENSIVE TABLE VALIDATION REPORT ===",
                f"Validation Status: {status}",
                f"Database: {database}",
                f"Environment: {environment}",
                f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                f"",
                f"Table Information:",
                f"  {table1_alias} ({full_table1_name}): {table1_info['row_count']:,} rows",
                f"  {table2_alias} ({full_table2_name}): {table2_info['row_count']:,} rows",
                f"",
                f"Validation Results:",
                f"  Mismatched Rows: {results['counts'].get('mismatches', 0):,}",
                f"  {table1_alias} Only Rows: {results['counts'].get('table1_only', 0):,}",
                f"  {table2_alias} Only Rows: {results['counts'].get('table2_only', 0):,}",
                f"  Total Issues: {results['counts'].get('mismatches', 0) + results['counts'].get('table1_only', 0) + results['counts'].get('table2_only', 0):,}",
                f"",
                f"Configuration:",
                f"  Primary Keys: {', '.join(pk_columns)}",
                f"  Comparison Columns: {len(comp_columns)} columns",
                f"  Max Sample Size: {max_sample_size}",
                f"  Target Schema: {target_schema}",
            ]
            
            if detailed_output:
                output_lines.extend([
                    f"",
                    f"Detailed Column Information:",
                    f"  Selected Columns: {', '.join(comp_columns[:10])}{'...' if len(comp_columns) > 10 else ''}",
                ])
                
                if validation_issues:
                    output_lines.extend([
                        f"",
                        f"Validation Issues:",
                    ])
                    for issue in validation_issues:
                        if issue.get('missing_columns'):
                            output_lines.append(f"  - {issue['type']}: {', '.join(issue['missing_columns'])} ({issue['severity']})")
            
            if create_tables and results['tables_created']:
                output_lines.extend([
                    f"",
                    f"Created Tables ({len(results['tables_created'])}):",
                ])
                for table in results['tables_created']:
                    output_lines.append(f"  - {table}")
                
                if results['sample_tables_created']:
                    output_lines.extend([
                        f"",
                        f"Sample Tables for Verification ({len(results['sample_tables_created'])}):",
                    ])
                    for table in results['sample_tables_created']:
                        output_lines.append(f"  - {table}")
            
            if results['errors']:
                output_lines.extend([
                    f"",
                    f"Errors Encountered ({len(results['errors'])}):",
                ])
                for error in results['errors']:
                    output_lines.append(f"  - {error}")
            
            output_lines.extend([
                f"",
                f"=== END VALIDATION REPORT ===",
            ])
            
            return "\n".join(output_lines)
            
        except Exception as e:
            return f"ERROR: Comprehensive validation failed - {str(e)}"


def main(session, database, table1_name, table2_name, primary_key_columns=None, 
         comparison_columns=None, table1_alias="SQL", table2_alias="MDP", 
         environment="SNOWFLAKE", create_tables=True, create_sample_tables=True,
         target_schema=None, max_sample_size=1000, max_auto_columns=50, detailed_output=True):
    
    try:
        # Parse parameters
        pk_columns = None
        if primary_key_columns:
            pk_columns = [col.strip().upper() for col in primary_key_columns.split(',')]
        
        comp_columns = None
        if comparison_columns:
            comp_columns = [col.strip().upper() for col in comparison_columns.split(',')]
        
        # Create comprehensive validator
        validator = ComprehensiveTableValidator(session)
        
        # Run comprehensive validation
        result = validator.validate_tables_comprehensive(
            database=database,
            table1_name=table1_name,
            table2_name=table2_name,
            primary_key_columns=pk_columns,
            comparison_columns=comp_columns,
            table1_alias=table1_alias,
            table2_alias=table2_alias,
            environment=environment,
            create_tables=create_tables,
            create_sample_tables=create_sample_tables,
            target_schema=target_schema,
            max_sample_size=max_sample_size,
            max_auto_columns=max_auto_columns,
            detailed_output=detailed_output
        )
        
        return result
        
    except Exception as e:
        return f"ERROR: Stored procedure execution failed - {str(e)}"
$$;
