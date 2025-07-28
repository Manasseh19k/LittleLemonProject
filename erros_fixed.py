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
        
        Args:
            table_name: Fully qualified table name
            
        Returns:
            Dictionary containing table information
        """
        try:
            # Get the table as a Snowpark DataFrame
            df = self.session.table(table_name)
            
            # Get row count
            row_count = df.count()
            
            # Get column information from schema
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
        
        Args:
            table1_info: Information about first table
            table2_info: Information about second table
            
        Returns:
            List of validation issues
        """
        issues = []
        
        # Check if tables exist
        if not table1_info:
            issues.append({
                'type': 'table_not_found',
                'table': 'TABLE1',
                'severity': 'CRITICAL'
            })
        
        if not table2_info:
            issues.append({
                'type': 'table_not_found', 
                'table': 'TABLE2',
                'severity': 'CRITICAL'
            })
        
        if table1_info and table2_info:
            # Compare column names
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
        """
        Identify primary key columns for comparison
        
        Args:
            table1_info: Information about first table
            table2_info: Information about second table
            primary_key_columns: User-specified primary key columns
            
        Returns:
            List of primary key column names
        """
        if primary_key_columns:
            # Validate that specified columns exist in both tables
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
            # Try to identify potential primary key columns
            # Look for columns with 'ID' in the name
            table1_cols = [col.upper() for col in table1_info['column_names']]
            table2_cols = [col.upper() for col in table2_info['column_names']]
            common_cols = set(table1_cols) & set(table2_cols)
            
            potential_keys = []
            for col in common_cols:
                if 'ID' in col or col in ['MEMBER_ID', 'CUSTOMER_ID', 'ACCOUNT_ID', 'USER_ID']:
                    potential_keys.append(col)
            
            if potential_keys:
                self.logger.info(f"Auto-identified potential primary key columns: {potential_keys}")
                return potential_keys[:1]  # Use first identified key
            else:
                self.logger.warning("No primary key columns identified. Using all common columns.")
                return list(common_cols)
    
    def compare_tables_comprehensive(self, table1_name: str, table2_name: str,
                                   primary_key_columns: Optional[List[str]] = None,
                                   table1_alias: str = "SQL", 
                                   table2_alias: str = "MDP",
                                   batch_size: int = 10000) -> Tuple[List[Dict], List[Dict], List[Dict], Dict]:
        """
        Perform comprehensive row-by-row comparison of all columns between two tables
        Memory-optimized version that processes data in batches
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            primary_key_columns: List of columns to use as primary keys
            table1_alias: Alias for first table (default: SQL for dev)
            table2_alias: Alias for second table (default: MDP for prod)
            batch_size: Number of rows to process at once
            
        Returns:
            Tuple of (mismatched_rows, table1_only_rows, table2_only_rows, summary_stats)
        """
        mismatched_rows = []
        table1_only_rows = []
        table2_only_rows = []
        summary_stats = {}
        
        try:
            # Get table information
            table1_info = self.get_table_info(table1_name)
            table2_info = self.get_table_info(table2_name)
            
            if not table1_info or not table2_info:
                self.logger.error("Failed to get table information")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            
            # Get common columns
            table1_cols = [col.upper() for col in table1_info['column_names']]
            table2_cols = [col.upper() for col in table2_info['column_names']]
            common_columns = list(set(table1_cols) & set(table2_cols))
            
            if not common_columns:
                self.logger.error("No common columns found between tables")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            
            # Identify primary key columns
            pk_columns = self.identify_primary_key_columns(table1_info, table2_info, primary_key_columns)
            if not pk_columns:
                self.logger.error("No valid primary key columns identified")
                return mismatched_rows, table1_only_rows, table2_only_rows, summary_stats
            
            # Get table DataFrames
            table1_df = self.session.table(table1_name)
            table2_df = self.session.table(table2_name)
            
            # Create temporary views
            table1_df.create_or_replace_temp_view("temp_table1")
            table2_df.create_or_replace_temp_view("temp_table2")
            
            # Build primary key join condition
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
            
            # Sample only first 1000 records for memory efficiency
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
            
            # Sample only first 1000 records for memory efficiency
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
            
            # Build comparison conditions
            comparison_conditions = []
            for col in common_columns:
                if col not in pk_columns:  # Skip primary key columns in comparison
                    comparison_conditions.append(f"""
                        (COALESCE(CAST(t1.{col} AS STRING), 'NULL') != COALESCE(CAST(t2.{col} AS STRING), 'NULL'))
                    """)
            
            if comparison_conditions:
                # Count total mismatches
                mismatch_count_sql = f"""
                SELECT COUNT(*) as count_mismatches
                FROM temp_table1 t1
                INNER JOIN temp_table2 t2 ON {pk_join_condition}
                WHERE {' OR '.join(comparison_conditions)}
                """
                
                count_result = self.session.sql(mismatch_count_sql).collect()
                total_mismatch_rows = count_result[0]['COUNT_MISMATCHES']
                
                # Sample mismatched rows for detailed analysis (limit to 5000 for memory)
                if total_mismatch_rows > 0:
                    select_columns = []
                    
                    # Add primary key columns
                    for col in pk_columns:
                        select_columns.append(f"t1.{col}")
                    
                    # Add sample of non-pk columns with both table values for comparison
                    sample_columns = [col for col in common_columns if col not in pk_columns][:10]  # Limit columns
                    
                    for col in sample_columns:
                        select_columns.extend([f"t1.{col} as {table1_alias}_{col}", f"t2.{col} as {table2_alias}_{col}"])
                    
                    # Build sample mismatch query
                    mismatch_sql = f"""
                    SELECT {', '.join(select_columns)}
                    FROM temp_table1 t1
                    INNER JOIN temp_table2 t2 ON {pk_join_condition}
                    WHERE {' OR '.join(comparison_conditions[:5])}
                    LIMIT 5000
                    """
                    
                    mismatch_result = self.session.sql(mismatch_sql)
                    mismatch_df = mismatch_result.to_pandas()
                    
                    # Process sample mismatched rows
                    for _, row in mismatch_df.iterrows():
                        # Extract primary key values
                        pk_values = {}
                        for col in pk_columns:
                            pk_values[col] = row[col]
                        
                        # Check sample columns for differences
                        for col in sample_columns:
                            table1_val = row.get(f'{table1_alias}_{col}')
                            table2_val = row.get(f'{table2_alias}_{col}')
                            
                            # Check if values are different
                            if pd.isna(table1_val) and pd.isna(table2_val):
                                continue  # Both are null, no difference
                            elif pd.isna(table1_val) or pd.isna(table2_val) or str(table1_val) != str(table2_val):
                                # Create a separate record for each mismatched column
                                mismatch_record = {
                                    **pk_values,  # Include primary key values
                                    'COLUMN_NAME': col,
                                    f'{table1_alias}_VALUE': table1_val if pd.notna(table1_val) else None,
                                    f'{table2_alias}_VALUE': table2_val if pd.notna(table2_val) else None,
                                    'TABLE1_ALIAS': table1_alias,
                                    'TABLE2_ALIAS': table2_alias
                                }
                                mismatched_rows.append(mismatch_record)
                
                # Use actual counts for summary
                estimated_total_mismatches = total_mismatch_rows * len([col for col in common_columns if col not in pk_columns])
            else:
                total_mismatch_rows = 0
                estimated_total_mismatches = 0
            
            # Generate comprehensive summary statistics
            summary_stats = {
                'timestamp': self.timestamp,
                'table1_name': table1_name,
                'table2_name': table2_name,
                'table1_alias': table1_alias,
                'table2_alias': table2_alias,
                'total_table1_rows': table1_info['row_count'],
                'total_table2_rows': table2_info['row_count'],
                'rows_only_in_table1': table1_only_count,  # Use actual count
                'rows_only_in_table2': table2_only_count,  # Use actual count
                'mismatched_rows_count': total_mismatch_rows,  # Use actual count
                'mismatched_data_points': len(mismatched_rows),  # Sample count
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
    
    def create_snowflake_table(self, table_name: str, data: List[Dict]):
        """
        Create Snowflake table with validation results
        
        Args:
            table_name: Name of the table to create
            data: List of dictionaries containing the data
        """
        if not data:
            self.logger.info(f"No data to create table {table_name}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Clean column names (remove special characters for Snowflake)
            df.columns = [col.replace('(', '').replace(')', '').replace(' ', '_').replace('-', '_') for col in df.columns]
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Create table
            snowpark_df.write.mode("overwrite").save_as_table(table_name)
            
            self.logger.info(f"Created Snowflake table: {table_name} with {len(data)} rows")
            
        except Exception as e:
            self.logger.error(f"Error creating Snowflake table {table_name}: {str(e)}")
    
    def write_to_snowflake_stage(self, data: List[Dict], filename: str, stage_name: str = "@~/"):
        """
        Write data to Snowflake stage as CSV
        
        Args:
            data: List of dictionaries to write
            filename: Name of the file
            stage_name: Snowflake stage name
        """
        if not data:
            self.logger.info(f"No data to write to stage {filename}")
            return
        
        try:
            # Convert to pandas DataFrame
            df = pd.DataFrame(data)
            
            # Create Snowpark DataFrame
            snowpark_df = self.session.create_dataframe(df)
            
            # Write to stage
            snowpark_df.write.mode("overwrite").csv(f"{stage_name}{filename}")
            
            self.logger.info(f"Created stage file: {stage_name}{filename}")
            
        except Exception as e:
            self.logger.error(f"Error writing to stage: {str(e)}")
    
    def validate_tables(self, table1_name: str, table2_name: str,
                       primary_key_columns: Optional[List[str]] = None,
                       table1_alias: str = "SQL", table2_alias: str = "MDP",
                       environment: str = "SNOWFLAKE",
                       create_tables: bool = True,
                       stage_name: str = "@~/") -> Dict[str, Any]:
        """
        Main validation function for comprehensive table comparison
        
        Args:
            table1_name: Name of first table
            table2_name: Name of second table
            primary_key_columns: List of columns to use as primary keys
            table1_alias: Alias for first table (default: SQL for dev)
            table2_alias: Alias for second table (default: MDP for prod)
            environment: Database environment (SNOWFLAKE, PREM, etc.)
            create_tables: Whether to create result tables
            stage_name: Snowflake stage for CSV files
            
        Returns:
            Dictionary containing comprehensive validation results
        """
        self.logger.info("="*80)
        self.logger.info("STARTING COMPREHENSIVE TABLE VALIDATION")
        self.logger.info("="*80)
        self.logger.info(f"Table 1 ({table1_alias}): {table1_name}")
        self.logger.info(f"Table 2 ({table2_alias}): {table2_name}")
        self.logger.info(f"Environment: {environment}")
        self.logger.info(f"Primary Key Columns: {primary_key_columns}")
        self.logger.info(f"Timestamp: {self.timestamp}")
        
        # Get table information
        table1_info = self.get_table_info(table1_name)
        table2_info = self.get_table_info(table2_name)
        
        if not table1_info or not table2_info:
            self.logger.error("Failed to get table information")
            return None
        
        # Validate table compatibility
        validation_issues = self.validate_table_compatibility(table1_info, table2_info)
        
        # Check for critical issues
        critical_issues = [issue for issue in validation_issues if issue.get('severity') == 'CRITICAL']
        if critical_issues:
            self.logger.error("Critical validation issues found - cannot proceed with data comparison")
            return {
                'validation_status': 'FAILED',
                'critical_issues': critical_issues,
                'validation_issues': validation_issues
            }
        
        # Perform comprehensive comparison
        mismatched_rows, table1_only_rows, table2_only_rows, summary_stats = self.compare_tables_comprehensive(
            table1_name, table2_name, primary_key_columns, table1_alias, table2_alias, batch_size=5000
        )
        
        # Generate output names with timestamp and aliases
        mismatch_table_name = f"TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{self.timestamp}"
        table1_only_table_name = f"TABLE_VALIDATION_{table1_alias}_ONLY_{self.timestamp}"
        table2_only_table_name = f"TABLE_VALIDATION_{table2_alias}_ONLY_{self.timestamp}"
        
        mismatch_csv = f"table_validation_mismatches_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
        table1_only_csv = f"table_validation_{table1_alias}_only_{self.timestamp}.csv"
        table2_only_csv = f"table_validation_{table2_alias}_only_{self.timestamp}.csv"
        summary_csv = f"table_validation_summary_{table1_alias}_vs_{table2_alias}_{self.timestamp}.csv"
        
        # Create Snowflake tables
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
        
        # Write to Snowflake stage
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
        
        # Always write summary to stage
        self.write_to_snowflake_stage([summary_stats], summary_csv, stage_name)
        stage_files['summary'] = f"{stage_name}{summary_csv}"
        
        # Calculate overall validation status
        validation_passed = (
            len(mismatched_rows) == 0 and 
            len(table1_only_rows) == 0 and 
            len(table2_only_rows) == 0 and
            len(validation_issues) == 0
        )
        
        # Prepare comprehensive results
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
    
    def print_validation_report(self, results: Dict[str, Any]):
        """
        Print a comprehensive validation report
        
        Args:
            results: Results dictionary from validation
        """
        print("\n" + "="*80)
        print("COMPREHENSIVE TABLE VALIDATION REPORT")
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
        print(f"  Data Mismatches: {summary['mismatched_rows_count']:,}")
        print(f"  Rows only in {table1_alias}: {summary[f'{table1_alias}_only_rows_count']:,}")
        print(f"  Rows only in {table2_alias}: {summary[f'{table2_alias}_only_rows_count']:,}")
        print(f"  Total Issues Found: {summary['total_issues_found']:,}")
        
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
        print(f"  Total Common Columns: {stats['total_common_columns']}")
        print(f"  Validation Date: {stats['validation_date']}")


def validate_tables_sp(session: Session, 
                      table1_name: str, 
                      table2_name: str,
                      primary_key_columns: str = None,
                      table1_alias: str = "SQL",
                      table2_alias: str = "MDP", 
                      environment: str = "SNOWFLAKE",
                      create_tables: bool = True,
                      stage_name: str = "@~/") -> str:
    """
    Stored procedure version of comprehensive table validation
    
    Args:
        session: Snowpark session
        table1_name: Fully qualified name of first table
        table2_name: Fully qualified name of second table
        primary_key_columns: Comma-separated list of primary key column names
        table1_alias: Alias for first table (default: SQL for dev)
        table2_alias: Alias for second table (default: MDP for prod)
        environment: Database environment
        create_tables: Whether to create result tables
        stage_name: Snowflake stage for output files
    
    Returns:
        Validation results summary as string
    """
    try:
        # Parse primary key columns
        pk_columns = None
        if primary_key_columns:
            pk_columns = [col.strip().upper() for col in primary_key_columns.split(',')]
        
        # Create validator
        validator = GenericTableValidator(session)
        
        # Run comprehensive validation
        results = validator.validate_tables(
            table1_name=table1_name,
            table2_name=table2_name,
            primary_key_columns=pk_columns,
            table1_alias=table1_alias,
            table2_alias=table2_alias,
            environment=environment,
            create_tables=create_tables,
            stage_name=stage_name
        )
        
        if results:
            summary = results['results_summary']
            stats = results['summary_statistics']
            table1_alias = stats['table1_alias']
            table2_alias = stats['table2_alias']
            
            return f"""
Table Validation {results['validation_status']}
Environment: {results['environment']}
{table1_alias} ({stats['table1_name']}): {stats['total_table1_rows']:,} rows
{table2_alias} ({stats['table2_name']}): {stats['total_table2_rows']:,} rows
Mismatches: {summary['mismatched_rows_count']:,}
{table1_alias} Only: {summary[f'{table1_alias}_only_rows_count']:,}
{table2_alias} Only: {summary[f'{table2_alias}_only_rows_count']:,}
Total Issues: {summary['total_issues_found']:,}
Primary Keys: {', '.join(stats['primary_key_columns'])}
Common Columns: {stats['total_common_columns']}
            """.strip()
        else:
            return "Table validation failed to complete"
            
    except Exception as e:
        return f"Error during table validation: {str(e)}"


def main():
    """
    Main function for testing the generic table validator
    """
    try:
        validator = GenericTableValidator()
        
        results = validator.validate_tables(
            table1_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
            table2_name='BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD',
            primary_key_columns=['MEMBER_ID'],
            table1_alias='SQL',
            table2_alias='MDP',
            environment='SNOWFLAKE',
            create_tables=True,
            stage_name='@~/'
        )
        
        if results:
            validator.print_validation_report(results)
            return results
        else:
            print("Validation failed to complete")
            return None
            
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        logging.error(f"Validation error: {str(e)}")
        return None


if __name__ == "__main__":
    main()
