CREATE OR REPLACE PROCEDURE MDP_SQL_COMPARE_PY(
    DATABASE VARCHAR,
    TABLE1_NAME VARCHAR,
    TABLE2_NAME VARCHAR,
    PRIMARY_KEY_COLUMNS VARCHAR DEFAULT NULL,
    COMPARISON_COLUMNS VARCHAR DEFAULT NULL,
    TABLE1_ALIAS VARCHAR DEFAULT 'SQL',
    TABLE2_ALIAS VARCHAR DEFAULT 'MDP',
    TARGET_SCHEMA VARCHAR DEFAULT NULL
)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'main'
AS
$$
from snowflake.snowpark import Session
from datetime import datetime
from typing import Optional, List

def build_full_table_name(database: str, table_name: str) -> str:
    if not database:
        return table_name
    if table_name.upper().startswith(database.upper() + "."):
        return table_name
    if "." in table_name:
        return f"{database}.{table_name}"
    else:
        return f"{database}.PUBLIC.{table_name}"

def get_table_columns(session: Session, table_name: str) -> List[str]:
    try:
        df = session.table(table_name)
        return [field.name.upper() for field in df.schema.fields]
    except:
        return []

def identify_primary_keys(session: Session, table1_name: str, table2_name: str, pk_columns: Optional[List[str]]) -> List[str]:
    if pk_columns:
        return [col.upper() for col in pk_columns]
    
    # Auto-detect primary keys
    table1_cols = get_table_columns(session, table1_name)
    table2_cols = get_table_columns(session, table2_name)
    common_cols = set(table1_cols) & set(table2_cols)
    
    # Look for ID columns
    for col in common_cols:
        if 'ID' in col or col in ['MEMBER_ID', 'CUSTOMER_ID', 'ACCOUNT_ID', 'USER_ID']:
            return [col]
    
    # Use first common column as fallback
    return [list(common_cols)[0]] if common_cols else []

def identify_comparison_columns(session: Session, table1_name: str, table2_name: str, 
                              pk_columns: List[str], comp_columns: Optional[List[str]]) -> List[str]:
    if comp_columns:
        return [col.upper() for col in comp_columns]
    
    # Auto-detect comparison columns
    table1_cols = get_table_columns(session, table1_name)
    table2_cols = get_table_columns(session, table2_name)
    common_cols = set(table1_cols) & set(table2_cols)
    
    # Remove primary key columns
    pk_cols_upper = [col.upper() for col in pk_columns]
    comparison_cols = [col for col in common_cols if col not in pk_cols_upper]
    
    return comparison_cols[:50]  # Limit to 50 columns

def create_comparison_tables(session: Session, table1_name: str, table2_name: str,
                           pk_columns: List[str], comp_columns: List[str],
                           table1_alias: str, table2_alias: str, target_schema: str) -> dict:
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    results = {'tables_created': [], 'counts': {}, 'errors': []}
    
    try:
        # Build join condition
        pk_join_conditions = []
        for col in pk_columns:
            pk_join_conditions.append(f"COALESCE(CAST(t1.{col} AS STRING), 'NULL') = COALESCE(CAST(t2.{col} AS STRING), 'NULL')")
        pk_join_condition = " AND ".join(pk_join_conditions)
        
        # 1. Create TABLE1-ONLY table
        table1_only_table = f"{target_schema}.TABLE_VALIDATION_{table1_alias}_ONLY_{timestamp}"
        table1_only_sql = f"""
        CREATE OR REPLACE TABLE {table1_only_table} AS
        SELECT t1.*
        FROM {table1_name} t1
        LEFT JOIN {table2_name} t2 ON {pk_join_condition}
        WHERE t2.{pk_columns[0]} IS NULL
        """
        session.sql(table1_only_sql).collect()
        
        # Count rows in table1-only
        count_result = session.sql(f"SELECT COUNT(*) as CNT FROM {table1_only_table}").collect()
        table1_only_count = count_result[0]['CNT']
        results['counts']['table1_only'] = table1_only_count
        
        if table1_only_count > 0:
            results['tables_created'].append(table1_only_table)
        else:
            # Drop empty table
            session.sql(f"DROP TABLE IF EXISTS {table1_only_table}").collect()
        
        # 2. Create TABLE2-ONLY table  
        table2_only_table = f"{target_schema}.TABLE_VALIDATION_{table2_alias}_ONLY_{timestamp}"
        table2_only_sql = f"""
        CREATE OR REPLACE TABLE {table2_only_table} AS
        SELECT t2.*
        FROM {table2_name} t2
        LEFT JOIN {table1_name} t1 ON {pk_join_condition}
        WHERE t1.{pk_columns[0]} IS NULL
        """
        session.sql(table2_only_sql).collect()
        
        # Count rows in table2-only
        count_result = session.sql(f"SELECT COUNT(*) as CNT FROM {table2_only_table}").collect()
        table2_only_count = count_result[0]['CNT']
        results['counts']['table2_only'] = table2_only_count
        
        if table2_only_count > 0:
            results['tables_created'].append(table2_only_table)
        else:
            # Drop empty table
            session.sql(f"DROP TABLE IF EXISTS {table2_only_table}").collect()
        
        # 3. Create MISMATCHES table
        mismatches_table = f"{target_schema}.TABLE_VALIDATION_MISMATCHES_{table1_alias}_VS_{table2_alias}_{timestamp}"
        
        # Build mismatch cases for each comparison column
        pk_select = ", ".join([f"t1.{col}" for col in pk_columns])
        mismatch_cases = []
        
        for col in comp_columns:
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
            mismatches_sql = f"""
            CREATE OR REPLACE TABLE {mismatches_table} AS
            {' UNION ALL '.join(mismatch_cases)}
            """
            session.sql(mismatches_sql).collect()
            
            # Count mismatches
            count_result = session.sql(f"SELECT COUNT(*) as CNT FROM {mismatches_table}").collect()
            mismatch_count = count_result[0]['CNT']
            results['counts']['mismatches'] = mismatch_count
            
            if mismatch_count > 0:
                results['tables_created'].append(mismatches_table)
            else:
                # Drop empty table
                session.sql(f"DROP TABLE IF EXISTS {mismatches_table}").collect()
        else:
            results['counts']['mismatches'] = 0
        
        return results
        
    except Exception as e:
        results['errors'].append(f"Table creation error: {str(e)}")
        return results

def main(session, database, table1_name, table2_name, primary_key_columns=None, 
         comparison_columns=None, table1_alias="SQL", table2_alias="MDP", target_schema=None):
    
    try:
        # Build full table names
        full_table1_name = build_full_table_name(database, table1_name)
        full_table2_name = build_full_table_name(database, table2_name)
        
        # Determine target schema
        if target_schema is None:
            table_parts = full_table1_name.split('.')
            if len(table_parts) >= 2:
                target_schema = '.'.join(table_parts[:-1])
            else:
                target_schema = f"{database}.PUBLIC" if database else "PUBLIC"
        
        # Parse parameters
        pk_columns = None
        if primary_key_columns:
            pk_columns = [col.strip() for col in primary_key_columns.split(',')]
        
        comp_columns = None
        if comparison_columns:
            comp_columns = [col.strip() for col in comparison_columns.split(',')]
        
        # Identify columns
        pk_cols = identify_primary_keys(session, full_table1_name, full_table2_name, pk_columns)
        if not pk_cols:
            return "ERROR: No valid primary key columns found"
        
        comp_cols = identify_comparison_columns(session, full_table1_name, full_table2_name, pk_cols, comp_columns)
        if not comp_cols:
            return "ERROR: No valid comparison columns found"
        
        # Create comparison tables
        results = create_comparison_tables(
            session, full_table1_name, full_table2_name, 
            pk_cols, comp_cols, table1_alias, table2_alias, target_schema
        )
        
        # Format simple response
        total_issues = results['counts'].get('table1_only', 0) + results['counts'].get('table2_only', 0) + results['counts'].get('mismatches', 0)
        status = "PASSED" if total_issues == 0 else "FAILED"
        
        response_lines = [
            f"Validation Status: {status}",
            f"Tables Created: {len(results['tables_created'])}",
            f"Mismatched Rows: {results['counts'].get('mismatches', 0)}",
            f"{table1_alias} Only Rows: {results['counts'].get('table1_only', 0)}",
            f"{table2_alias} Only Rows: {results['counts'].get('table2_only', 0)}",
            f"Total Issues: {total_issues}"
        ]
        
        if results['tables_created']:
            response_lines.append("Created Tables:")
            for table in results['tables_created']:
                response_lines.append(f"  - {table}")
        
        if results['errors']:
            response_lines.append("Errors:")
            for error in results['errors']:
                response_lines.append(f"  - {error}")
        
        return "\n".join(response_lines)
        
    except Exception as e:
        return f"ERROR: {str(e)}"
$$;
