-- ============================================================================
-- Table Validator Stored Procedure Usage Examples
-- ============================================================================

-- 1. Basic usage with automatic primary key detection
CALL GENERIC_TABLE_VALIDATOR(
    'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
    'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD'
);

-- 2. Usage with specified primary key columns
CALL GENERIC_TABLE_VALIDATOR(
    'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER',
    'BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD',
    'MEMBER_ID,ENROLLMENT_DATE'  -- Comma-separated primary key columns
);

-- 3. Usage with custom table aliases and environment
CALL GENERIC_TABLE_VALIDATOR(
    'DEV_SCHEMA.TABLE_A',
    'PROD_SCHEMA.TABLE_A',
    'ID,DATE_KEY',
    'DEV',     -- Table 1 alias
    'PROD',    -- Table 2 alias
    'SNOWFLAKE_PROD'  -- Environment name
);

-- 4. Usage without creating permanent tables (only stage files)
CALL GENERIC_TABLE_VALIDATOR(
    'SCHEMA1.CUSTOMERS',
    'SCHEMA2.CUSTOMERS', 
    'CUSTOMER_ID',
    'OLD',
    'NEW',
    'MIGRATION_TEST',
    FALSE  -- Don't create permanent tables
);

-- 5. Usage with custom stage location
CALL GENERIC_TABLE_VALIDATOR(
    'FINANCE.TRANSACTIONS_Q1',
    'FINANCE.TRANSACTIONS_Q1_CORRECTED',
    'TRANSACTION_ID',
    'Q1_ORIG',
    'Q1_FIXED',
    'DATA_CORRECTION',
    TRUE,
    '@my_custom_stage/'  -- Custom stage path
);

-- 6. Complete example with all parameters
CALL GENERIC_TABLE_VALIDATOR(
    'DATABASE1.SCHEMA1.EMPLOYEE_DATA',      -- Table 1
    'DATABASE2.SCHEMA1.EMPLOYEE_DATA',      -- Table 2
    'EMPLOYEE_ID,DEPT_CODE',                -- Primary keys
    'LEGACY_SYSTEM',                        -- Table 1 alias
    'NEW_SYSTEM',                           -- Table 2 alias
    'MIGRATION_VALIDATION',                 -- Environment
    TRUE,                                   -- Create tables
    '@validation_results/'                  -- Stage path
);

-- ============================================================================
-- Querying Results After Validation
-- ============================================================================

-- Check what validation tables were created (replace timestamp as needed)
SHOW TABLES LIKE 'TABLE_VALIDATION_%';

-- Query mismatches (example table name - actual will have timestamp)
SELECT * FROM TABLE_VALIDATION_MISMATCHES_SQL_VS_MDP_20240128_143022
LIMIT 100;

-- Query records only in first table
SELECT * FROM TABLE_VALIDATION_SQL_ONLY_20240128_143022
LIMIT 100;

-- Query records only in second table  
SELECT * FROM TABLE_VALIDATION_MDP_ONLY_20240128_143022
LIMIT 100;

-- ============================================================================
-- Checking Stage Files
-- ============================================================================

-- List files in your user stage
LIST @~/;

-- List files in custom stage
LIST @validation_results/;

-- Query a stage file directly (example - replace with actual filename)
SELECT * FROM @~/table_validation_summary_sql_vs_mdp_20240128_143022.csv;

-- Copy stage file to local (if needed)
GET @~/table_validation_mismatches_sql_vs_mdp_20240128_143022.csv file://C:/temp/;

-- ============================================================================
-- Error Handling and Troubleshooting
-- ============================================================================

-- If you get permission errors, ensure you have access to:
-- 1. Both source tables
-- 2. CREATE TABLE privileges in current database/schema
-- 3. WRITE privileges to the specified stage

-- Check table access
DESCRIBE TABLE your_table_name;
SELECT COUNT(*) FROM your_table_name LIMIT 1;

-- Check stage access
LIST @~/;
PUT file://test.txt @~/;

-- ============================================================================
-- Cleanup Commands
-- ============================================================================

-- Drop validation tables (be careful - replace with actual names)
-- DROP TABLE IF EXISTS TABLE_VALIDATION_MISMATCHES_SQL_VS_MDP_20240128_143022;
-- DROP TABLE IF EXISTS TABLE_VALIDATION_SQL_ONLY_20240128_143022;
-- DROP TABLE IF EXISTS TABLE_VALIDATION_MDP_ONLY_20240128_143022;

-- Remove stage files
-- REMOVE @~/table_validation_mismatches_sql_vs_mdp_20240128_143022.csv;
-- REMOVE @~/table_validation_sql_only_20240128_143022.csv;
-- REMOVE @~/table_validation_mdp_only_20240128_143022.csv;
-- REMOVE @~/table_validation_summary_sql_vs_mdp_20240128_143022.csv;
