Function available memory exhausted. Please visit https://docs.snowflake.com/en/developer-guide/stored-procedure/python/procedure-python-profiler for help identifying bottlenecks.


ERROR - Error writing to stage: (1304): 01be03f5-0001-f293-0000-9e5d03f9e35a: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e35a: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.
Error writing to stage table_validation_mismatches_SQL_vs_MDP_20250729_104848.csv: (1304): 01be03f5-0001-f293-0000-9e5d03f9e35a: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e35a: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.


ERROR - Error writing to stage: (1304): 01be03f5-0001-f293-0000-9e5d03f9e37e: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e37e: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.
Error writing to stage table_validation_SQL_only_20250729_104848.csv: (1304): 01be03f5-0001-f293-0000-9e5d03f9e37e: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e37e: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.


ERROR - Error writing to stage: (1304): 01be03f5-0001-f293-0000-9e5d03f9e3a2: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e3a2: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.
Error writing to stage table_validation_MDP_only_20250729_104848.csv: (1304): 01be03f5-0001-f293-0000-9e5d03f9e3a2: 100093 (22000): 01be03f5-0001-f293-0000-9e5d03f9e3a2: Cannot unload empty string without file format option field_optionally_enclosed_by being specified.


ERROR - Error writing to stage: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()
Error writing to stage table_validation_summary_SQL_vs_MDP_20250729_104848.csv: The truth value of an array with more than one element is ambiguous. Use a.any() or a.all()


2025-07-30 05:46:28,412 - INFO - query execution done
2025-07-30 05:46:28,414 - INFO - query: [SELECT  *  FROM BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER]
2025-07-30 05:46:28,464 - INFO - query execution done
2025-07-30 05:46:28,468 - INFO - query: [SELECT count(1) AS "COUNT(LITERAL())" FROM ( SELECT  *  FROM BCBSND_CONFORMED_DE...]
2025-07-30 05:46:28,537 - INFO - query execution done
2025-07-30 05:46:28,539 - INFO - query: [SELECT  *  FROM BCBSND_CONFORMED_DEV.OUTBOUND.MEMBER_ENROLLMENT_MASTER_PROD]
2025-07-30 05:46:28,588 - INFO - query execution done
2025-07-30 05:46:28,589 - INFO - Creating complete SQL-only table...
2025-07-30 05:46:28,590 - INFO - query: [CREATE OR REPLACE TEMPORARY TABLE TEMP_SQL_ONLY_20250730_054624 AS SELECT t1.* F...]
2025-07-30 05:46:32,049 - INFO - query execution done
2025-07-30 05:46:32,052 - INFO - query: [SELECT COUNT(*) as CNT FROM TEMP_SQL_ONLY_20250730_054624]
2025-07-30 05:46:32,338 - INFO - query execution done
2025-07-30 05:46:32,339 - INFO - Creating complete MDP-only table...
2025-07-30 05:46:32,340 - INFO - query: [CREATE OR REPLACE TEMPORARY TABLE TEMP_MDP_ONLY_20250730_054624 AS SELECT t2.* F...]
2025-07-30 05:46:34,050 - INFO - query execution done
2025-07-30 05:46:34,052 - INFO - query: [SELECT COUNT(*) as CNT FROM TEMP_MDP_ONLY_20250730_054624]
2025-07-30 05:46:34,274 - INFO - query execution done
2025-07-30 05:46:34,276 - INFO - Creating complete mismatches table...
2025-07-30 05:46:34,278 - INFO - query: [CREATE OR REPLACE TEMPORARY TABLE TEMP_MISMATCHES_20250730_054624 AS  SELECT t1....]
2025-07-30 05:48:33,998 - INFO - query execution done
2025-07-30 05:48:34,001 - INFO - query: [SELECT COUNT(*) as CNT FROM TEMP_MISMATCHES_20250730_054624]
2025-07-30 05:48:34,134 - INFO - query execution done
2025-07-30 05:48:34,137 - INFO - query: [SELECT COUNT(DISTINCT CONCAT(COALESCE(CAST(MEMBER_ID AS STRING), 'NULL'))) as CN...]
2025-07-30 05:48:39,999 - INFO - query execution done
2025-07-30 05:48:40,000 - INFO - ✅ STREAMING COMPARISON COMPLETED:
2025-07-30 05:48:40,000 - INFO -   - Total SQL rows: 1,881,446
2025-07-30 05:48:40,001 - INFO -   - Total MDP rows: 2,045,680
2025-07-30 05:48:40,001 - INFO -   - SQL only rows: 2,749
2025-07-30 05:48:40,001 - INFO -   - MDP only rows: 35,378
2025-07-30 05:48:40,001 - INFO -   - Unique mismatched rows: 691,193
2025-07-30 05:48:40,001 - INFO -   - Total mismatch data points: 300,127,584
2025-07-30 05:48:40,002 - INFO - query: [CREATE OR REPLACE TABLE TABLE_VALIDATION_MISMATCHES_SQL_VS_MDP_20250730_054624 A...]
2025-07-30 05:48:40,052 - INFO - query execution done
 Error during validation: (1304): 01be0868-0001-f35e-0000-9e5d04011a16: 003001 (42501): 01be0868-0001-f35e-0000-9e5d04011a16: SQL access control error:
Insufficient privileges to operate on schema 'OUTBOUND'
2025-07-30 05:48:40,060 - ERROR - Validation error: (1304): 01be0868-0001-f35e-0000-9e5d04011a16: 003001 (42501): 01be0868-0001-f35e-0000-9e5d04011a16: SQL access control error:
Insufficient privileges to operate on schema 'OUTBOUND'
2025-07-30 05:48:40,076 - WARNING - Closing a session in a stored procedure is a no-op.

