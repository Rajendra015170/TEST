-- ========================================================================
-- ENCRYPT_TABLES Stored Procedure - Usage Examples and Documentation
-- ========================================================================

-- PROCEDURE SIGNATURE:
-- DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
--     "SRC_DATABASE_NAME" VARCHAR, 
--     "SRC_SCHEMA_NAME" VARCHAR, 
--     "TGT_DATABASE_NAME" VARCHAR, 
--     "TGT_SCHEMA_NAME" VARCHAR,
--     "TABLE_LIST_TABLE" VARCHAR DEFAULT NULL  -- Table containing list of tables to process
-- )

-- ========================================================================
-- SETUP: CREATE TABLE LIST TABLE (One-time setup)
-- ========================================================================

-- Create a table to store the list of tables you want to encrypt
CREATE OR REPLACE TABLE DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT (
    TABLE_NAME VARCHAR(255) NOT NULL,
    PRIORITY NUMBER DEFAULT 1,
    DESCRIPTION VARCHAR(500),
    CREATED_DATE TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Insert tables you want to encrypt
INSERT INTO DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT (TABLE_NAME, DESCRIPTION) VALUES
('CUSTOMERS', 'Customer personal information'),
('ORDERS', 'Order details with PII'),
('PAYMENTS', 'Payment information'),
('USER_PROFILES', 'User profile data'),
('EMPLOYEE_DATA', 'Employee sensitive data');

-- ========================================================================
-- USAGE EXAMPLES
-- ========================================================================

-- Example 1: Encrypt ALL tables in a schema (existing behavior)
CALL DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    'SOURCE_DB', 
    'PUBLIC', 
    'TARGET_DB', 
    'ENCRYPTED_SCHEMA'
);

-- Example 2: Encrypt SPECIFIC tables from your table list
CALL DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    'SOURCE_DB', 
    'PUBLIC', 
    'TARGET_DB', 
    'ENCRYPTED_SCHEMA',
    'DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT'
);

-- Example 3: Use a different table list (maybe for different environments)
-- First create environment-specific table
CREATE OR REPLACE TABLE DEV_DB_MANAGER.ENCRYPTION.PROD_TABLES_TO_ENCRYPT AS
SELECT * FROM DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT 
WHERE TABLE_NAME IN ('CUSTOMERS', 'PAYMENTS');

-- Then use it in the procedure
CALL DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    'SOURCE_DB', 
    'PUBLIC', 
    'TARGET_DB', 
    'ENCRYPTED_SCHEMA',
    'DEV_DB_MANAGER.ENCRYPTION.PROD_TABLES_TO_ENCRYPT'
);

-- ========================================================================
-- MONITORING AND STATUS CHECKING
-- ========================================================================

-- Check the status of the last encryption run
-- Replace 'your_session_id' with the actual session ID from the return message
SELECT * FROM TASK_STATUS_your_session_id 
ORDER BY START_TIME DESC;

-- Get summary of processing results
SELECT 
    STATUS,
    COUNT(*) as TABLE_COUNT,
    AVG(PROCESSING_DURATION) as AVG_DURATION_SECONDS
FROM TASK_STATUS_your_session_id 
WHERE TABLE_NAME NOT IN ('PROCEDURE_START', 'PROCEDURE_END')
GROUP BY STATUS;

-- Get failed tables with error details
SELECT 
    TABLE_NAME,
    ERROR_MESSAGE,
    START_TIME,
    END_TIME
FROM TASK_STATUS_your_session_id 
WHERE STATUS = 'FAILED';

-- ========================================================================
-- KEY IMPROVEMENTS IN THIS VERSION
-- ========================================================================

/*
1. ENHANCED ERROR HANDLING:
   - Individual table failures don't stop the entire process
   - Detailed error logging with timestamps
   - Global exception handler for procedure-level failures
   - Comprehensive status tracking

2. NEW TABLE_LIST_TABLE PARAMETER:
   - Filter specific tables for encryption by reading from a database table
   - Pass table name containing list of tables to process
   - NULL or empty string processes all tables (existing logic)
   - Maintains backward compatibility

3. PERFORMANCE OPTIMIZATIONS:
   - Removed ASYNC operations (they were causing issues)
   - Better SQL query structure
   - Reduced redundant operations
   - Enhanced NULL handling with COALESCE

4. IMPROVED LOGGING:
   - Start/end timestamps for each table
   - Processing duration tracking
   - Procedure-level status tracking
   - Better error message details

5. DATA QUALITY IMPROVEMENTS:
   - Better handling of invalid characters
   - Enhanced NULL value processing
   - Improved regex for character replacement
   - More robust encryption logic

6. MONITORING CAPABILITIES:
   - Detailed status table with timing information
   - Summary statistics in return message
   - Easy identification of failed tables
   - Processing duration metrics
*/

-- ========================================================================
-- TROUBLESHOOTING COMMON ISSUES
-- ========================================================================

/*
ISSUE: Procedure returns immediately without processing
SOLUTION: Check if source tables exist and have data

ISSUE: Some tables fail with encryption errors
SOLUTION: Check the CLASSIFICATION_DETAILS table for proper tag mapping

ISSUE: Performance is slow
SOLUTION: Consider processing tables in smaller batches using TABLE_LIST parameter

ISSUE: Invalid character errors
SOLUTION: The procedure now handles this automatically with enhanced regex

ISSUE: Memory issues with large tables
SOLUTION: Process one table at a time using TABLE_LIST parameter
*/

-- ========================================================================
-- TABLE LIST MANAGEMENT
-- ========================================================================

-- View current table list
SELECT * FROM DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT ORDER BY TABLE_NAME;

-- Add a new table to encrypt
INSERT INTO DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT (TABLE_NAME, DESCRIPTION) 
VALUES ('NEW_SENSITIVE_TABLE', 'Newly identified sensitive table');

-- Remove a table from encryption list
DELETE FROM DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT 
WHERE TABLE_NAME = 'OLD_TABLE';

-- Create priority-based table list (encrypt high priority tables first)
CREATE OR REPLACE TABLE DEV_DB_MANAGER.ENCRYPTION.HIGH_PRIORITY_TABLES AS
SELECT TABLE_NAME FROM DEV_DB_MANAGER.ENCRYPTION.TABLES_TO_ENCRYPT 
WHERE PRIORITY >= 5
ORDER BY PRIORITY DESC;

-- Use priority-based table list
CALL DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    'SOURCE_DB', 
    'PUBLIC', 
    'TARGET_DB', 
    'ENCRYPTED_SCHEMA',
    'DEV_DB_MANAGER.ENCRYPTION.HIGH_PRIORITY_TABLES'
);

-- Create temporary table list for testing
CREATE OR REPLACE TEMPORARY TABLE TEMP_TEST_TABLES (TABLE_NAME VARCHAR);
INSERT INTO TEMP_TEST_TABLES VALUES ('TEST_TABLE_1'), ('TEST_TABLE_2');

-- Use temporary table list
CALL DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    'SOURCE_DB', 
    'PUBLIC', 
    'TARGET_DB', 
    'ENCRYPTED_SCHEMA',
    'TEMP_TEST_TABLES'
);

-- ========================================================================
-- VALIDATION QUERIES
-- ========================================================================

-- Verify source tables exist
SELECT COUNT(*) as SOURCE_TABLE_COUNT
FROM {SRC_DATABASE_NAME}.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{SRC_SCHEMA_NAME}'
  AND TABLE_TYPE <> 'VIEW'
  AND TABLE_NAME NOT LIKE '%_BKP%'
  AND TABLE_NAME NOT LIKE '%_BACKUP%'
  AND TABLE_NAME NOT LIKE '%_BCKUP%'
  AND TABLE_NAME NOT LIKE '%DBT_%'
  AND TABLE_NAME NOT LIKE '%-%';

-- Check classification details
SELECT 
    "SCHEMA",
    "TABLE", 
    COUNT(*) as PII_COLUMN_COUNT
FROM DEV_DB_MANAGER.ENCRYPTION.CLASSIFICATION_DETAILS
WHERE "SCHEMA" = '{SRC_SCHEMA_NAME}'
GROUP BY "SCHEMA", "TABLE"
ORDER BY PII_COLUMN_COUNT DESC;

-- Verify target tables were created
SELECT COUNT(*) as TARGET_TABLE_COUNT
FROM {TGT_DATABASE_NAME}.INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = '{TGT_SCHEMA_NAME}';