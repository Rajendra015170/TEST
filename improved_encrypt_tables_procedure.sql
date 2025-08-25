CREATE OR REPLACE PROCEDURE DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES(
    "SRC_DATABASE_NAME" VARCHAR, 
    "SRC_SCHEMA_NAME" VARCHAR, 
    "TGT_DATABASE_NAME" VARCHAR, 
    "TGT_SCHEMA_NAME" VARCHAR,
    "TABLE_LIST_TABLE" VARCHAR DEFAULT NULL  -- New parameter: table name containing list of tables to process or NULL for all tables
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE 
    tbl_name VARCHAR := '''';
    tables_to_check RESULTSET; 
    encr_tbl_tag_cnt NUMBER := 0;
    inf_table VARCHAR := SRC_DATABASE_NAME || ''.INFORMATION_SCHEMA.TABLES'';
    inf_columns VARCHAR := SRC_DATABASE_NAME || ''.INFORMATION_SCHEMA.COLUMNS'';
    mask_char VARCHAR := '''';
    mask_len VARCHAR := '''';
    trim_pad VARCHAR := '''';
    tag_db VARCHAR := ''DEV_DB_MANAGER.ENCRYPTION.'';  
    metatable VARCHAR := tag_db || ''CLASSIFICATION_DETAILS''; 
    session_id VARCHAR := (SELECT CURRENT_SESSION());
    temp_table VARCHAR := ''LIST_OF_TABLES_'' || session_id;
    status_table VARCHAR := ''TASK_STATUS_'' || session_id;
    total_tbl_processed_cnt NUMBER := 0;
    successful_tbl_cnt NUMBER := 0;
    failed_tbl_cnt NUMBER := 0;
    current_table_processing VARCHAR := '''';
    error_msg VARCHAR := '''';
    table_filter_condition VARCHAR := '''';
        
BEGIN 
    -- Initialize status tracking
    current_table_processing := ''INITIALIZATION'';
    
    -- Create a Temporary table for Status Tracking with enhanced fields
    CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:status_table) (
        TABLE_NAME VARCHAR,
        STATUS VARCHAR,
        ERROR_MESSAGE VARCHAR,
        START_TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
        END_TIME TIMESTAMP,
        PROCESSING_DURATION NUMBER
    );

    -- Log initialization
    INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME, STATUS) 
    VALUES (''PROCEDURE_START'', ''INITIALIZED'');

    -- Build table filter condition if TABLE_LIST_TABLE is provided
    IF (TABLE_LIST_TABLE IS NOT NULL AND TRIM(TABLE_LIST_TABLE) != '''') THEN
        -- Build IN clause from the table containing list of tables
        LET table_list_query VARCHAR := ''SELECT LISTAGG('''''''''' || TABLE_NAME || '''''''', '''','''') FROM '' || TABLE_LIST_TABLE;
        LET table_names_list VARCHAR;
        
        -- Execute query to get comma-separated table names
        EXECUTE IMMEDIATE table_list_query INTO table_names_list;
        
        IF (table_names_list IS NOT NULL AND TRIM(table_names_list) != '''') THEN
            table_filter_condition := '' AND tbl.TABLE_NAME IN ('' || table_names_list || '')'';
        END IF;
    END IF;

    -- Drop and create temporary table with enhanced logic
    DROP TABLE IF EXISTS IDENTIFIER(:temp_table); 
    
    -- Build the dynamic SQL for temp table creation
    LET temp_table_sql VARCHAR := ''
    CREATE OR REPLACE TEMPORARY TABLE '' || temp_table || '' AS
    WITH GET_DISTINCT_COLS AS (
        SELECT "DATABASE", "SCHEMA", "TABLE", "COLUMN",
               "MAPPED_TAG",
               "ALPHABET",
               REPLACE(MAPPED_TAG, ''''ENCR_'''', ''''KEY_'''') AS ENCR_KEY,
               REPLACE(MAPPED_TAG, ''''ENCR_'''', ''''TWEAK_'''') AS ENCR_TWEAK
        FROM '' || metatable || ''
    ),
    CTE_BUILD_TBL AS (
        SELECT 
            tbl.TABLE_SCHEMA,
            tbl.TABLE_NAME,
            col.COLUMN_NAME,
            col.ORDINAL_POSITION
        FROM '' || inf_table || '' AS tbl
        INNER JOIN '' || inf_columns || '' AS col
           ON tbl.TABLE_NAME = col.TABLE_NAME 
          AND tbl.TABLE_SCHEMA = col.TABLE_SCHEMA
        WHERE tbl.TABLE_SCHEMA = '''''' || src_schema_name || ''''''
            AND tbl.TABLE_NAME NOT LIKE ''''%_BKP%'''' 
            AND tbl.TABLE_NAME NOT LIKE ''''%_BACKUP%'''' 
            AND tbl.TABLE_NAME NOT LIKE ''''%_BCKUP%''''
            AND tbl.TABLE_NAME NOT LIKE ''''%DBT_%''''
            AND tbl.TABLE_NAME NOT LIKE ''''%-%''''
            AND tbl.TABLE_TYPE <> ''''VIEW'''' '' || 
            table_filter_condition || ''
        ORDER BY tbl.TABLE_NAME, col.ORDINAL_POSITION ASC
    )
    SELECT A.TABLE_NAME, A.COLUMN_NAME, A.ORDINAL_POSITION, 
           B.MAPPED_TAG, B.ENCR_KEY, B.ENCR_TWEAK, B.ALPHABET 
    FROM CTE_BUILD_TBL A
    LEFT JOIN GET_DISTINCT_COLS B
        ON B."SCHEMA" = A.table_schema
        AND B."TABLE" = A.table_name 
        AND B."COLUMN" = A.column_name
    ORDER BY A.TABLE_NAME, A.ORDINAL_POSITION ASC'';
    
    EXECUTE IMMEDIATE temp_table_sql;
    
    -- Get distinct tables to process
    tables_to_check := (SELECT DISTINCT TABLE_NAME FROM IDENTIFIER(:temp_table) ORDER BY TABLE_NAME ASC);
    
    -- Process each table with comprehensive error handling
    FOR tbls IN tables_to_check DO
        BEGIN
            LET tbl_name VARCHAR := tbls.table_name;
            current_table_processing := tbl_name;
            
            LET qry_head VARCHAR := '''';
            LET dcrpt_qry_head VARCHAR := '''';
            LET qry_stmt VARCHAR := '''';
            LET sql_stmt VARCHAR := '''';
            LET dcrpt_sql_stmt VARCHAR := '''';
            LET set_tag_stmt VARCHAR := '''';
            LET set_tag_stmt_head VARCHAR := '' ALTER TABLE '' || :tgt_database_name || ''.'' || tgt_schema_name || ''.'' || :tbl_name || '' MODIFY COLUMN '';
            LET c1 CURSOR FOR SELECT * FROM IDENTIFIER(?) WHERE TABLE_NAME = ? ORDER BY TABLE_NAME, ORDINAL_POSITION ASC;
            
            -- Log table processing start
            INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME, STATUS) 
            VALUES (:tbl_name, ''IN_PROGRESS'');
            
            -- Get PII count for this table
            encr_tbl_tag_cnt := (
                SELECT COALESCE(PII_COUNT, 0) 
                FROM (
                    SELECT TABLE_NAME, COUNT(MAPPED_TAG) AS PII_COUNT 
                    FROM IDENTIFIER(:temp_table) 
                    WHERE TABLE_NAME = :tbl_name 
                      AND MAPPED_TAG IS NOT NULL
                    GROUP BY TABLE_NAME
                )
            );

            total_tbl_processed_cnt := total_tbl_processed_cnt + 1;
            
            -- Process table based on PII classification
            IF (encr_tbl_tag_cnt = 0) THEN
                -- No PII columns - simple clone
                sql_stmt := ''CREATE OR REPLACE TABLE '' || :tgt_database_name || ''.'' || 
                           tgt_schema_name || ''.'' || :tbl_name || 
                           '' CLONE '' || :src_database_name || ''.'' || 
                           src_schema_name || ''.'' || :tbl_name || '';'';
                           
                EXECUTE IMMEDIATE sql_stmt;
                
            ELSE 
                -- Has PII columns - encryption needed
                OPEN c1 USING (:temp_table, :tbl_name);
                
                FOR record IN c1 DO
                    IF (record.MAPPED_TAG IS NULL) THEN 
                        sql_stmt := sql_stmt || '' '' || record.COLUMN_NAME || '','';
                        dcrpt_sql_stmt := dcrpt_sql_stmt || '' '' || record.COLUMN_NAME || '','';
                    ELSE                
                        IF (record.ALPHABET = ''numeric'') THEN
                            mask_char := ''000000'';
                            mask_len := 6;
                            trim_pad := ''LTRIM('' || record.COLUMN_NAME || '', ''''000000'''') '';
                        ELSE
                            mask_char := ''AAAA'';
                            mask_len := 4;
                            trim_pad := ''LTRIM('' || record.COLUMN_NAME || '', ''''AAAA'''') '';
                        END IF;
                        
                        -- Enhanced encryption with better error handling for invalid characters
                        sql_stmt := sql_stmt || '' '' || 
                                   ''ALTR_DSAAS_DB.ALTR_DSAAS_0BFD0C5E_B187_4E65_980F_9B8BE4717768.ALTR_FPE_ENCRYPT('' ||
                                   ''LPAD(COALESCE(regexp_replace('' || record.COLUMN_NAME || 
                                   '', ''''[^\\\\x01-\\\\x7F]'''', ''''''''), ''''''''), '' ||
                                   ''LENGTH(COALESCE('' || record.COLUMN_NAME || '', '''''''')) + '' || 
                                   mask_len || '', '''''' || mask_char || ''''''), '' ||
                                   '''''' || record.ENCR_KEY || '''''', '' ||
                                   '''''' || record.ENCR_TWEAK || '''''', '' ||
                                   '''''' || record.ALPHABET || '''''') AS '' || 
                                   record.COLUMN_NAME || '','';
                        
                        set_tag_stmt := set_tag_stmt || record.COLUMN_NAME || 
                                       '' SET TAG '' || :tag_db || record.MAPPED_TAG || 
                                       ''='''''''''' || '','' ;
                        dcrpt_sql_stmt := dcrpt_sql_stmt || '' '' || :trim_pad || 
                                         record.COLUMN_NAME || '','';
                    END IF;
                END FOR;
                
                CLOSE c1;
                
                -- Build final SQL statements
                sql_stmt := LEFT(sql_stmt, LENGTH(sql_stmt) - 1) || 
                           '' FROM '' || :src_database_name || ''.'' || 
                           :src_schema_name || ''.'' || :tbl_name;
                           
                qry_head := ''CREATE OR REPLACE TABLE '' || :tgt_database_name || 
                           ''.'' || :tgt_schema_name || ''.'' || :tbl_name || 
                           '' AS SELECT '';
                
                -- Execute encryption
                EXECUTE IMMEDIATE (qry_head || sql_stmt);
                
            END IF;
            
            -- Update status to completed
            UPDATE IDENTIFIER(:status_table) 
            SET STATUS = ''COMPLETED'', 
                END_TIME = CURRENT_TIMESTAMP(),
                PROCESSING_DURATION = DATEDIFF(second, START_TIME, CURRENT_TIMESTAMP())
            WHERE TABLE_NAME = :tbl_name;
            
            successful_tbl_cnt := successful_tbl_cnt + 1;
            
        EXCEPTION
            WHEN OTHER THEN
                failed_tbl_cnt := failed_tbl_cnt + 1;
                error_msg := SQLERRM;
                
                -- Log detailed error information
                INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME, STATUS, ERROR_MESSAGE, END_TIME) 
                VALUES (:tbl_name, ''FAILED'', :error_msg, CURRENT_TIMESTAMP());
                
                -- Continue processing other tables rather than failing entire procedure
                CONTINUE;
        END;
    END FOR;
    
    -- Final status update
    INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME, STATUS, ERROR_MESSAGE) 
    VALUES (''PROCEDURE_END'', ''COMPLETED'', 
            ''Total: '' || :total_tbl_processed_cnt || 
            '', Successful: '' || :successful_tbl_cnt || 
            '', Failed: '' || :failed_tbl_cnt);
    
    -- Return comprehensive summary
    RETURN ''Encryption Process Summary - Total Tables Processed: '' || :total_tbl_processed_cnt || 
           '', Successfully Encrypted: '' || :successful_tbl_cnt || 
           '', Failed: '' || :failed_tbl_cnt || 
           ''. Check status table '' || :status_table || '' for detailed results.'';

EXCEPTION
    WHEN OTHER THEN
        -- Global exception handler
        error_msg := SQLERRM;
        
        -- Log global error
        INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME, STATUS, ERROR_MESSAGE) 
        VALUES (:current_table_processing, ''PROCEDURE_FAILED'', :error_msg);
        
        -- Return error information
        RETURN ''Procedure failed during processing of table: '' || :current_table_processing || 
               ''. Error: '' || :error_msg || 
               ''. Check status table '' || :status_table || '' for details.'';
END;
';