CREATE OR REPLACE PROCEDURE DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES("SRC_DATABASE_NAME" VARCHAR, "SRC_SCHEMA_NAME" VARCHAR, "TGT_DATABASE_NAME" VARCHAR, "TGT_SCHEMA_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE 
    tbl_name varchar := '''';
    tables_to_check RESULTSET; 
    encr_tbl_tag_cnt NUMBER := 0;
    inf_table VARCHAR := SRC_DATABASE_NAME || ''.INFORMATION_SCHEMA.TABLES'';
    inf_columns VARCHAR := SRC_DATABASE_NAME || ''.INFORMATION_SCHEMA.COLUMNS'';
    mask_char VARCHAR := '''';
    mask_len VARCHAR := '''';
    trim_pad VARCHAR :='''';
    tag_db VARCHAR := ''DEV_DB_MANAGER.ENCRYPTION.'';  
    metatable VARCHAR := tag_db || ''CLASSIFICATION_DETAILS''; 
    session_id Varchar := (SELECT CURRENT_SESSION());
    temp_table VARCHAR := ''LIST_OF_TABLES_'' || session_id;
    status_table VARCHAR := ''TASK_STATUS_'' || session_id;
    total_tbl_processed_cnt NUMBER :=0;
    is_transient BOOLEAN := FALSE;
    table_type_keyword VARCHAR := '''';
        
BEGIN 

-- Create a  Temporary table for Status Tracking 

CREATE OR REPLACE TEMPORARY TABLE IDENTIFIER(:status_table) (
    TABLE_NAME VARCHAR,
    STATUS VARCHAR,
    ERROR_MESSAGE VARCHAR
);

DROP TABLE IF EXISTS IDENTIFIER(:temp_table); 
CREATE or replace TEMPORARY TABLE IDENTIFIER(:temp_table) AS
With GET_DISTINCT_COLS AS
    (
        SELECT "DATABASE","SCHEMA" ,"TABLE","COLUMN",
        "MAPPED_TAG",
        "ALPHABET",
        REPLACE(MAPPED_TAG,''ENCR_'',''KEY_'') ENCR_KEY,
        REPLACE(MAPPED_TAG,''ENCR_'',''TWEAK_'') ENCR_TWEAK
        FROM  IDENTIFIER(:metatable)
    ),
    CTE_BUILD_TBL AS
    (
        SELECT 
            tbl.TABLE_SCHEMA,
            tbl.TABLE_NAME,
            col.COLUMN_NAME,
            col.ORDINAL_POSITION,
            CASE 
                WHEN tbl.IS_TRANSIENT = ''YES'' THEN TRUE 
                ELSE FALSE 
            END AS IS_TRANSIENT_TABLE
        FROM IDENTIFIER(:inf_table) AS tbl
        INNER JOIN IDENTIFIER(:inf_columns) AS col
           ON tbl.TABLE_NAME = col.TABLE_NAME 
          AND tbl.TABLE_SCHEMA = col.TABLE_SCHEMA
        WHERE tbl.TABLE_SCHEMA = :src_schema_name
            AND tbl.TABLE_NAME  NOT LIKE ''%_BKP%'' 
            AND tbl.TABLE_NAME NOT LIKE ''%_BACKUP%'' 
            AND tbl.TABLE_NAME NOT LIKE ''%_BCKUP%''
            AND tbl.TABLE_NAME NOT LIKE ''%DBT_%''
            AND tbl.TABLE_NAME NOT LIKE ''%-%''
            AND tbl.TABLE_TYPE <> ''VIEW''
        ORDER BY tbl.TABLE_NAME, col.ORDINAL_POSITION ASC
    )
    SELECT A.TABLE_NAME,A.COLUMN_NAME,A.ORDINAL_POSITION,A.IS_TRANSIENT_TABLE,B.MAPPED_TAG,B.ENCR_KEY,B.ENCR_TWEAK,B.ALPHABET 
    FROM  CTE_BUILD_TBL A
    LEFT  JOIN GET_DISTINCT_COLS B
        ON  B."SCHEMA"=A.table_schema
        AND B."TABLE"= A.table_name 
        AND B."COLUMN" = A.column_name
    ORDER BY 
             A.TABLE_NAME,
             A.ORDINAL_POSITION
             ASC;  
    
    tables_to_check := (SELECT DISTINCT TABLE_NAME FROM IDENTIFIER(:temp_table) ORDER BY TABLE_NAME ASC);  
    
    for tbls in tables_to_check do
         LET tbl_name varchar := tbls.table_name;
         LET qry_head varchar := '''';
         LET dcrpt_qry_head varchar := '''';
         LET qry_stmt varchar := '''';
         LET sql_stmt varchar := '''';
         LET dcrpt_sql_stmt   := '''';
         LET set_tag_stmt varchar := '''';
         LET set_tag_stmt_head VARCHAR := '' ALTER TABLE '' || :tgt_database_name || ''.'' || tgt_schema_name || ''_ENCR.'' || :tbl_name || '' MODIFY COLUMN '';
         LET c1 cursor for SELECT * FROM IDENTIFIER(?) WHERE TABLE_NAME = ?  ORDER BY TABLE_NAME,ORDINAL_POSITION ASC;         
         
         -- Check if source table is transient
         is_transient := (SELECT DISTINCT IS_TRANSIENT_TABLE FROM IDENTIFIER(:temp_table) WHERE TABLE_NAME = :tbl_name);
         
         -- Set table type keyword based on transient status
         IF (is_transient = TRUE) THEN
             table_type_keyword := ''TRANSIENT '';
         ELSE
             table_type_keyword := '''';
         END IF;
         
         encr_tbl_tag_cnt :=(SELECT PII_COUNT FROM (Select TABLE_NAME,COUNT(MAPPED_TAG) PII_COUNT from IDENTIFIER(:temp_table) WHERE TABLE_NAME=:tbl_name GROUP BY TABLE_NAME));

         INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME,STATUS) VALUES(:tbl_name,''IN PROGRESS'');
         
         total_tbl_processed_cnt := total_tbl_processed_cnt + 1;
         
         if (encr_tbl_tag_cnt = 0) THEN
            sql_stmt := ''CREATE OR REPLACE '' || :table_type_keyword || ''TABLE '' || :tgt_database_name || ''.'' || tgt_schema_name || ''.'' || :tbl_name || ''  CLONE '' || :src_database_name || ''.'' || src_schema_name || ''.'' || :tbl_name || '';'' ;
            ASYNC( EXECUTE IMMEDIATE (sql_stmt||'';''));
         else 
            open c1 USING (:temp_table,:tbl_name);
            for record in c1 do
                if (record.MAPPED_TAG is NULL ) THEN 
                    sql_stmt:= sql_stmt || '' '' || record.COLUMN_NAME  || '','';
                    dcrpt_sql_stmt:= dcrpt_sql_stmt || '' '' || record.COLUMN_NAME  || '','';
                else                
                    if (record.ALPHABET =''numeric'') THEN
                        mask_char := ''000000'';
                        mask_len  := 6;
                        --trim_pad  := ''TO_NUMBER(''  ||   record.COLUMN_NAME || '') '';  -- Poor Data quality. Coulmns have alphanumeric data
                        trim_pad  := ''LTRIM(''  ||   record.COLUMN_NAME || '', ''''000000'''' ) '';
                    else
                        mask_char := ''AAAA'';
                        mask_len  := 4;
                        trim_pad  := ''LTRIM(''  ||   record.COLUMN_NAME || '', ''''AAAA'''' ) '';
                    end if;
                    --sql_stmt:= sql_stmt || '' '' || ''ALTR_DSAAS_DB.ALTR_DSAAS_0BFD0C5E_B187_4E65_980F_9B8BE4717768.ALTR_FPE_ENCRYPT(LPAD(''  ||   record.COLUMN_NAME || '',LENGTH('' || record.COLUMN_NAME ||  '') +'' || :mask_len || '', '''''' || :mask_char || '''''' ),'''''' ||  record.ENCR_KEY || '''''','''''' ||  record.ENCR_TWEAK  || '''''','''''' || record.ALPHABET || '''''') '' || record.COLUMN_NAME ||  '','';
                    -- Replace invalid chars with empty string. 
                    sql_stmt:= sql_stmt || '' '' || ''ALTR_DSAAS_DB.ALTR_DSAAS_0BFD0C5E_B187_4E65_980F_9B8BE4717768.ALTR_FPE_ENCRYPT(LPAD(regexp_replace(''  ||   record.COLUMN_NAME || '',''''[^\\\\x01-\\\\x7F]'''' , ''''''''),LENGTH('' || record.COLUMN_NAME ||  '') +'' || :mask_len || '', '''''' || :mask_char || '''''' ),'''''' ||  record.ENCR_KEY || '''''','''''' ||  record.ENCR_TWEAK  || '''''','''''' || record.ALPHABET || '''''') '' || record.COLUMN_NAME ||  '','';
                    
                    set_tag_stmt := set_tag_stmt || record.COLUMN_NAME || '' SET TAG '' || :tag_db || record.MAPPED_TAG || ''='''''''''' || '','' ;
                    dcrpt_sql_stmt:= dcrpt_sql_stmt || '' '' || :trim_pad || record.COLUMN_NAME ||  '','';
                end if;
            end for;
            close c1;
            sql_stmt := left(sql_stmt,LENGTH(sql_stmt)-1) || '' FROM '' || :src_database_name || ''.'' || :src_schema_name || ''.'' || :tbl_name ;
            qry_head :=  ''CREATE OR REPLACE '' || :table_type_keyword || ''table ''|| :tgt_database_name || ''.'' || :tgt_schema_name || ''.'' || :tbl_name  || ''  AS SELECT '';

            -- folowing code is to create DECRYPT view from Encrypted Table
            -- dcrpt_qry_head :=  ''CREATE OR REPLACE VIEW ''|| :tgt_database_name || ''.'' || :tgt_schema_name || ''.'' ||  :tbl_name  || ''  AS SELECT '';
            -- dcrpt_sql_stmt := left(dcrpt_sql_stmt,LENGTH(dcrpt_sql_stmt)-1) || '' FROM '' || :tgt_database_name || ''.'' || :tgt_schema_name || ''.'' || :tbl_name || ''_ENCR'' ;
            ASYNC (EXECUTE IMMEDIATE (qry_head||sql_stmt||'';''));

            -- Following code is to Tag PII Columns in Encrypted Tables
            -- if (set_tag_stmt != '''') THEN 
            --    set_tag_stmt := set_tag_stmt || left(set_tag_stmt,LENGTH(set_tag_stmt)-1) || '';'' ;
            --    EXECUTE IMMEDIATE (set_tag_stmt_head || set_tag_stmt);
            --end if ;    
         end if; 
         UPDATE IDENTIFIER(:status_table) SET STATUS=''COMPLETED'' WHERE TABLE_NAME =:tbl_name;
         
    end for;
    
AWAIT ALL;
return   ''Total # of tables Encrypted - '' || :total_tbl_processed_cnt ;
EXCEPTION
            WHEN OTHER THEN
                INSERT INTO IDENTIFIER(:status_table) (TABLE_NAME,STATUS,ERROR_MESSAGE) 
                VALUES(:tbl_name,''FAILED'',ERROR_MESSAGE());   
END;
';