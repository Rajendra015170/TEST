CREATE OR REPLACE PROCEDURE DEV_DB_MANAGER.ENCRYPTION.ENCRYPT_TABLES_JAVA(
    "SRC_DATABASE_NAME" VARCHAR, 
    "SRC_SCHEMA_NAME" VARCHAR, 
    "TGT_DATABASE_NAME" VARCHAR, 
    "TGT_SCHEMA_NAME" VARCHAR
)
RETURNS VARCHAR
LANGUAGE JAVA
PACKAGES = ('com.snowflake:snowpark:latest')
HANDLER = 'EncryptTablesHandler.encryptTables'
AS
$$
import com.snowflake.snowpark_java.*;
import java.util.*;
import java.util.concurrent.*;
import java.sql.*;
import java.util.stream.Collectors;

public class EncryptTablesHandler {
    
    private static final String TAG_DB = "DEV_DB_MANAGER.ENCRYPTION.";
    private static final String METATABLE = TAG_DB + "CLASSIFICATION_DETAILS_NEW";
    private static final String ENCRYPTION_FUNCTION = "ALTR_DSAAS_DB.ALTR_DSAAS_0BFD0C5E_B187_4E65_980F_9B8BE4717768.ALTR_FPE_ENCRYPT";
    
    // Configuration for different data types
    private static final Map<String, MaskConfig> MASK_CONFIGS = Map.of(
        "numeric", new MaskConfig("000000", 6, "LTRIM({}, '000000')"),
        "default", new MaskConfig("AAAA", 4, "LTRIM({}, 'AAAA')")
    );
    
    public static String encryptTables(Session session, 
                                     String srcDatabaseName, 
                                     String srcSchemaName,
                                     String tgtDatabaseName, 
                                     String tgtSchemaName) {
        
        EncryptionProcessor processor = new EncryptionProcessor(session, 
            srcDatabaseName, srcSchemaName, tgtDatabaseName, tgtSchemaName);
        
        try {
            return processor.processEncryption();
        } catch (Exception e) {
            throw new RuntimeException("Encryption process failed: " + e.getMessage(), e);
        }
    }
    
    static class EncryptionProcessor {
        private final Session session;
        private final String srcDatabaseName;
        private final String srcSchemaName;
        private final String tgtDatabaseName;
        private final String tgtSchemaName;
        private final String sessionId;
        private final String statusTable;
        private final String tempTable;
        private final ExecutorService executorService;
        
        public EncryptionProcessor(Session session, String srcDb, String srcSchema, 
                                 String tgtDb, String tgtSchema) {
            this.session = session;
            this.srcDatabaseName = srcDb;
            this.srcSchemaName = srcSchema;
            this.tgtDatabaseName = tgtDb;
            this.tgtSchemaName = tgtSchema;
            this.sessionId = getCurrentSessionId();
            this.statusTable = "TASK_STATUS_" + sessionId;
            this.tempTable = "LIST_OF_TABLES_" + sessionId;
            this.executorService = Executors.newFixedThreadPool(10); // Configurable thread pool
        }
        
        public String processEncryption() {
            try {
                // Initialize tracking tables
                initializeTrackingTables();
                
                // Build metadata table
                buildMetadataTable();
                
                // Get list of tables to process
                List<TableInfo> tablesToProcess = getTablesList();
                
                // Process tables in parallel with controlled concurrency
                int totalProcessed = processTablesInParallel(tablesToProcess);
                
                return String.format("Total # of tables Encrypted - %d", totalProcessed);
                
            } finally {
                executorService.shutdown();
            }
        }
        
        private void initializeTrackingTables() {
            // Create status tracking table
            String createStatusTable = String.format("""
                CREATE OR REPLACE TEMPORARY TABLE %s (
                    TABLE_NAME VARCHAR,
                    STATUS VARCHAR,
                    ERROR_MESSAGE VARCHAR,
                    START_TIME TIMESTAMP,
                    END_TIME TIMESTAMP,
                    PROCESSING_TIME_SECONDS NUMBER
                )
                """, statusTable);
            
            session.sql(createStatusTable).collect();
        }
        
        private void buildMetadataTable() {
            String createTempTable = String.format("""
                CREATE OR REPLACE TEMPORARY TABLE %s AS
                WITH GET_DISTINCT_COLS AS (
                    SELECT "DATABASE", "SCHEMA", "TABLE", "COLUMN",
                           "MAPPED_TAG", "ALPHABET",
                           REPLACE(MAPPED_TAG, 'ENCR_', 'KEY_') ENCR_KEY,
                           REPLACE(MAPPED_TAG, 'ENCR_', 'TWEAK_') ENCR_TWEAK
                    FROM %s
                ),
                CTE_BUILD_TBL AS (
                    SELECT tbl.TABLE_SCHEMA, tbl.TABLE_NAME, col.COLUMN_NAME, col.ORDINAL_POSITION
                    FROM %s.INFORMATION_SCHEMA.TABLES AS tbl
                    INNER JOIN %s.INFORMATION_SCHEMA.COLUMNS AS col
                        ON tbl.TABLE_NAME = col.TABLE_NAME 
                        AND tbl.TABLE_SCHEMA = col.TABLE_SCHEMA
                    WHERE tbl.TABLE_SCHEMA = '%s'
                        AND tbl.TABLE_NAME NOT LIKE '%%_BKP%%'
                        AND tbl.TABLE_NAME NOT LIKE '%%_BACKUP%%'
                        AND tbl.TABLE_NAME NOT LIKE '%%_BCKUP%%'
                        AND tbl.TABLE_NAME NOT LIKE '%%DBT_%%'
                        AND tbl.TABLE_NAME NOT LIKE '%%-%'
                        AND tbl.TABLE_TYPE <> 'VIEW'
                    ORDER BY tbl.TABLE_NAME, col.ORDINAL_POSITION ASC
                )
                SELECT A.TABLE_NAME, A.COLUMN_NAME, A.ORDINAL_POSITION, 
                       B.MAPPED_TAG, B.ENCR_KEY, B.ENCR_TWEAK, B.ALPHABET
                FROM CTE_BUILD_TBL A
                LEFT JOIN GET_DISTINCT_COLS B
                    ON B."SCHEMA" = A.table_schema
                    AND B."TABLE" = A.table_name 
                    AND B."COLUMN" = A.column_name
                ORDER BY A.TABLE_NAME, A.ORDINAL_POSITION ASC
                """, 
                tempTable, METATABLE, srcDatabaseName, srcDatabaseName, srcSchemaName);
            
            session.sql(createTempTable).collect();
        }
        
        private List<TableInfo> getTablesList() {
            String getTablesQuery = String.format("""
                SELECT DISTINCT TABLE_NAME,
                       COUNT(CASE WHEN MAPPED_TAG IS NOT NULL THEN 1 END) as PII_COUNT
                FROM %s 
                GROUP BY TABLE_NAME 
                ORDER BY TABLE_NAME ASC
                """, tempTable);
            
            Row[] rows = session.sql(getTablesQuery).collect();
            
            return Arrays.stream(rows)
                .map(row -> new TableInfo(
                    row.getString(0), 
                    row.getInt(1)
                ))
                .collect(Collectors.toList());
        }
        
        private int processTablesInParallel(List<TableInfo> tables) {
            List<CompletableFuture<Boolean>> futures = new ArrayList<>();
            
            for (TableInfo table : tables) {
                CompletableFuture<Boolean> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        return processTable(table);
                    } catch (Exception e) {
                        logError(table.getName(), e.getMessage());
                        return false;
                    }
                }, executorService);
                
                futures.add(future);
            }
            
            // Wait for all tasks to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            
            // Count successful completions
            return (int) futures.stream()
                .mapToInt(future -> {
                    try {
                        return future.get() ? 1 : 0;
                    } catch (Exception e) {
                        return 0;
                    }
                }).sum();
        }
        
        private boolean processTable(TableInfo tableInfo) {
            String tableName = tableInfo.getName();
            long startTime = System.currentTimeMillis();
            
            try {
                logTableStart(tableName);
                
                if (tableInfo.getPiiCount() == 0) {
                    // Simple clone for tables without PII
                    cloneTable(tableName);
                } else {
                    // Encrypt table with PII data
                    encryptTable(tableName);
                }
                
                long endTime = System.currentTimeMillis();
                logTableCompletion(tableName, startTime, endTime);
                return true;
                
            } catch (Exception e) {
                logError(tableName, e.getMessage());
                return false;
            }
        }
        
        private void cloneTable(String tableName) {
            String cloneQuery = String.format("""
                CREATE OR REPLACE TABLE %s.%s.%s 
                CLONE %s.%s.%s
                """, 
                tgtDatabaseName, tgtSchemaName, tableName,
                srcDatabaseName, srcSchemaName, tableName);
            
            session.sql(cloneQuery).collect();
        }
        
        private void encryptTable(String tableName) {
            List<ColumnInfo> columns = getTableColumns(tableName);
            
            StringBuilder selectClause = new StringBuilder();
            StringBuilder setTagStmt = new StringBuilder();
            
            for (ColumnInfo column : columns) {
                if (selectClause.length() > 0) {
                    selectClause.append(", ");
                }
                
                if (column.getMappedTag() == null) {
                    // Non-PII column - copy as is
                    selectClause.append(column.getColumnName());
                } else {
                    // PII column - encrypt
                    String encryptedColumn = buildEncryptionExpression(column);
                    selectClause.append(encryptedColumn).append(" AS ").append(column.getColumnName());
                    
                    // Build tag statement
                    if (setTagStmt.length() > 0) {
                        setTagStmt.append(", ");
                    }
                    setTagStmt.append(column.getColumnName())
                             .append(" SET TAG ")
                             .append(TAG_DB)
                             .append(column.getMappedTag())
                             .append("=''");
                }
            }
            
            // Create encrypted table
            String createQuery = String.format("""
                CREATE OR REPLACE TABLE %s.%s.%s AS 
                SELECT %s 
                FROM %s.%s.%s
                """, 
                tgtDatabaseName, tgtSchemaName, tableName,
                selectClause.toString(),
                srcDatabaseName, srcSchemaName, tableName);
            
            session.sql(createQuery).collect();
        }
        
        private String buildEncryptionExpression(ColumnInfo column) {
            MaskConfig config = MASK_CONFIGS.getOrDefault(
                column.getAlphabet(), 
                MASK_CONFIGS.get("default")
            );
            
            return String.format("""
                %s(
                    LPAD(
                        regexp_replace(%s, '[^\\x01-\\x7F]', ''),
                        LENGTH(%s) + %d, 
                        '%s'
                    ),
                    '%s',
                    '%s',
                    '%s'
                )
                """,
                ENCRYPTION_FUNCTION,
                column.getColumnName(),
                column.getColumnName(),
                config.getMaskLen(),
                config.getMaskChar(),
                column.getEncrKey(),
                column.getEncrTweak(),
                column.getAlphabet()
            );
        }
        
        private List<ColumnInfo> getTableColumns(String tableName) {
            String query = String.format("""
                SELECT COLUMN_NAME, ORDINAL_POSITION, MAPPED_TAG, ENCR_KEY, ENCR_TWEAK, ALPHABET
                FROM %s 
                WHERE TABLE_NAME = '%s' 
                ORDER BY ORDINAL_POSITION ASC
                """, tempTable, tableName);
            
            Row[] rows = session.sql(query).collect();
            
            return Arrays.stream(rows)
                .map(row -> new ColumnInfo(
                    row.getString(0), // COLUMN_NAME
                    row.getInt(1),    // ORDINAL_POSITION
                    row.getString(2), // MAPPED_TAG
                    row.getString(3), // ENCR_KEY
                    row.getString(4), // ENCR_TWEAK
                    row.getString(5)  // ALPHABET
                ))
                .collect(Collectors.toList());
        }
        
        private void logTableStart(String tableName) {
            String insertQuery = String.format("""
                INSERT INTO %s (TABLE_NAME, STATUS, START_TIME) 
                VALUES ('%s', 'IN PROGRESS', CURRENT_TIMESTAMP())
                """, statusTable, tableName);
            
            session.sql(insertQuery).collect();
        }
        
        private void logTableCompletion(String tableName, long startTime, long endTime) {
            double processingTime = (endTime - startTime) / 1000.0;
            
            String updateQuery = String.format("""
                UPDATE %s 
                SET STATUS = 'COMPLETED', 
                    END_TIME = CURRENT_TIMESTAMP(),
                    PROCESSING_TIME_SECONDS = %.2f
                WHERE TABLE_NAME = '%s'
                """, statusTable, processingTime, tableName);
            
            session.sql(updateQuery).collect();
        }
        
        private void logError(String tableName, String errorMessage) {
            String updateQuery = String.format("""
                UPDATE %s 
                SET STATUS = 'FAILED', 
                    ERROR_MESSAGE = '%s',
                    END_TIME = CURRENT_TIMESTAMP()
                WHERE TABLE_NAME = '%s'
                """, statusTable, errorMessage.replace("'", "''"), tableName);
            
            try {
                session.sql(updateQuery).collect();
            } catch (Exception e) {
                // Log to system if database logging fails
                System.err.println("Failed to log error for table " + tableName + ": " + errorMessage);
            }
        }
        
        private String getCurrentSessionId() {
            Row[] result = session.sql("SELECT CURRENT_SESSION()").collect();
            return result[0].getString(0);
        }
    }
    
    // Data classes
    static class TableInfo {
        private final String name;
        private final int piiCount;
        
        public TableInfo(String name, int piiCount) {
            this.name = name;
            this.piiCount = piiCount;
        }
        
        public String getName() { return name; }
        public int getPiiCount() { return piiCount; }
    }
    
    static class ColumnInfo {
        private final String columnName;
        private final int ordinalPosition;
        private final String mappedTag;
        private final String encrKey;
        private final String encrTweak;
        private final String alphabet;
        
        public ColumnInfo(String columnName, int ordinalPosition, String mappedTag, 
                         String encrKey, String encrTweak, String alphabet) {
            this.columnName = columnName;
            this.ordinalPosition = ordinalPosition;
            this.mappedTag = mappedTag;
            this.encrKey = encrKey;
            this.encrTweak = encrTweak;
            this.alphabet = alphabet;
        }
        
        public String getColumnName() { return columnName; }
        public int getOrdinalPosition() { return ordinalPosition; }
        public String getMappedTag() { return mappedTag; }
        public String getEncrKey() { return encrKey; }
        public String getEncrTweak() { return encrTweak; }
        public String getAlphabet() { return alphabet; }
    }
    
    static class MaskConfig {
        private final String maskChar;
        private final int maskLen;
        private final String trimPadTemplate;
        
        public MaskConfig(String maskChar, int maskLen, String trimPadTemplate) {
            this.maskChar = maskChar;
            this.maskLen = maskLen;
            this.trimPadTemplate = trimPadTemplate;
        }
        
        public String getMaskChar() { return maskChar; }
        public int getMaskLen() { return maskLen; }
        public String getTrimPadTemplate() { return trimPadTemplate; }
    }
}
$$;