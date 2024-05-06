/* This is JDBC version of thrift file - stripped of internal fields */

namespace java com.databricks.jdbc.client.impl.thrift.generated
enum TProtocolVersion {
  __HIVE_JDBC_WORKAROUND = -7
  __TEST_PROTOCOL_VERSION = 0xFF01
  HIVE_CLI_SERVICE_PROTOCOL_V1 = 0
  HIVE_CLI_SERVICE_PROTOCOL_V2 = 1
  HIVE_CLI_SERVICE_PROTOCOL_V3 = 2
  HIVE_CLI_SERVICE_PROTOCOL_V4 = 3
  HIVE_CLI_SERVICE_PROTOCOL_V5 = 4
  HIVE_CLI_SERVICE_PROTOCOL_V6 = 5
  HIVE_CLI_SERVICE_PROTOCOL_V7 = 6
  HIVE_CLI_SERVICE_PROTOCOL_V8 = 7
  HIVE_CLI_SERVICE_PROTOCOL_V9 = 8
  HIVE_CLI_SERVICE_PROTOCOL_V10 = 9
  SPARK_CLI_SERVICE_PROTOCOL_V1 = 0xA501
  SPARK_CLI_SERVICE_PROTOCOL_V2 = 0xA502
  SPARK_CLI_SERVICE_PROTOCOL_V3 = 0xA503
  SPARK_CLI_SERVICE_PROTOCOL_V4 = 0xA504
  SPARK_CLI_SERVICE_PROTOCOL_V5 = 0xA505
  SPARK_CLI_SERVICE_PROTOCOL_V6 = 0xA506
  SPARK_CLI_SERVICE_PROTOCOL_V7 = 0xA507
  SPARK_CLI_SERVICE_PROTOCOL_V8 = 0xA508
  SPARK_CLI_SERVICE_PROTOCOL_V9 = 0xA509
}
enum TTypeId {
  BOOLEAN_TYPE,
  TINYINT_TYPE,
  SMALLINT_TYPE,
  INT_TYPE,
  BIGINT_TYPE,
  FLOAT_TYPE,
  DOUBLE_TYPE,
  STRING_TYPE,
  TIMESTAMP_TYPE,
  BINARY_TYPE,
  ARRAY_TYPE,
  MAP_TYPE,
  STRUCT_TYPE,
  UNION_TYPE,
  USER_DEFINED_TYPE,
  DECIMAL_TYPE,
  NULL_TYPE,
  DATE_TYPE,
  VARCHAR_TYPE,
  CHAR_TYPE,
  INTERVAL_YEAR_MONTH_TYPE,
  INTERVAL_DAY_TIME_TYPE
}
const set<TTypeId> PRIMITIVE_TYPES = [
  TTypeId.BOOLEAN_TYPE,
  TTypeId.TINYINT_TYPE,
  TTypeId.SMALLINT_TYPE,
  TTypeId.INT_TYPE,
  TTypeId.BIGINT_TYPE,
  TTypeId.FLOAT_TYPE,
  TTypeId.DOUBLE_TYPE,
  TTypeId.STRING_TYPE,
  TTypeId.TIMESTAMP_TYPE,
  TTypeId.BINARY_TYPE,
  TTypeId.DECIMAL_TYPE,
  TTypeId.NULL_TYPE,
  TTypeId.DATE_TYPE,
  TTypeId.VARCHAR_TYPE,
  TTypeId.CHAR_TYPE,
  TTypeId.INTERVAL_YEAR_MONTH_TYPE,
  TTypeId.INTERVAL_DAY_TIME_TYPE
]
const set<TTypeId> COMPLEX_TYPES = [
  TTypeId.ARRAY_TYPE
  TTypeId.MAP_TYPE
  TTypeId.STRUCT_TYPE
  TTypeId.UNION_TYPE
  TTypeId.USER_DEFINED_TYPE
]
const set<TTypeId> COLLECTION_TYPES = [
  TTypeId.ARRAY_TYPE
  TTypeId.MAP_TYPE
]
const map<TTypeId,string> TYPE_NAMES = {
  TTypeId.BOOLEAN_TYPE: "BOOLEAN",
  TTypeId.TINYINT_TYPE: "TINYINT",
  TTypeId.SMALLINT_TYPE: "SMALLINT",
  TTypeId.INT_TYPE: "INT",
  TTypeId.BIGINT_TYPE: "BIGINT",
  TTypeId.FLOAT_TYPE: "FLOAT",
  TTypeId.DOUBLE_TYPE: "DOUBLE",
  TTypeId.STRING_TYPE: "STRING",
  TTypeId.TIMESTAMP_TYPE: "TIMESTAMP",
  TTypeId.BINARY_TYPE: "BINARY",
  TTypeId.ARRAY_TYPE: "ARRAY",
  TTypeId.MAP_TYPE: "MAP",
  TTypeId.STRUCT_TYPE: "STRUCT",
  TTypeId.UNION_TYPE: "UNIONTYPE",
  TTypeId.DECIMAL_TYPE: "DECIMAL",
  TTypeId.NULL_TYPE: "NULL"
  TTypeId.DATE_TYPE: "DATE"
  TTypeId.VARCHAR_TYPE: "VARCHAR"
  TTypeId.CHAR_TYPE: "CHAR"
  TTypeId.INTERVAL_YEAR_MONTH_TYPE: "INTERVAL_YEAR_MONTH"
  TTypeId.INTERVAL_DAY_TIME_TYPE: "INTERVAL_DAY_TIME"
}
typedef i32 TTypeEntryPtr
const string CHARACTER_MAXIMUM_LENGTH = "characterMaximumLength"
const string PRECISION = "precision"
const string SCALE = "scale"
union TTypeQualifierValue {
  1: optional i32 i32Value
  2: optional string stringValue
}
struct TTypeQualifiers {
  1: required map <string, TTypeQualifierValue> qualifiers
}
struct TPrimitiveTypeEntry {
  1: required TTypeId type
  2: optional TTypeQualifiers typeQualifiers
}
struct TArrayTypeEntry {
  1: required TTypeEntryPtr objectTypePtr
}
struct TMapTypeEntry {
  1: required TTypeEntryPtr keyTypePtr
  2: required TTypeEntryPtr valueTypePtr
}
struct TStructTypeEntry {
  1: required map<string, TTypeEntryPtr> nameToTypePtr
}
struct TUnionTypeEntry {
  1: required map<string, TTypeEntryPtr> nameToTypePtr
}
struct TUserDefinedTypeEntry {
  1: required string typeClassName
}
union TTypeEntry {
  1: TPrimitiveTypeEntry primitiveEntry
  2: TArrayTypeEntry arrayEntry
  3: TMapTypeEntry mapEntry
  4: TStructTypeEntry structEntry
  5: TUnionTypeEntry unionEntry
  6: TUserDefinedTypeEntry userDefinedTypeEntry
}
struct TTypeDesc {
  1: required list<TTypeEntry> types
}
struct TColumnDesc {
  1: required string columnName
  2: required TTypeDesc typeDesc
  3: required i32 position
  4: optional string comment
}
struct TTableSchema {
  1: required list<TColumnDesc> columns
}
struct TBoolValue {
  1: optional bool value
}
struct TByteValue {
  1: optional byte value
}
struct TI16Value {
  1: optional i16 value
}
struct TI32Value {
  1: optional i32 value
}
struct TI64Value {
  1: optional i64 value
}
struct TDoubleValue {
  1: optional double value
}
struct TStringValue {
  1: optional string value
}
union TColumnValue {
  1: TBoolValue   boolVal      
  2: TByteValue   byteVal      
  3: TI16Value    i16Val       
  4: TI32Value    i32Val       
  5: TI64Value    i64Val       
  6: TDoubleValue doubleVal    
  7: TStringValue stringVal    
}
struct TRow {
  1: required list<TColumnValue> colVals
}
struct TBoolColumn {
  1: required list<bool> values
  2: required binary nulls
}
struct TByteColumn {
  1: required list<byte> values
  2: required binary nulls
}
struct TI16Column {
  1: required list<i16> values
  2: required binary nulls
}
struct TI32Column {
  1: required list<i32> values
  2: required binary nulls
}
struct TI64Column {
  1: required list<i64> values
  2: required binary nulls
}
struct TDoubleColumn {
  1: required list<double> values
  2: required binary nulls
}
struct TStringColumn {
  1: required list<string> values
  2: required binary nulls
}
struct TBinaryColumn {
  1: required list<binary> values
  2: required binary nulls
}
union TColumn {
  1: TBoolColumn   boolVal      
  2: TByteColumn   byteVal      
  3: TI16Column    i16Val       
  4: TI32Column    i32Val       
  5: TI64Column    i64Val       
  6: TDoubleColumn doubleVal    
  7: TStringColumn stringVal    
  8: TBinaryColumn binaryVal    
}
enum TSparkRowSetType {
  ARROW_BASED_SET,
  COLUMN_BASED_SET,
  ROW_BASED_SET,
  URL_BASED_SET
}
enum TDBSqlCompressionCodec {
  NONE,
  LZ4_FRAME,
  LZ4_BLOCK
}
struct TDBSqlJsonArrayFormat {
  1: optional TDBSqlCompressionCodec compressionCodec
}
struct TDBSqlCsvFormat {
  1: optional TDBSqlCompressionCodec compressionCodec
}
enum TDBSqlArrowLayout {
  ARROW_BATCH, 
  ARROW_STREAMING 
}
struct TDBSqlArrowFormat {
  1: optional TDBSqlArrowLayout arrowLayout
  2: optional TDBSqlCompressionCodec compressionCodec
}
union TDBSqlResultFormat {
  1: TDBSqlArrowFormat arrowFormat
  2: TDBSqlCsvFormat csvFormat
  3: TDBSqlJsonArrayFormat jsonArrayFormat
}
struct TSparkArrowBatch {
  1: required binary batch
  2: required i64 rowCount
}
struct TSparkArrowResultLink {
  1: required string fileLink
  2: required i64 expiryTime
  3: required i64 startRowOffset
  4: required i64 rowCount
  5: required i64 bytesNum
  6: optional map<string, string> httpHeaders
}
struct TDBSqlCloudResultFile {
  1: optional string filePath
  2: optional i64 startRowOffset
  3: optional i64 rowCount
  4: optional i64 uncompressedBytes
  5: optional i64 compressedBytes
  6: optional string fileLink
  7: optional i64 linkExpiryTime
  8: optional map<string, string> httpHeaders
}
struct TRowSet {
  1: required i64 startRowOffset
  2: required list<TRow> rows
  3: optional list<TColumn> columns
  4: optional binary binaryColumns
  5: optional i32 columnCount
  0x501: optional list<TSparkArrowBatch> arrowBatches;
  0x502: optional list<TSparkArrowResultLink> resultLinks;
}
struct TDBSqlTempView {
  1: optional string name
  2: optional string sqlStatement
  3: optional map<string, string> properties
  4: optional string viewSchema
}
struct TDBSqlSessionCapabilities {
  1: optional bool supportsMultipleCatalogs
}
struct TExpressionInfo {
    1: optional string className
    2: optional string usage
    3: optional string name
    4: optional string extended
    5: optional string db
    6: optional string arguments
    7: optional string examples
    8: optional string note
    9: optional string group
    10: optional string since
    11: optional string deprecated
    12: optional string source
}
struct TDBSqlConfValue {
  1: optional string value
}
struct TSQLVariable {
  1: optional TIdentifier catalogName
  2: optional TIdentifier schemaName
  3: optional TIdentifier variableName
  4: optional string defaultExpressionSQL
  5: optional string variableDataType
  6: optional string currentValue
}
struct TDBSqlSessionConf {
  1: optional map<string, string> confs
  2: optional list<TDBSqlTempView> tempViews
  3: optional string currentDatabase
  4: optional string currentCatalog
  5: optional TDBSqlSessionCapabilities sessionCapabilities
  6: optional list<TExpressionInfo> expressionsInfos
  7: optional map<string, TDBSqlConfValue> internalConfs
  8: optional list<TSQLVariable> tempVariables
}
enum TOperationIdempotencyType {
  UNKNOWN,
  NON_IDEMPOTENT,
  IDEMPOTENT
}
enum TOperationTimeoutLevel {
  CLUSTER,
  SESSION
}
enum TStatusCode {
  SUCCESS_STATUS,
  SUCCESS_WITH_INFO_STATUS,
  STILL_EXECUTING_STATUS,
  ERROR_STATUS,
  INVALID_HANDLE_STATUS
}
struct TStatus {
  1: required TStatusCode statusCode
  2: optional list<string> infoMessages
  3: optional string sqlState  
  4: optional i32 errorCode    
  5: optional string errorMessage
  6: optional string displayMessage
  0x501: optional string errorDetailsJson
}
enum TOperationState {
  INITIALIZED_STATE,
  RUNNING_STATE,
  FINISHED_STATE,
  CANCELED_STATE,
  CLOSED_STATE,
  ERROR_STATE,
  UKNOWN_STATE,
  PENDING_STATE,
  TIMEDOUT_STATE,
}
typedef string TIdentifier
typedef string TPattern
typedef string TPatternOrIdentifier
struct TNamespace {
  1: optional TIdentifier catalogName
  2: optional TIdentifier schemaName
}
struct THandleIdentifier {
  1: required binary guid,
  2: required binary secret,
}
struct TSessionHandle {
  1: required THandleIdentifier sessionId
}
enum TOperationType {
  EXECUTE_STATEMENT,
  GET_TYPE_INFO,
  GET_CATALOGS,
  GET_SCHEMAS,
  GET_TABLES,
  GET_TABLE_TYPES,
  GET_COLUMNS,
  GET_FUNCTIONS,
  UNKNOWN,
}
struct TOperationHandle {
  1: required THandleIdentifier operationId
  2: required TOperationType operationType
  3: required bool hasResultSet
  4: optional double modifiedRowCount
}
struct TOpenSessionReq {
  1: optional TProtocolVersion client_protocol = TProtocolVersion.__HIVE_JDBC_WORKAROUND
  2: optional string username
  3: optional string password
  4: optional map<string, string> configuration
  0x501: optional list<TGetInfoType> getInfos
  0x502: optional i64 client_protocol_i64
  0x503: optional map<string, string> connectionProperties
  0x504: optional TNamespace initialNamespace
  0x505: optional bool canUseMultipleCatalogs
}
struct TOpenSessionResp {
  1: required TStatus status
  2: required TProtocolVersion serverProtocolVersion
  3: optional TSessionHandle sessionHandle
  4: optional map<string, string> configuration
  0x504: optional TNamespace initialNamespace
  0x505: optional bool canUseMultipleCatalogs
  0x501: optional list<TGetInfoValue> getInfos
}
struct TCloseSessionReq {
  1: required TSessionHandle sessionHandle
}
struct TCloseSessionResp {
  1: required TStatus status
}
enum TGetInfoType {
  CLI_MAX_DRIVER_CONNECTIONS =           0,
  CLI_MAX_CONCURRENT_ACTIVITIES =        1,
  CLI_DATA_SOURCE_NAME =                 2,
  CLI_FETCH_DIRECTION =                  8,
  CLI_SERVER_NAME =                      13,
  CLI_SEARCH_PATTERN_ESCAPE =            14,
  CLI_DBMS_NAME =                        17,
  CLI_DBMS_VER =                         18,
  CLI_ACCESSIBLE_TABLES =                19,
  CLI_ACCESSIBLE_PROCEDURES =            20,
  CLI_CURSOR_COMMIT_BEHAVIOR =           23,
  CLI_DATA_SOURCE_READ_ONLY =            25,
  CLI_DEFAULT_TXN_ISOLATION =            26,
  CLI_IDENTIFIER_CASE =                  28,
  CLI_IDENTIFIER_QUOTE_CHAR =            29,
  CLI_MAX_COLUMN_NAME_LEN =              30,
  CLI_MAX_CURSOR_NAME_LEN =              31,
  CLI_MAX_SCHEMA_NAME_LEN =              32,
  CLI_MAX_CATALOG_NAME_LEN =             34,
  CLI_MAX_TABLE_NAME_LEN =               35,
  CLI_SCROLL_CONCURRENCY =               43,
  CLI_TXN_CAPABLE =                      46,
  CLI_USER_NAME =                        47,
  CLI_TXN_ISOLATION_OPTION =             72,
  CLI_INTEGRITY =                        73,
  CLI_GETDATA_EXTENSIONS =               81,
  CLI_NULL_COLLATION =                   85,
  CLI_ALTER_TABLE =                      86,
  CLI_ORDER_BY_COLUMNS_IN_SELECT =       90,
  CLI_SPECIAL_CHARACTERS =               94,
  CLI_MAX_COLUMNS_IN_GROUP_BY =          97,
  CLI_MAX_COLUMNS_IN_INDEX =             98,
  CLI_MAX_COLUMNS_IN_ORDER_BY =          99,
  CLI_MAX_COLUMNS_IN_SELECT =            100,
  CLI_MAX_COLUMNS_IN_TABLE =             101,
  CLI_MAX_INDEX_SIZE =                   102,
  CLI_MAX_ROW_SIZE =                     104,
  CLI_MAX_STATEMENT_LEN =                105,
  CLI_MAX_TABLES_IN_SELECT =             106,
  CLI_MAX_USER_NAME_LEN =                107,
  CLI_OJ_CAPABILITIES =                  115,
  CLI_XOPEN_CLI_YEAR =                   10000,
  CLI_CURSOR_SENSITIVITY =               10001,
  CLI_DESCRIBE_PARAMETER =               10002,
  CLI_CATALOG_NAME =                     10003,
  CLI_COLLATION_SEQ =                    10004,
  CLI_MAX_IDENTIFIER_LEN =               10005,
}
union TGetInfoValue {
  1: string stringValue
  2: i16 smallIntValue
  3: i32 integerBitmask
  4: i32 integerFlag
  5: i32 binaryValue
  6: i64 lenValue
}
struct TGetInfoReq {
  1: required TSessionHandle sessionHandle
  2: required TGetInfoType infoType
}
struct TGetInfoResp {
  1: required TStatus status
  2: required TGetInfoValue infoValue
}
struct TSparkGetDirectResults {
  1: required i64 maxRows
  2: optional i64 maxBytes
}
struct TSparkDirectResults {
  1: optional TGetOperationStatusResp operationStatus
  2: optional TGetResultSetMetadataResp resultSetMetadata
  3: optional TFetchResultsResp resultSet
  4: optional TCloseOperationResp closeOperation
}
struct TSparkArrowTypes {
 1: optional bool timestampAsArrow
 2: optional bool decimalAsArrow
 3: optional bool complexTypesAsArrow
 4: optional bool intervalTypesAsArrow
 5: optional bool nullTypeAsArrow
}
enum TResultPersistenceMode {
  ONLY_LARGE_RESULTS,
  ALL_QUERY_RESULTS,
  ALL_RESULTS
}
struct TExecuteStatementReq {
  1: required TSessionHandle sessionHandle
  2: required string statement
  3: optional map<string, string> confOverlay
  (deprecated)
  4: optional bool runAsync = false
  0x501: optional TSparkGetDirectResults getDirectResults
  5: optional i64 queryTimeout = 0
  0x502: optional bool canReadArrowResult
  0x503: optional bool canDownloadResult
  0x504: optional bool canDecompressLZ4Result
  0x505: optional i64 maxBytesPerFile
  0x506: optional TSparkArrowTypes useArrowNativeTypes
  0x507: optional i64 resultRowLimit
  0x508: optional TSparkParameterList parameters
  0x509: optional i64 maxBytesPerBatch
  0x510: optional TStatementConf statementConf
  0xD07: optional TResultPersistenceMode resultPersistenceMode
  0xD10: optional bool enforceResultPersistenceMode
}
struct TDBSqlStatement {
  1: optional string statement
}
union TSparkParameterValue {
  1: string stringValue
  2: double doubleValue
  3: bool booleanValue
}
struct TSparkParameter {
  1: optional i32 ordinal
  2: optional string name
  3: optional string type
  4: optional TSparkParameterValue value
}
typedef list<TSparkParameter> TSparkParameterList
struct TStatementConf {
  1: optional bool sessionless
  2: optional TNamespace initialNamespace
  3: optional TProtocolVersion client_protocol
  4: optional i64 client_protocol_i64
}
struct TExecuteStatementResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetTypeInfoReq {
  1: required TSessionHandle sessionHandle
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetTypeInfoResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetCatalogsReq {
  1: required TSessionHandle sessionHandle
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetCatalogsResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetSchemasReq {
  1: required TSessionHandle sessionHandle
  2: optional TIdentifier catalogName
  3: optional TPatternOrIdentifier schemaName
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetSchemasResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetTablesReq {
  1: required TSessionHandle sessionHandle
  2: optional TPatternOrIdentifier catalogName
  3: optional TPatternOrIdentifier schemaName
  4: optional TPatternOrIdentifier tableName
  5: optional list<string> tableTypes
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetTablesResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetTableTypesReq {
  1: required TSessionHandle sessionHandle
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetTableTypesResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetColumnsReq {
  1: required TSessionHandle sessionHandle
  2: optional TIdentifier catalogName
  3: optional TPatternOrIdentifier schemaName
  4: optional TPatternOrIdentifier tableName
  5: optional TPatternOrIdentifier columnName
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetColumnsResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetFunctionsReq {
  1: required TSessionHandle sessionHandle
  2: optional TIdentifier catalogName
  3: optional TPatternOrIdentifier schemaName
  4: required TPatternOrIdentifier functionName
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetFunctionsResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetPrimaryKeysReq {
  1: required TSessionHandle sessionHandle
  2: optional TIdentifier catalogName
  3: optional TIdentifier schemaName
  4: optional TIdentifier tableName
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetPrimaryKeysResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetCrossReferenceReq {
  1: required TSessionHandle sessionHandle
  2: optional TIdentifier parentCatalogName
  3: optional TIdentifier parentSchemaName
  4: optional TIdentifier parentTableName
  5: optional TIdentifier foreignCatalogName
  6: optional TIdentifier foreignSchemaName
  7: optional TIdentifier foreignTableName
  0x501: optional TSparkGetDirectResults getDirectResults
  0x502: optional bool runAsync = false
}
struct TGetCrossReferenceResp {
  1: required TStatus status
  2: optional TOperationHandle operationHandle
  0x501: optional TSparkDirectResults directResults
}
struct TGetOperationStatusReq {
  1: required TOperationHandle operationHandle
  2: optional bool getProgressUpdate
}
struct TGetOperationStatusResp {
  1: required TStatus status
  2: optional TOperationState operationState
  3: optional string sqlState
  4: optional i32 errorCode
  
  5: optional string errorMessage
  (deprecated)
  6: optional string taskStatus
  7: optional i64 operationStarted
  8: optional i64 operationCompleted
  9: optional bool hasResultSet
  10: optional TProgressUpdateResp progressUpdateResponse
  11: optional i64 numModifiedRows
  0x501: optional string displayMessage
  0x502: optional string diagnosticInfo
  0x503: optional string errorDetailsJson
}
struct TCancelOperationReq {
  1: required TOperationHandle operationHandle
}
struct TCancelOperationResp {
  1: required TStatus status
}
enum TDBSqlCloseOperationReason {
    NONE,
    COMMAND_INACTIVITY_TIMEOUT,
    CLOSE_SESSION
}
struct TCloseOperationReq {
  1: required TOperationHandle operationHandle
}
struct TCloseOperationResp {
  1: required TStatus status
}
struct TGetResultSetMetadataReq {
  1: required TOperationHandle operationHandle
}
struct TGetResultSetMetadataResp {
  1: required TStatus status
  2: optional TTableSchema schema
  0x501: optional TSparkRowSetType resultFormat;
  0x502: optional bool lz4Compressed;
  0x503: optional binary arrowSchema
  0x504: optional TCacheLookupResult cacheLookupResult
  0x505: optional i64 uncompressedBytes;
  0x506: optional i64 compressedBytes;
  0x507: optional bool isStagingOperation;
}
enum TCacheLookupResult {
    CACHE_INELIGIBLE,
    LOCAL_CACHE_HIT,
    REMOTE_CACHE_HIT,
    CACHE_MISS
}
enum TCloudFetchDisabledReason {
    ARROW_SUPPORT,
    CLOUD_FETCH_SUPPORT,
    PROTOCOL_VERSION,
    REGION_SUPPORT,
    BLOCKLISTED_OPERATION,
    SMALL_RESULT_SIZE,
    CUSTOMER_STORAGE_SUPPORT,
    UNKNOWN,
    METADATA_OPERATION
}
enum TDBSqlManifestFileFormat {
    THRIFT_GET_RESULT_SET_METADATA_RESP
}
enum TFetchOrientation {
  FETCH_NEXT,
  FETCH_PRIOR,
  FETCH_RELATIVE,
  FETCH_ABSOLUTE,
  FETCH_FIRST,
  FETCH_LAST
}
enum TDBSqlFetchDisposition {
  DISPOSITION_UNSPECIFIED,
  DISPOSITION_INLINE,
  DISPOSITION_EXTERNAL_LINKS,
  DISPOSITION_INTERNAL_DBFS
}
struct TFetchResultsReq {
  1: required TOperationHandle operationHandle
  2: required TFetchOrientation orientation = TFetchOrientation.FETCH_NEXT
  3: required i64 maxRows
  4: optional i16 fetchType = 0
  0x501: optional i64 maxBytes
  0x502: optional i64 startRowOffset
  0x503: optional bool includeResultSetMetadata
}
struct TFetchResultsResp {
  1: required TStatus status
  2: optional bool hasMoreRows
  3: optional TRowSet results
  0x501: optional TGetResultSetMetadataResp resultSetMetadata
}
struct  TGetDelegationTokenReq {
  1: required TSessionHandle sessionHandle
  2: required string owner
  3: required string renewer
}
struct TGetDelegationTokenResp {
  1: required TStatus status
  2: optional string delegationToken
}
struct TCancelDelegationTokenReq {
  1: required TSessionHandle sessionHandle
  2: required string delegationToken
}
struct TCancelDelegationTokenResp {
  1: required TStatus status
}
struct TRenewDelegationTokenReq {
  1: required TSessionHandle sessionHandle
  2: required string delegationToken
}
struct TRenewDelegationTokenResp {
  1: required TStatus status
}
enum TJobExecutionStatus {
    IN_PROGRESS,
    COMPLETE,
    NOT_AVAILABLE
}
struct TProgressUpdateResp {
  1: required list<string> headerNames
  2: required list<list<string>> rows
  3: required double progressedPercentage
  4: required TJobExecutionStatus status
  5: required string footerSummary
  6: required i64 startTime
}
service TCLIService {
  TOpenSessionResp OpenSession(1:TOpenSessionReq req);
  TCloseSessionResp CloseSession(1:TCloseSessionReq req);
  TGetInfoResp GetInfo(1:TGetInfoReq req);
  TExecuteStatementResp ExecuteStatement(1:TExecuteStatementReq req);
  TGetTypeInfoResp GetTypeInfo(1:TGetTypeInfoReq req);
  TGetCatalogsResp GetCatalogs(1:TGetCatalogsReq req);
  TGetSchemasResp GetSchemas(1:TGetSchemasReq req);
  TGetTablesResp GetTables(1:TGetTablesReq req);
  TGetTableTypesResp GetTableTypes(1:TGetTableTypesReq req);
  TGetColumnsResp GetColumns(1:TGetColumnsReq req);
  TGetFunctionsResp GetFunctions(1:TGetFunctionsReq req);
  TGetPrimaryKeysResp GetPrimaryKeys(1:TGetPrimaryKeysReq req);
  TGetCrossReferenceResp GetCrossReference(1:TGetCrossReferenceReq req);
  TGetOperationStatusResp GetOperationStatus(1:TGetOperationStatusReq req);
  TCancelOperationResp CancelOperation(1:TCancelOperationReq req);
  TCloseOperationResp CloseOperation(1:TCloseOperationReq req);
  TGetResultSetMetadataResp GetResultSetMetadata(1:TGetResultSetMetadataReq req);
  TFetchResultsResp FetchResults(1:TFetchResultsReq req);
  TGetDelegationTokenResp GetDelegationToken(1:TGetDelegationTokenReq req);
  TCancelDelegationTokenResp CancelDelegationToken(1:TCancelDelegationTokenReq req);
  TRenewDelegationTokenResp RenewDelegationToken(1:TRenewDelegationTokenReq req);
}
