
CREATE PROCEDURE [dv_release].[dv_build_release_config_table]
(
  @vault_config_table_schema	varchar(128)	= NULL
, @vault_config_table_name		varchar(128)	= NULL
, @vault_release_number			int				= NULL
, @vault_exclude_columnsCSV		nvarchar(4000)	= null
, @vault_statement				nvarchar(max)	output
, @vault_change_count			int				output
, @DoGenerateError              bit				= 0
, @DoThrowError                 bit				= 1
)

AS
BEGIN
SET NOCOUNT ON

/*========================================================================================================================
Description:	This script generates a MERGE statement for the table provided, for a chosen Release Number, using the rows currently in the table.
				The MERGE statement is in a table variable.
Reference:		Based on code published by Eitan Blumin, Madeira SQL Server Services (http://www.madeira.co.il/generate-merge-statements-for-your-tables/)

Notes:			Tables without a primary key are not supported.
				Columns of the image data type may show conversion errors. This has yet to be resolved.
				This script can be used in test environments to capture a snapshot of a table before a test is performed,
				which changes the data in the table. Once the test is completed, the MERGE statement can be executed
				in order to bring the table back to its state before the test.
=========================================================================================================================*/

-- Internal use variables
DECLARE @ExcludeColumns table(column_name sysname)
insert @ExcludeColumns values ('version_number'), ('updated_by') ,('update_date_time')
if isnull(@vault_exclude_columnsCSV, '') <> ''
	insert @ExcludeColumns SELECT * FROM [dbo].[fn_split_strings] (@vault_exclude_columnsCSV, ',')

DECLARE
	@delete_unmatched_rows	BIT = 0,	-- enable/disable DELETION of rows
	@debug_mode				BIT = 0,	-- enable/disable debug mode
	@include_timestamp		BIT = 0,	-- include timestamp columns or not
	@omit_computed_cols		BIT = 1,	-- omit computed columns or not (in case target table doesn't have computed columns)
	@top_clause				NVARCHAR(4000)	= N'TOP 100 PERCENT' , -- you can use this to limit number of generated rows (e.g. TOP 200)
	@MergeStmnt				NVARCHAR(MAX),	
	@CurrColumnId			INT,
	@CurrColumnName			SYSNAME,
	@CurrColumnType			VARCHAR(1000),
	@ColumnList				NVARCHAR(MAX),
	@UpdateSet				NVARCHAR(MAX),
	@PKJoinClause			NVARCHAR(MAX),
	@HasIdentity			BIT,
	@GetValues				NVARCHAR(MAX),
	@Values					NVARCHAR(MAX),
	@release_key			int,
	@change_count			int = 0,
	@currtable				SYSNAME,
	@currschema				SYSNAME,
	@release_number			int

-- Log4TSQL Journal Constants 
DECLARE @SEVERITY_CRITICAL      smallint = 1;
DECLARE @SEVERITY_SEVERE        smallint = 2;
DECLARE @SEVERITY_MAJOR         smallint = 4;
DECLARE @SEVERITY_MODERATE      smallint = 8;
DECLARE @SEVERITY_MINOR         smallint = 16;
DECLARE @SEVERITY_CONCURRENCY   smallint = 32;
DECLARE @SEVERITY_INFORMATION   smallint = 256;
DECLARE @SEVERITY_SUCCESS       smallint = 512;
DECLARE @SEVERITY_DEBUG         smallint = 1024;
DECLARE @NEW_LINE               char(1)  = CHAR(10);

-- Log4TSQL Standard/ExceptionHandler variables
DECLARE	  @_Error         int
		, @_RowCount      int
		, @_Step          varchar(128)
		, @_Message       nvarchar(512)
		, @_ErrorContext  nvarchar(512)

-- Log4TSQL JournalWriter variables
DECLARE   @_FunctionName			varchar(255)
		, @_SprocStartTime			datetime
		, @_JournalOnOff			varchar(3)
		, @_Severity				smallint
		, @_ExceptionId				int
		, @_StepStartTime			datetime
		, @_ProgressText			nvarchar(max)

SET @_Error             = 0;
SET @_FunctionName      = OBJECT_NAME(@@PROCID);
SET @_Severity          = @SEVERITY_INFORMATION;
SET @_SprocStartTime    = sysdatetimeoffset();
SET @_ProgressText      = '' 
SET @_JournalOnOff      = log4.GetJournalControl(@_FunctionName, 'HOWTO');  -- left Group Name as HOWTO for now.

-- set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_config_table_schema    : ' + COALESCE(@vault_config_table_schema, '<NULL>')
						+ @NEW_LINE + '    @vault_config_table_name      : ' + COALESCE(@vault_config_table_name, '<NULL>')
						+ @NEW_LINE + '    @vault_release_number         : ' + COALESCE(cast(@vault_release_number as varchar(20)), '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError              : ' + COALESCE(CAST(@DoGenerateError AS varchar), '<NULL>')
						+ @NEW_LINE + '    @DoThrowError                 : ' + COALESCE(CAST(@DoThrowError AS varchar), '<NULL>')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';

SET @_Step = 'Initialise Variables';

SELECT
	@CurrColumnId	= NULL,
	@CurrColumnName = NULL,
	@CurrColumnType = NULL,
	@MergeStmnt		= NULL,
	@ColumnList		= NULL,
	@UpdateSet		= NULL,
	@PKJoinClause	= NULL,
	@GetValues		= NULL,
	@Values			= NULL,
	@HasIdentity	= 0

SELECT
    @currtable		=	@vault_config_table_name,
	@currschema		=	@vault_config_table_schema,
	@release_number	=	@vault_release_number 
			
SET @_Step = 'Get Release Header';
select @release_key = release_key from [dv_release].[dv_release_master] where release_number = @release_number

-- Find the table's Primary Key column(s) to build a JOIN clause

SELECT 
	@PKJoinClause = ISNULL(@PKJoinClause + N'
AND ',N'') + 'trgt.' + QUOTENAME(col.name) + N' = src.' + QUOTENAME(col.name)
FROM
	sys.indexes AS ind
INNER JOIN 
	sys.index_columns AS indcol
ON
	ind.object_id = indcol.object_id
AND ind.index_id = indcol.index_id
INNER JOIN
	sys.columns AS col
ON
	ind.object_id = col.object_id
AND indcol.column_id = col.column_id
WHERE 
	ind.is_primary_key = 1
AND ind.object_id = OBJECT_ID(QUOTENAME(@CurrSchema) + '.' + QUOTENAME(@CurrTable))

IF @debug_mode = 1
	PRINT 'PK Join Clause:
' + @PKJoinClause


-- If nothing found, abort (table is not supported)

IF @PKJoinClause IS NULL
BEGIN
	RAISERROR(N'ERROR: Table %s is not supported because it''s missing a Primary Key.', 16, 1, @CurrTable) WITH NOWAIT;
	GOTO Quit;
END


SET @_Step = 'Get the first column ID'

SELECT
	@CurrColumnId = MIN(ORDINAL_POSITION) 	
FROM
	INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
WHERE
	TABLE_NAME = @CurrTable
AND TABLE_SCHEMA = @CurrSchema


SET @_Step = 'Loop through all the columns'

WHILE @CurrColumnId IS NOT NULL
BEGIN
	SELECT
		@CurrColumnName = QUOTENAME(COLUMN_NAME), 
		@CurrColumnType = DATA_TYPE 
	FROM
		INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
	WHERE
		ORDINAL_POSITION = @CurrColumnId
	AND TABLE_NAME = @CurrTable
	AND TABLE_SCHEMA = @CurrSchema
	

	IF @debug_mode = 1
		PRINT 'Processing column ' + @CurrColumnName

	
	-- Making sure whether to output computed columns or not
IF @CurrColumnName IN (SELECT QUOTENAME(column_name) from @ExcludeColumns)
	BEGIN
		GOTO SKIP_COLUMN
	END

IF @omit_computed_cols = 1
	BEGIN
		IF (SELECT COLUMNPROPERTY( OBJECT_ID(QUOTENAME(@CurrSchema) + '.' + @CurrTable),SUBSTRING(@CurrColumnName,2,LEN(@CurrColumnName) - 2),'IsComputed')) = 1 
		BEGIN
			GOTO SKIP_COLUMN					
		END
	END
	

	-- Concatenate column value selection to the values list

SET @GetValues = ISNULL( @GetValues + ' + '',''' , '''(''' ) + ' + ' +
	CASE
		-- Format column value retrieval based on its data type
		WHEN @CurrColumnType IN ('text','char','varchar') 
			THEN 
				'COALESCE('''''''' + REPLACE(CONVERT(nvarchar(max),' + @CurrColumnName + '),'''''''','''''''''''')+'''''''',''NULL'')'					
		WHEN @CurrColumnType IN ('ntext','nchar','nvarchar','xml') 
			THEN  
				'COALESCE(''N'''''' + REPLACE(CONVERT(nvarchar(max),' + @CurrColumnName + '),'''''''','''''''''''')+'''''''',''NULL'')'					
		WHEN @CurrColumnType LIKE '%date%'
			THEN 
				'COALESCE('''''''' + RTRIM(CONVERT(varchar(max),' + @CurrColumnName + ',109))+'''''''',''NULL'')'
		WHEN @CurrColumnType IN ('uniqueidentifier') 
			THEN  
				'COALESCE('''''''' + REPLACE(CONVERT(varchar(255),RTRIM(' + @CurrColumnName + ')),'''''''','''''''''''')+'''''''',''NULL'')'
		WHEN @CurrColumnType IN ('binary','varbinary','image') 
			THEN  
				'COALESCE(RTRIM(CONVERT(nvarchar(max),' + @CurrColumnName + ',1)),''NULL'')'  
		WHEN @CurrColumnType IN ('timestamp','rowversion') 
			THEN  
				CASE 
					WHEN @include_timestamp = 0 
						THEN 
							'''DEFAULT''' 
						ELSE 
							'COALESCE(RTRIM(CONVERT(varchar(max),' + 'CONVERT(int,' + @CurrColumnName + '))),''NULL'')'  
				END
		WHEN @CurrColumnType IN ('float','real','money','smallmoney')
			THEN
				'COALESCE(LTRIM(RTRIM(' + 'CONVERT(varchar(max), ' +  @CurrColumnName  + ',2)' + ')),''NULL'')' 
		ELSE 
			'COALESCE(LTRIM(RTRIM(' + 'CONVERT(varchar(max), ' +  @CurrColumnName  + ')' + ')),''NULL'')' 
	END
	

	-- Concatenate column name to column list

	SET @ColumnList = ISNULL(@ColumnList + N',',N'') + @CurrColumnName


	-- Make sure to output SET IDENTITY_INSERT ON/OFF in case the table has an IDENTITY column

	IF (SELECT COLUMNPROPERTY( OBJECT_ID(QUOTENAME(@CurrSchema) + '.' + @CurrTable),SUBSTRING(@CurrColumnName,2,LEN(@CurrColumnName) - 2),'IsIdentity')) = 1 
	BEGIN
		SET @HasIdentity = 1		
	END
	ELSE
	BEGIN
		-- If column is not IDENTITY, concatenate it to UPDATE SET clause
		SET @UpdateSet = ISNULL(@UpdateSet + N'
		, ', N'') + @CurrColumnName + N' = src.' + @CurrColumnName
	END

	
	SKIP_COLUMN: -- The label used in GOTO to skip column


	-- Get next column in order

	SELECT
		@CurrColumnId = MIN(ORDINAL_POSITION) 
	FROM
		INFORMATION_SCHEMA.COLUMNS (NOLOCK) 
	WHERE 	
		ORDINAL_POSITION > @CurrColumnId
	AND TABLE_NAME = @CurrTable
	AND TABLE_SCHEMA = @CurrSchema


-- Column loop ends here

END


SET @_Step = 'Finalise VALUES constructor'

SET @GetValues = @GetValues + ' + '')'' ';

IF @debug_mode = 1
	PRINT 'Values Retrieval:
' + @GetValues + '

';


-- Using everything we found above, save all the table records as a values constructor (using dynamic SQL)

DECLARE @Params NVARCHAR(MAX)
DECLARE @CMD NVARCHAR(MAX);

SET @Params = N'@Result NVARCHAR(MAX) OUTPUT'
SET @CMD = 'SELECT ' + @top_clause + N' 
	@Result = ISNULL(@Result + '',
		'','''') + ' + @GetValues + ' FROM ' + QUOTENAME(@CurrSchema) + '.' + QUOTENAME(@CurrTable) + 
		' WHERE release_key = ' + cast(@release_key as varchar(20))

IF @debug_mode = 1
	SELECT @CMD;

-- Execute command and get the @Values parameter as output

EXECUTE sp_executesql @CMD, @Params, @Values OUTPUT
select @change_count = @@rowcount

SET @_Step = 'If table returned no rows'

IF @change_count = 0 OR @Values IS NULL
BEGIN
	-- If deletion is enabled

	IF @delete_unmatched_rows = 1

		-- Generate a DELETE statement
		SET @MergeStmnt = N'DELETE FROM ' + QUOTENAME(@CurrSchema) + N'.' + QUOTENAME(@CurrTable)
	ELSE

		-- Otherwise, generate an empty script
		SET @MergeStmnt = N''
END
ELSE

SET @_Step = 'Build the MERGE statement'

BEGIN

	-- Use IDENTITY_INSERT if table has an identity column

	IF @HasIdentity = 1
		SET @MergeStmnt = 'SET IDENTITY_INSERT ' + QUOTENAME(@CurrSchema) + '.' + QUOTENAME(@CurrTable) + ' ON;'
	ELSE
		SET @MergeStmnt = N''

	-- Build the MERGE statement using all the parts we found

	SET @MergeStmnt = @MergeStmnt + N' MERGE INTO ' + QUOTENAME(@CurrSchema) + N'.' + QUOTENAME(@CurrTable) + N' AS trgt ' +
	'USING	(VALUES ' + @Values + N'
			) AS src(' + @ColumnList + N')
	ON
		' + @PKJoinClause + N'
	WHEN MATCHED THEN
		UPDATE SET
			' + @UpdateSet + N'
	WHEN NOT MATCHED BY TARGET THEN
		INSERT (' + @ColumnList + N')
		VALUES (' + @ColumnList + N')
	' + CASE WHEN @delete_unmatched_rows = 1 THEN -- optional
	N'WHEN NOT MATCHED BY SOURCE THEN
		DELETE' ELSE N'' END + N'
	;
	';
	
    -- Get the Rowcount
	SET @MergeStmnt = @MergeStmnt + ' select @result = @@rowcount; '
	-- Use IDENTITY_INSERT if table has an identity column

	IF @HasIdentity = 1
		SET @MergeStmnt = @MergeStmnt + 'SET IDENTITY_INSERT ' + QUOTENAME(@CurrSchema) + '.' + QUOTENAME(@CurrTable) + ' OFF;'

END


Quit:


SET @_Step = 'Output the final statement'

SELECT @vault_statement		= @MergeStmnt
      ,@vault_change_count	= @change_count
/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Release for: ' + quotename(@vault_config_table_schema) + '.' + quotename(@vault_config_table_name) + '('+ cast(@vault_release_number as varchar(20)) + ')'

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Release for: ' + quotename(@vault_config_table_schema) + '.' + quotename(@vault_config_table_name) + '('+ cast(@vault_release_number as varchar(20)) + ')'
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1) -- undocumented uncommitable transaction
	BEGIN
		ROLLBACK TRAN;
		SET @_ErrorContext = @_ErrorContext + ' (Forced rolled back of all changes)';
	END
	
EXEC log4.ExceptionHandler
		  @ErrorContext  = @_ErrorContext
		, @ErrorNumber   = @_Error OUT
		, @ReturnMessage = @_Message OUT
		, @ExceptionId   = @_ExceptionId OUT
;
END CATCH

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	--! Clean up

	--!
	--! Use dbo.udf_FormatElapsedTime() to get a nicely formatted run time string e.g.
	--! "0 hr(s) 1 min(s) and 22 sec(s)" or "1345 milliseconds"
	--!
	IF @_Error = 0
		BEGIN
			SET @_Step			= 'OnComplete'
			SET @_Severity		= @SEVERITY_SUCCESS
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' in a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END
	ELSE
		BEGIN
			SET @_Step			= COALESCE(@_Step, 'OnError')
			SET @_Severity		= @SEVERITY_SEVERE
			SET @_Message		= COALESCE(@_Message, @_Step)
								+ ' after a total run time of ' + log4.FormatElapsedTime(@_SprocStartTime, NULL, 3)
			SET @_ProgressText  = @_ProgressText + @NEW_LINE + @_Message;
		END

	IF @_JournalOnOff = 'ON'
		EXEC log4.JournalWriter
				  @Task				= @_FunctionName
				, @FunctionName		= @_FunctionName
				, @StepInFunction	= @_Step
				, @MessageText		= @_Message
				, @Severity			= @_Severity
				, @ExceptionId		= @_ExceptionId
				--! Supply all the progress info after we've gone to such trouble to collect it
				, @ExtraInfo        = @_ProgressText

	--! Finally, throw an exception that will be detected by the caller
	IF @DoThrowError = 1 AND @_Error > 0
		RAISERROR(@_Message, 16, 99);

	SET NOCOUNT OFF;

	--! Return the value of @@ERROR (which will be zero on success)
	RETURN (@_Error);
END