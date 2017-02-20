
CREATE PROCEDURE [dv_config].[dv_populate_source_table_columns]
(
	 @vault_stage_database					varchar(128)
    ,@vault_stage_schema					varchar(128)
	,@vault_stage_table						varchar(128)
	,@vault_source_unique_name				varchar(128)
	,@vault_source_type						varchar(50)
	,@vault_stage_table_load_type			varchar(50)
	,@vault_source_system_name				varchar(128)
	--,@vault_source_procedure_schema			varchar(128)	= Null		
	--,@vault_source_procedure_name			varchar(128)	= Null
	,@vault_release_number					int				= 0
	,@vault_rerun_column_insert				bit				= 0
	,@DoGenerateError						bit				= 0
	,@DoThrowError							bit				= 1
)
AS
BEGIN
SET NOCOUNT ON;

-- Internal use variables

declare @stage_table_key					 int
       ,@stage_schema_key                    int
	   ,@system_key							 int
	   ,@procedure_fully_qualified			 nvarchar(512)
	   ,@table_fully_qualified				 nvarchar(512)


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

--set the Parameters for logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @vault_stage_database         : ' + COALESCE(@vault_stage_database						, '<NULL>')
						+ @NEW_LINE + '    @vault_stage_schema           : ' + COALESCE(@vault_stage_schema						    , '<NULL>')
						+ @NEW_LINE + '    @vault_stage_table            : ' + COALESCE(@vault_stage_table							, '<NULL>')
						+ @NEW_LINE + '    @vault_source_unique_name     : ' + COALESCE(@vault_source_unique_name					, '<NULL>')
						+ @NEW_LINE + '    @vault_source_type            : ' + COALESCE(@vault_source_type							, '<NULL>')
						+ @NEW_LINE + '    @vault_stage_table_load_type  : ' + COALESCE(@vault_stage_table_load_type				, '<NULL>')
						+ @NEW_LINE + '    @vault_source_system_name     : ' + COALESCE(@vault_source_system_name					, '<NULL>')
						--+ @NEW_LINE + '    @vault_source_procedure_schema: ' + COALESCE(@vault_source_procedure_schema				, '<NULL>')
						--+ @NEW_LINE + '    @vault_source_procedure_name  : ' + COALESCE(@vault_source_procedure_name				, '<NULL>')
						+ @NEW_LINE + '    @vault_release_number         : ' + COALESCE(cast(@vault_release_number as varchar)		, '<NULL>')
						+ @NEW_LINE + '    @vault_rerun_column_insert    : ' + COALESCE(cast(@vault_rerun_column_insert as varchar)	, '<NULL>')
						+ @NEW_LINE + '    @DoGenerateError : ' + COALESCE(CAST(@DoGenerateError AS varchar)						, '<NULL>')
						+ @NEW_LINE + '    @DoThrowError    : ' + COALESCE(CAST(@DoThrowError AS varchar)							, '<NULL>')
						+ @NEW_LINE

BEGIN TRANSACTION
BEGIN TRY   
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate Inputs';


select @table_fully_qualified = quotename(@vault_stage_database) + '.' + quotename(@vault_stage_schema) + '.' + quotename(@vault_stage_table)
--select @procedure_fully_qualified = quotename(@vault_stage_database) + '.' + quotename(@vault_source_procedure_schema) + '.' + quotename(@vault_source_procedure_name)

--if not (isnull(@vault_source_procedure_schema, '') = '' and isnull(@vault_source_procedure_name, '') = '')
--	if OBJECT_ID(@procedure_fully_qualified, N'P') is null
--		raiserror('Procedure %s does not exist. Please Create it and retry this Process', 16, 1, @procedure_fully_qualified)

SET @_Step = 'Initialise Variables';

SET @_Step = 'Create Config For Table';

select @system_key			 = [source_system_key]
	from [dbo].[dv_source_system] where [source_system_name] = @vault_source_system_name

select @stage_schema_key = ss.stage_schema_key
from [dbo].[dv_stage_schema] ss
inner join [dbo].[dv_stage_database] sdb on sdb.stage_database_key = ss.stage_database_key
where ss.stage_schema_name = @vault_stage_schema
  and sdb.stage_database_name = @vault_stage_database
select @stage_table_key = [source_table_key] 
	from [dbo].[dv_source_table] where [source_unique_name] = @vault_source_unique_name
if @@ROWCOUNT = 0  -- Table doesn't exist in Config.
	begin
	EXECUTE @stage_table_key = [dbo].[dv_source_table_insert] 
				 @source_unique_name     = @vault_source_unique_name
				,@source_type			 = @vault_source_type         
				,@load_type              = @vault_stage_table_load_type             
				,@system_key			 = @system_key    
				,@source_table_schema    = null    
				,@source_table_name      = ''    
				,@stage_schema_key       = @stage_schema_key	    
				,@stage_table_name       = @vault_stage_table	
				,@is_retired			 = 0
				,@release_number		 = @vault_release_number
	end
	else  -- Table Does Exist in Config
	begin
	if @vault_rerun_column_insert = 1
	    begin
		delete from [dbo].[dv_hub_column] where [column_key] in (select [column_key] from [dbo].[dv_column] where [table_key] = @stage_table_key)
		delete from [dbo].[dv_column] where [table_key] = @stage_table_key
		EXECUTE [dbo].[dv_source_table_update] 
			     @source_table_key		 = @stage_table_key					
		        ,@source_unique_name     = @vault_source_unique_name
				,@source_type			 = @vault_source_type         
				,@load_type              = @vault_stage_table_load_type             
				,@system_key			 = @system_key   
				,@source_table_schema    = null    
				,@source_table_name      = ''    
				,@stage_schema_key       = @stage_schema_key	    
				,@stage_table_name       = @vault_stage_table	
				,@is_retired			 = 0
		end
	else
		begin
			raiserror('Table %s has already been defined. Either Remove it or set the @vault_rerun_column_insert paramater to 1 and try again.',  16, 1, @table_fully_qualified)
		end
	end
		
DECLARE @sql nvarchar(4000);
DECLARE @parm_definition nvarchar(4000);
declare @column_list_xml xml
declare @column_list table(column_name varchar(128));

declare @columns table(
		 column_name				varchar(128)
		,column_type				varchar(30)
		,column_length				int 
		,column_precision			int 
		,column_scale				int 
		,Collation_Name				nvarchar(128) 
		,bk_ordinal_position		int
		,source_ordinal_position	int) 

SET @parm_definition = N'@schema_name varchar(128), @table_name varchar(128), @column_list_OUT xml OUTPUT'

select @sql = 
'select @column_list_OUT = (
select [column_name]				= c.[name]
      ,[column_type]				= t.name
      ,[column_length]				= c.max_length
      ,[column_precision]			= c.[precision]
      ,[column_scale]				= c.[scale]
      ,[collation_Name]				= c.collation_name
      ,[source_ordinal_position]    = row_number() over (order by c.column_id)
from            [' + @vault_stage_database + '].sys.columns c
inner join      [' + @vault_stage_database + '].sys.objects o
on c.object_id = o.object_id
inner join      [' + @vault_stage_database + '].sys.schemas s
on o.schema_id = s.schema_id
inner join      [' + @vault_stage_database + '].sys.types t
on  c.system_type_id    = t.system_type_id
and t.is_user_defined   = 0
and t.user_type_id		= t.system_type_id
where 1=1
and o.type in(''U'', ''V'')
and s.name = @schema_name
and o.name = @table_name
for xml raw)'
declare
@column_name				varchar(128),
@column_type				varchar(30),
@column_length				int = NULL,
@column_precision			int = NULL,
@column_scale				int = NULL,
@Collation_Name				nvarchar(128) = NULL,
@bk_ordinal_position		int,
@source_ordinal_position	int,
@is_source_date				bit,
@is_retired					bit

--select @sql, @parm_definition, @vault_stage_schema, @vault_stage_table
if @_JournalOnOff = 'ON'
	set @_ProgressText  = @_ProgressText + @NEW_LINE + @sql + @NEW_LINE;
exec sp_executesql @sql, @parm_definition, @schema_name = @vault_stage_schema, @table_name = @vault_stage_table, @column_list_OUT=@column_list_xml output;
--select @column_list_xml
declare Col_Cursor cursor forward_only for 
SELECT  
 --      Tbl.Col.value('@table_key', 'int')					table_key,
       Tbl.Col.value('@column_name', 'varchar(128)')		column_name,  
       Tbl.Col.value('@column_type', 'varchar(30)')			column_type,
       Tbl.Col.value('@column_length', 'int')				column_length,
       Tbl.Col.value('@column_precision', 'int')			column_precision,
       Tbl.Col.value('@column_scale', 'int')				column_scale,
	   Tbl.Col.value('@Collation_Name', 'nvarchar(128)')	collation_name,
	   cast(0 as int)										bk_ordinal_position,
	   Tbl.Col.value('@source_ordinal_position', 'int')		source_ordinal_position,
	   cast(0 as bit)										is_source_date,
	   cast(0 as bit)										is_retired
FROM @column_list_xml.nodes('//row') Tbl(Col)
order by column_name
open Col_Cursor
fetch next from Col_Cursor into  @column_name				
								,@column_type				
								,@column_length				
								,@column_precision			
								,@column_scale				
								,@Collation_Name				
								,@bk_ordinal_position		
								,@source_ordinal_position	
								,@is_source_date
								,@is_retired	

while @@FETCH_STATUS = 0
begin								

EXECUTE [dbo].[dv_column_insert] 
	    @table_key					= @stage_table_key
	   ,@satellite_col_key          = NULL
	   ,@release_number				= @vault_release_number
	   ,@column_name				= @column_name					
	   ,@column_type				= @column_type				
	   ,@column_length				= @column_length				
	   ,@column_precision			= @column_precision			
	   ,@column_scale				= @column_scale				
	   ,@Collation_Name				= @Collation_Name				
	   ,@bk_ordinal_position		= @bk_ordinal_position		
	   ,@source_ordinal_position	= @source_ordinal_position	
	   ,@is_source_date				= @is_source_date	
	   ,@is_retired			        = @is_retired

fetch next from Col_Cursor into  @column_name				
								,@column_type				
								,@column_length				
								,@column_precision			
								,@column_scale				
								,@Collation_Name				
								,@bk_ordinal_position		
								,@source_ordinal_position	
								,@is_source_date			
								,@is_retired
end
close Col_Cursor
deallocate Col_Cursor


/*--------------------------------------------------------------------------------------------------------------*/
IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Populated Config for Table: ' + @table_fully_qualified

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Populate Config for Table: ' + @table_fully_qualified
IF (XACT_STATE() = -1) -- uncommitable transaction
OR (@@TRANCOUNT > 0) -- AND XACT_STATE() != 1) -- undocumented uncommitable transaction
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