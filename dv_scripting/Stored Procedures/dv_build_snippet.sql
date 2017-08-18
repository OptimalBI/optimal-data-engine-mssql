
create Procedure [dv_scripting].[dv_build_snippet] 
(@input_string nvarchar(4000)
,@argument_list nvarchar(512)
,@output_string nvarchar(4000) output
,@dogenerateerror				bit				= 0
,@dothrowerror					bit				= 1
)
AS
BEGIN
set nocount on

--  Working Storage
declare @start_pos int,
		@open_pos int,
		@brcktcount int,
		@string_pos int,
		@current_row int,
		@parent int,
		@string nvarchar(4000),
		@command nvarchar(4000),
		@parm_definition nvarchar(500),
		@commandstring nvarchar(4000),
		@expression nvarchar(4000),
		@snippet nvarchar(4000),
		@resultvar nvarchar(4000),
		@param nvarchar(512)
	    ,@replace nvarchar(512)
	    ,@default_func nvarchar(512)

declare @snippet_table TABLE 
       ([id] integer NOT NULL identity(1,1),
		parent int NULL,
		command	nvarchar(4000) NULL,
		expression nvarchar(4000) NULL,
		snippet nvarchar(4000) NULL)
declare @source_arg_list TABLE
       (ItemNumber int
       ,arg nvarchar(512))
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


-- set Log4TSQL Parameters for Logging:
SET @_ProgressText		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
						+ @NEW_LINE + '    @input_string 				: ' + COALESCE(@input_string, 'NULL')
						+ @NEW_LINE + '    @DoGenerateError             : ' + COALESCE(CAST(@DoGenerateError AS varchar), 'NULL')
						+ @NEW_LINE + '    @DoThrowError                : ' + COALESCE(CAST(@DoThrowError AS varchar), 'NULL')
						+ @NEW_LINE

BEGIN TRY
SET @_Step = 'Generate any required error';
IF @DoGenerateError = 1
   select 1 / 0
SET @_Step = 'Validate inputs';

IF isnull(@input_string, '') = ''
			RAISERROR('Empty input string passed', 16, 1);

/*--------------------------------------------------------------------------------------------------------------*/
--Initialise outer loop - works through the input string,
--						  writing each function call to a new row in the table variable.
--                        continues until reduced to single function calls, which can be executed.

SET @_Step = 'Replace Arguments in the string by ##number'

set @string = ltrim(rtrim(@input_string))

-- set default func snippet
-- potentially could be replaced with somwthing else
set @default_func='[dv_scripting].[dv_'

-- identify string in case of ode scripting function 
--if (@snippet_type ='default')
 --set @string = right(@string, len(@string) - charindex( @default_func, @string,1)+1) 

 --trim off any superfluous stuff from the head of the command - command must start with a function!
insert @source_arg_list select * from [dbo].[fn_split_strings] (@argument_list, ',')


-- replace all #i argument in the func call/code by function parameter
declare curPar CURSOR LOCAL for
select '##' + cast(ItemNumber as varchar(100)) as func_string
		,arg as replace_string
from @source_arg_list
open curPar
fetch next from curPar into @param, @replace
while @@FETCH_STATUS = 0 
	BEGIN
	set @string = replace(@string,@param, @replace) 
	fetch next from curPar into @param, @replace
	END
close curPar
deallocate curPar


SET @_Step = 'Initialise Outer Loop'

/*
if (@snippet_type ='dv_scripting')
begin
    set @string = right(@string, len(@string) - charindex( @default_func, @string,1)+1)
    set @start_pos = charindex('(', @string,1);
    -- function name as command
    set @command = left(@string, @start_pos-1);
end
else 
    set @command = @string
 */

set @command = @string
set @snippet = @string

insert @snippet_table select 0, @command,  @string, @snippet

select @current_row = SCOPE_IDENTITY()
set @open_pos = 1

while @current_row <= (select max([id]) from @snippet_table)
begin
    -- Initialise second loop - works through a single row, pulling out each first level function call, into a new row in the table
	SET @_Step = 'Initialise Second Loop'
	set @start_pos = charindex( @default_func, @string, @open_pos )
	while @start_pos > 0
	begin
		set @open_pos =  charindex('(', @string,@start_pos)
		set @brcktcount = 1
		while @brcktcount > 0
		begin
			set @open_pos += 1
			if substring(@string,@open_pos, 1) = '(' set @brcktcount = @brcktcount + 1
			if substring(@string,@open_pos, 1) = ')' set @brcktcount = @brcktcount - 1
		end

		set @commandstring = substring(@string,@start_pos,@open_pos-@start_pos+ 1) 
		
		set @command = left(@commandstring,charindex('(', @commandstring,1)-1)
		set @snippet = left(right(@commandstring, len(@commandstring) - charindex('(', @commandstring, 1)), len(right(@commandstring, len(@commandstring) - charindex('(', @commandstring, 1))) -1)
		
		insert @snippet_table select @current_row, @command, @commandstring, @snippet

		set @start_pos = charindex( @default_func, @string,@open_pos )
	end
	set @open_pos = 0
	set @current_row = @current_row + 1
	select @string = snippet from @snippet_table where [id] = @current_row
end

-- Now work backwards through the table, replacing commands with code snippets
SET @_Step = 'Replace Commands with Snippets'
set @parm_definition = N'@codesnippet nvarchar(4000) OUTPUT'
select @commandstring = expression from @snippet_table where parent = 0


select @current_row = max([id]) from @snippet_table
select @parent = parent 
      ,@command = 'SELECT @codesnippet = ' + command + '(' + snippet + ')'
	  ,@expression = expression
	  from @snippet_table where [id] = @current_row

	-- Loop through the scripts, replacing the snippets in the parent
	-- alows nesting



while @current_row > 1
begin
    
	EXECUTE sp_executesql @command, @parm_definition, @codesnippet = @snippet OUTPUT

	--if its the master row, just replace the whole snippet
	update @snippet_table set snippet = replace(snippet,@expression, @snippet) where [id] = case when @parent = 0 then @current_row else @parent end
	set @current_row = @current_row -1

	

	select @parent = parent
	      ,@command = 'SELECT @codesnippet = ' + command + '(' + snippet + ')'
		  ,@expression = expression
		  from @snippet_table where [id] = @current_row
end

select @output_string = replace(snippet, '#', '''') from @snippet_table where parent = 0

/*--------------------------------------------------------------------------------------------------------------*/

SET @_ProgressText  = @_ProgressText + @NEW_LINE
				+ 'Step: [' + @_Step + '] completed ' 

IF @@TRANCOUNT > 0 COMMIT TRAN;

SET @_Message   = 'Successfully Created Code Snippet ' 

END TRY
BEGIN CATCH
SET @_ErrorContext	= 'Failed to Create Code Snippet' 
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
