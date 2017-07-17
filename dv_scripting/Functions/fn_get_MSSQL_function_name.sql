


CREATE 
FUNCTION [dv_scripting].[fn_get_MSSQL_function_name]  
(@source_table_name	VARCHAR(128)
,@function_type VARCHAR(10))
/********************************************************************************************
This takes a source_table_name and returns the name of the function, which ODE provides for data access.
Function Types are:
    "pit" - get all data at a point in time;
	"all" - get net changes between 2 dates;
SELECT [dv_scripting].[fn_get_MSSQL_function_name]('dl_dlapp','all')
SELECT [dv_scripting].[fn_get_MSSQL_function_name]('dl_dlapp','pit')
********************************************************************************************/
RETURNS varchar(1024)
AS
BEGIN
DECLARE @SQL				VARCHAR(300)	= ''
       ,@crlf				CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@func_prefix		VARCHAR(128)
	   ,@func_suffix		VARCHAR(128)
	   ,@func_schema		VARCHAR(128) 

SELECT @func_prefix = CAST([dbo].[fn_get_default_value] ('Prefix', 'MSSQL_AccessFunction') as VARCHAR)
SELECT @func_suffix = CAST([dbo].[fn_get_default_value] ('Suffix_all', 'MSSQL_AccessFunction') as VARCHAR)
SELECT @func_schema = CAST([dbo].[fn_get_default_value] ('Schema', 'MSSQL_AccessFunction') as VARCHAR)
SELECT @SQL = @func_schema + '.' + @func_prefix + '@schema_' + @source_table_name + @func_suffix 
IF @function_type = 'pit' 
   SET @SQL = @source_table_name
ELSE IF @function_type = 'all' 
   SET @SQL += '(sys.fn_cdc_increment_lsn(CONVERT(BINARY(10),''@cdc_start_lsn'', 1)), sys.fn_cdc_get_max_lsn(),''all'')'
ELSE 
	SET @SQL = 'Invalid @function_type provided'
RETURN @SQL

END