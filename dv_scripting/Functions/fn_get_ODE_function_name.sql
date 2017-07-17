


CREATE 
FUNCTION [dv_scripting].[fn_get_ODE_function_name]  
(@source_table_name	VARCHAR(128)
,@function_type VARCHAR(10))
/********************************************************************************************
This takes a source_table_name and returns the name of the function, which ODE provides for data access.
Function Types are:
    "all" - get all changes between 2 dates;
	"pit" - get a full copy of the source at a specified point in time. 
SELECT [dv_scripting].[fn_get_ODE_function_name]('MyTableName','pit')
SELECT [dv_scripting].[fn_get_ODE_function_name]('MyTableName','all')
********************************************************************************************/
RETURNS varchar(1024)
AS
BEGIN
DECLARE @SQL				VARCHAR(300)	= ''
       ,@crlf				CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@func_prefix		VARCHAR(128)
	   ,@func_suffix		VARCHAR(128) 

SELECT @func_prefix = CAST([dbo].[fn_get_default_value] ('Prefix', 'ODE_AccessFunction') as VARCHAR)
	SELECT @func_suffix = CAST([dbo].[fn_get_default_value] ('Suffix_'+ @function_type, 'ODE_AccessFunction') as VARCHAR)
--SET @SQL = QUOTENAME(@func_prefix + @source_table_name + @func_suffix)
SET @SQL = @func_prefix + @source_table_name + @func_suffix 
IF @function_type = 'pit' 
   SET @SQL += '(@pit)'
ELSE IF @function_type = 'all' 
   SET @SQL += '(@cdc_start_time, @cdc_end_time)'
ELSE 
	SET @SQL = 'Invalid @function_type provided'
RETURN @SQL

END