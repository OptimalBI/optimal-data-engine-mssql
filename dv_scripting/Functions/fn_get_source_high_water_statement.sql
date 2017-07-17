


CREATE
FUNCTION [dv_scripting].[fn_get_source_high_water_statement]  
(@source_table_key	INT
,@high_water_type VARCHAR(10))
/********************************************************************************************
This takes a source_table_key and outputs a script, which will return the current state of the source
satellite table(s). This is the start point or high water mark for the next CDC load.
@high_water_type can be set to "ODEcdc" or "MSSQLcdc".
Use this function to accertain the high water mark of the source from which you wish to pull CDC data.

Date:
	Returns the maximum High Water Mark for all Satellites, which partake in the Load. 
LSN: 
	Finds the Highest LSN for all Sats, for the Data Source.
	Also ensures that, if there are multiple Sats, the LSN's match between them.
SELECT * FROM [dbo].[dv_source_table]
SELECT [dv_scripting].[fn_get_source_high_water_statement] (33, 'ODEcdc') 
SELECT [dv_scripting].[fn_get_source_high_water_statement] (33, 'MSSQLcdc')
********************************************************************************************/
RETURNS varchar(4000)
AS
BEGIN
DECLARE @SQL				VARCHAR(MAX)	= ''
       ,@crlf				CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@source_table_name	VARCHAR(128)

SELECT @source_table_name = [source_table_nme] 
FROM [dbo].[dv_source_table] st
--INNER JOIN [dbo].[dv_column] c ON c.[table_key] = st.source_table_key
--INNER JOIN [dbo].[dv_satellite_column] sc ON sc.[satellite_col_key] = c.[satellite_col_key]
--INNER JOIN [dbo].[dv_satellite] s ON s.[satellite_key] = sc.[satellite_key]

WHERE st.[source_table_key] = @source_table_key

IF @high_water_type = 'ODEcdc'
BEGIN	
	SET @SQL += 'SELECT TOP 1 CAST([high_water_date] AS VARCHAR(50)) AS source_hw_date FROM [dbo].[dv_task_state]' + @crlf
	SET @SQL += 'WHERE [object_name] = ''' + @source_table_name + '''' + @crlf
	SET @SQL += 'AND [object_type] = ''sat''' + @crlf
	SET @SQL += 'ORDER BY [task_end_datetime] DESC'
END
ELSE
IF @high_water_type = 'MSSQLcdc'
BEGIN	
	SET @SQL += 'SELECT CONVERT(VARCHAR(30), sys.fn_cdc_get_max_lsn (), 1) AS source_hw_lsn' + @crlf
END
ELSE
BEGIN
SET @SQL += 'SELECT source_hw_date = CAST(sysdatetimeoffset() AS VARCHAR(50))'
END
RETURN @SQL

END