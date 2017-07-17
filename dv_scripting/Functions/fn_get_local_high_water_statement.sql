


CREATE 
FUNCTION [dv_scripting].[fn_get_local_high_water_statement]  
(@source_table_key	INT
,@high_water_type VARCHAR(10))
/********************************************************************************************
This takes a source_table_key and outputs a script, which will return the current state of the source
satellite table(s). This is the start point or high water mark for the next CDC load.
@high_water_type can be set to "OdeCdc" or "TBA".
Date:
	Returns the maximum High Water Mark for all Satellites, which partake in the Load. 
LSN: 
	Finds the Highest LSN for all Sats, for the Data Source.
	Also ensures that, if there are multiple Sats, the LSN's match between them.
select * from dv_source_table
SELECT [dv_scripting].[fn_get_local_high_water_statement] (1019, 'MSSQLcdc')
********************************************************************************************/
RETURNS varchar(4000)
AS
BEGIN
DECLARE @SQL				 VARCHAR(MAX)	= ''
       ,@crlf				 CHAR(2)			= CHAR(13) + CHAR(10)
	   ,@source_unique_name	 VARCHAR(128)
	   ,@vault_database_name VARCHAR(128)

SELECT @source_unique_name = [source_unique_name]
      ,@vault_database_name = QUOTENAME(s.[satellite_database])
FROM [dbo].[dv_source_table] st
INNER JOIN [dbo].[dv_column] c ON c.[table_key] = st.source_table_key
INNER JOIN [dbo].[dv_satellite_column] sc ON sc.[satellite_col_key] = c.[satellite_col_key]
INNER JOIN [dbo].[dv_satellite] s ON s.[satellite_key] = sc.[satellite_key]
WHERE st.[source_table_key] = @source_table_key

IF @high_water_type IN('ODEcdc', 'MSSQLcdc')
BEGIN
	IF @high_water_type = 'ODEcdc'
	BEGIN
	    SET @SQL += 'DECLARE @local_hw_date VARCHAR(50)' + @crlf	
		SET @SQL += 'SELECT TOP 1 @local_hw_date = CAST([source_high_water_date] AS VARCHAR(50)) ' + @crlf
		END
	ELSE
	    BEGIN
		SET @SQL += 'DECLARE @source_high_water_lsn VARCHAR(50)' + @crlf
		SET @SQL += 'SELECT TOP 1 @source_high_water_lsn = CONVERT(VARCHAR(30), source_high_water_lsn, 1)'  + @crlf
		END
	SET @SQL += 'FROM ' +  @vault_database_name + '.' + '[dbo].[dv_task_state]' + @crlf
	SET @SQL += 'WHERE [object_name] = ''' + @source_unique_name + '''' + @crlf
	SET @SQL += 'AND [object_type] = ''sat''' + @crlf
	SET @SQL += 'ORDER BY [task_end_datetime] DESC' + @crlf

	IF @high_water_type = 'ODEcdc'
		SET @SQL += 'SELECT CASE WHEN ISNULL(@local_hw_date, '''') = '''' THEN ''UNKNOWN'' ELSE @local_hw_date END AS local_hw_date'
    ELSE
		SET @SQL += 'SELECT local_hw_lsn  = CASE WHEN ISNULL(@source_high_water_lsn, '''') = '''' THEN ''0x00000000000000000000'' ELSE @source_high_water_lsn END'
END
ELSE	
BEGIN
	SET @SQL += ''
END 
RETURN @SQL

END