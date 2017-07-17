--select [dbo].[fn_get_satellite_pit statement](DEFAULT)
--select [dbo].[fn_get_satellite_pit statement](sysdatetimeoffset())

CREATE FUNCTION [dbo].[fn_get_satellite_cdc_all_statement]
(
 @cdc_start_time	datetimeoffset(7) = NULL
,@cdc_end_time		datetimeoffset(7) = NULL
)
RETURNS nvarchar(1000)
AS
BEGIN
	DECLARE @SQL						nvarchar(max)	= ''
	DECLARE @sat_current_row_col		varchar(128)
	DECLARE @sat_version_start_date		varchar(128)
	DECLARE @sat_version_end_date		varchar(128)

	select @sat_current_row_col = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Current_Row'
		select @sat_version_start_date = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Version_Start_Date'

	set @SQL = ''
	set @SQL += '(' + @sat_version_start_date + ' > ''' + cast(@cdc_start_time as varchar(50)) + ''' AND ' + @sat_version_start_date + ' <= ''' + cast(@cdc_end_time as varchar(50)) + ''')'

	RETURN @SQL

END