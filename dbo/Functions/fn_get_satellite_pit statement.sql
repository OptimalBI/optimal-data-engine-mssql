--select [dbo].[fn_get_satellite_pit statement](DEFAULT)
--select [dbo].[fn_get_satellite_pit statement](sysdatetimeoffset())

CREATE FUNCTION [dbo].[fn_get_satellite_pit statement]
(
@sat_pit					datetimeoffset(7) = NULL
)
RETURNS nvarchar(1000)
AS
BEGIN
	DECLARE @SQL						nvarchar(max)	= ''
    DECLARE @sat_tombstone_indicator	varchar(128)
	DECLARE @sat_current_row_col		varchar(128)
	DECLARE @sat_version_start_date		varchar(128)
	DECLARE @sat_version_end_date		varchar(128)

	select @sat_current_row_col = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Current_Row'
	select @sat_tombstone_indicator = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Tombstone_Indicator'
	select @sat_version_start_date = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Version_Start_Date'
	select @sat_version_end_date = quotename(column_name)
	from [dbo].[dv_default_column]
	where object_type	= 'sat' and object_column_type = 'Version_End_Date'

	set @SQL = '(' + @sat_tombstone_indicator + ' = 0 AND '
	if isnull(@sat_pit, '') = '' 
		select @SQL +=  @sat_current_row_col + ' = 1'
		else
		begin
		select @SQL += '(' + @sat_version_end_date + ' > ''' + cast(@sat_pit as varchar(50)) + ''' AND ' + @sat_version_start_date + ' <= ''' + cast(@sat_pit as varchar(50)) + ''')'
		end
	set @SQL += ')'
	RETURN @SQL

END