
CREATE 
FUNCTION [dbo].[fn_GetDefaultValue] 
(
	@default_name varchar(50)
   ,@default_type varchar(50)
)
RETURNS sql_variant
AS
BEGIN
DECLARE @ResultVar sql_variant

select @ResultVar = 
     case when [data_type] = 'varchar'	then cast([default_varchar]	 as sql_variant)
	      when [data_type] = 'int'		then cast([default_integer]	 as sql_variant)
		  when [data_type] = 'datetime' then cast([default_dateTime] as sql_variant)
		  else null
		  end
from [dbo].[dv_defaults]
where 1=1
and [default_type] = @default_type
and [default_subtype] = @default_name
RETURN @ResultVar

END



