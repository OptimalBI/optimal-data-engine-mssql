
CREATE FUNCTION [dv_scripting].[dv_int_to_datetime](@date as sql_variant, @time as sql_variant)
returns nvarchar(4000)
as
begin
return 'try_convert(datetime, format(' + cast(@date as varchar(128)) + ',''0000-00-00'') + '' '' + format(' + cast(@time as varchar(128)) + '* 10,''00:00:00:000''), 121)'

end