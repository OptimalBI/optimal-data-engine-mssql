
CREATE FUNCTION [dv_scripting].[dv_int_to_date](@date as sql_variant)
returns nvarchar(4000)
as
begin
return 'try_convert(date, format(' + cast(@date as varchar(128)) + ',''0000-00-00''))'

end