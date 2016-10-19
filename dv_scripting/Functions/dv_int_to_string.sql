
CREATE FUNCTION [dv_scripting].[dv_int_to_string](@int as sql_variant)
returns varchar(4096)
as
begin
return 'convert(varchar(256), '+  cast(@int as varchar(128)) + ')'

end