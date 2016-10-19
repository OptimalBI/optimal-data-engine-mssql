
CREATE FUNCTION [dv_scripting].[dv_concat](@string as varchar(4096))
returns varchar(4096)
as
begin
return replace(replace(@string, '"', ''''), '||', '+') 

end