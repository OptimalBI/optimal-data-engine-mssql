
CREATE FUNCTION [dv_scripting].[dv_concat](@string as nvarchar(4000))
returns nvarchar(4000)
as
begin
return replace(replace(@string, '"', ''''), '||', '+') 

end