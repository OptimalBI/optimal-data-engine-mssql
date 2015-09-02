

CREATE 
FUNCTION [dbo].[fn_GetObjectName] 
(
	@object_name varchar(256)
   ,@object_type varchar(50)   
)
RETURNS varchar(256)
AS
BEGIN
DECLARE @ResultVar varchar(256)

select @ResultVar = case
			when [default_subtype] = 'prefix' then [default_varchar] + @object_name
            when [default_subtype] = 'suffix' then @object_name + [default_varchar]
			end 
from [dbo].[dv_defaults]
where 1=1
and [default_type] = @object_type
and [default_subtype] in('prefix', 'suffix')
RETURN @ResultVar

END