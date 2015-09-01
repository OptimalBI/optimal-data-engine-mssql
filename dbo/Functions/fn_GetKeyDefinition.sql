
CREATE FUNCTION [dbo].[fn_GetKeyDefinition]
(@object_name varchar(256)
,@object_type varchar(30)
)
RETURNS TABLE 
AS
RETURN 
(
select top 1 [column_name] = rtrim(quotename(isnull(column_prefix, '') + replace(column_name, '%', [dbo].[fn_GetObjectName](@object_name,@object_type) ) + isnull(column_suffix, '')))
    ,[column_type]
    ,[column_length]
	,[column_precision]
	,[column_scale]
	,[collation_Name]
	,[bk_ordinal_position] = -1
    ,[ordinal_position]
	,[Satellite_Ordinal_Position] = -1
FROM [dbo].[dv_default_column]
where 1=1
and object_type = @object_type
--and is_pk = 1
and object_column_type = 'Object_Key'

)

