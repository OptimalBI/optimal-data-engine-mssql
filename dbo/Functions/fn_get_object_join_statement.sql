

CREATE FUNCTION [dbo].[fn_get_object_join_statement]
(
	@parent_object_key	int
   ,@parent_object_type varchar(50)
   ,@parent_alias		varchar(50) = NULL
   ,@child_object_key	int
   ,@child_object_type	varchar(50)
   ,@child_alias		varchar(50) = NULL
)
RETURNS nvarchar(max)
AS
BEGIN
	DECLARE @SQL						nvarchar(max)	= ''
	DECLARE @crlf						char(2)			= CHAR(13) + CHAR(10)
	DECLARE @def_schema					varchar(128)	= cast([dbo].[fn_get_default_value] ('schema',@child_object_type) as varchar(128))


set @SQL = ''
/* ------  Hub Logic --------------*/                
	if @parent_object_type = 'hub' and @child_object_type = 'sat'
		begin
		if isnull(@parent_alias, '') = '' set @parent_alias = 'h'
		if isnull(@child_alias, '') = ''  set @child_alias  = 's'

		end


/* ------  Link Logic --------------*/    
	if @parent_object_type = 'lnk'
	  if isnull(@parent_alias, '') = '' set @parent_alias = 'l' 
	  if @child_object_type = 'hub'
		begin
		
		if isnull(@child_alias, '') = ''  set @child_alias  = 'h'
		select @SQL += 'INNER JOIN ' + quotename(h.hub_database) + '.' + quotename(coalesce(h.hub_schema, @def_schema, 'dbo'))  + '.' + quotename((select [dbo].[fn_get_object_name] (h.hub_name, 'hub'))) + ' ' + quotename(@child_alias) + ' ON ' + 
					   quotename(@child_alias) + '.'+ (select column_name from [dbo].[fn_get_key_definition] (h.hub_name, 'hub')) + ' = ' + quotename(@parent_alias) + '.' + (select column_name from [dbo].[fn_get_key_definition] (lkc.link_key_column_name, 'hub'))
		from [dbo].[dv_link] l
		inner join [dbo].[dv_link_key_column] lkc on lkc.link_key = l.link_key
		inner join [dbo].[dv_hub_column] hc on hc.link_key_column_key = lkc.link_key_column_key
		inner join [dbo].[dv_hub_key_column] hkc on hkc.hub_key_column_key = hc.hub_key_column_key
		inner join [dbo].[dv_hub] h on h.hub_key = hkc.hub_key
		where l.link_key = @parent_object_key
		and h.hub_key = @child_object_key
  		end
     else if @child_object_type = 'sat'
		begin
		if isnull(@child_alias, '') = ''  set @child_alias  = 's'
		end 

	RETURN @SQL

END