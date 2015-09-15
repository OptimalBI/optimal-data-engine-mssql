
CREATE PROC [dv_scheduler].[dv_source_table_hiearchy_delete] 
    @source_table_hiearchy_key int
   ,@force					   int = 0
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	declare @rn1 int
	       ,@rn2 int
	BEGIN TRAN
	select @rn1 = count(*)
	from [dv_scheduler].[dv_source_table_hierarchy]
	where  [source_table_hierarchy_key] = @source_table_hiearchy_key
	if @rn1 > 0
	    begin
		select @rn2 = count(*)
		from [dv_scheduler].[dv_source_table_hierarchy]
		where  [source_table_hierarchy_key] = @source_table_hiearchy_key
		  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		if @rn2 <> @rn1
			raiserror('Only Cancelled Source Table Hierarchies may be deleted, unless the Force Parameter has been set', 16, 1)
		else
			DELETE
			FROM   [dv_scheduler].[dv_source_table_hierarchy]
			WHERE  [source_table_hierarchy_key] = @source_table_hiearchy_key
			  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		end
	COMMIT