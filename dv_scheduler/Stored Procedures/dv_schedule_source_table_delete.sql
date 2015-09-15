
CREATE PROC [dv_scheduler].[dv_schedule_source_table_delete] 
    @schedule_source_table_key	int
   ,@force						int = 0
AS 
	declare @rn1 int
	       ,@rn2 int
	BEGIN TRAN
	select @rn1 = count(*)
	from [dv_scheduler].[dv_schedule_source_table]
	where  [schedule_source_table_key] = @schedule_source_table_key	
	if @rn1 > 0
	    begin
		select @rn2 = count(*)
		from [dv_scheduler].[dv_schedule_source_table]
		where  [schedule_source_table_key] = @schedule_source_table_key	
		  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		if @rn2 <> @rn1
			raiserror('Only Cancelled Schedule Source Table Links may be deleted, unless the Force Parameter has been set', 16, 1)
		else
			DELETE
			FROM   [dv_scheduler].[dv_schedule_source_table]
			WHERE  [schedule_source_table_key] = @schedule_source_table_key	
			  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		end
	COMMIT