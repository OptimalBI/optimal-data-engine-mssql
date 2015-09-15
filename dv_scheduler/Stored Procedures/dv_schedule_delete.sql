
CREATE PROC [dv_scheduler].[dv_schedule_delete] 
    @schedule_key int,
	@force		  bit = 0
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	declare @rn1 int
	       ,@rn2 int
	BEGIN TRAN
	select @rn1 = count(*)
	from [dv_scheduler].[dv_schedule]
	where  [schedule_key] = @schedule_key
	if @rn1 > 0
	    begin
		select @rn2 = count(*)
		from [dv_scheduler].[dv_schedule]
		where  [schedule_key] = @schedule_key
		  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		if @rn2 <> @rn1
			raiserror('Only Cancelled Schedules may be deleted, unless the Force Parameter has been set', 16, 1)
		else
			delete
			from [dv_scheduler].[dv_schedule]
			where  [schedule_key] = @schedule_key
			  and (isnull(@force, 0) | isnull([is_cancelled], 0) = 1)
		end

 	COMMIT