CREATE PROC [dv_scheduler].[dv_run_insert] 
     @schedule_list				varchar(4000)
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	declare @schedule_list_var	varchar(4000)
	       ,@rc					int
	declare @tbl_schedule_list table(schedule_name varchar(128))

	select @schedule_list_var = replace(@schedule_list, ' ','')	
	insert @tbl_schedule_list select item from [dbo].[fn_split_strings] (@schedule_list_var, ',')	
	select @rc = count(*) 
		from @tbl_schedule_list
		where schedule_name not in (select schedule_name from [dv_scheduler].[vw_dv_schedule_current])
	if @rc > 0 
		RAISERROR('Invalid Schedule Name Provided:  %s', 16, 1, @schedule_list)
	else 
	    begin
		INSERT INTO [dv_scheduler].[dv_run]
			   ([run_schedule_name]) values(@schedule_list_var)	
		end
		-- Begin Return Select <- do not remove
		SELECT [run_key]
			  ,[run_status]
			  ,[run_schedule_name]
			  ,[run_start_datetime]
			  ,[run_end_datetime]
			  ,[updated_datetime]
		  FROM [dv_scheduler].[dv_run]
		WHERE  [run_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()