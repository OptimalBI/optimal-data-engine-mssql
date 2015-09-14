CREATE PROC [dv_scheduler].[dv_schedule_insert] 
     @schedule_name				varchar(128)
    ,@schedule_description		varchar(256)
    ,@schedule_frequency		varchar(128)
	,@release_number int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key				int
	       ,@schedule_name_no_spaces	varchar(128)
	       ,@rc							int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)
    else
	begin
	    select @schedule_name_no_spaces = replace(@schedule_name, ' ','')
		INSERT INTO [dv_scheduler].[dv_schedule]
			   ([schedule_name],[schedule_description],[schedule_frequency],[release_key])
		SELECT @schedule_name, @schedule_description, @schedule_frequency, @release_key
	end
	-- Begin Return Select <- do not remove
		SELECT [schedule_key]
			  ,[schedule_name]
			  ,[schedule_description]
			  ,[schedule_frequency]
			  ,[release_key]
			  ,[version_number]
			  ,[updated_by]
			  ,[updated_datetime]
		  FROM [dv_scheduler].[dv_schedule]
			WHERE  [schedule_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()