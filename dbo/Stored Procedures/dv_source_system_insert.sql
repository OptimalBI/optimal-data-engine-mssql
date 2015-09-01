CREATE PROC [dbo].[dv_source_system_insert] 
    @source_system_name varchar(50),
    @timevault_name varchar(50),
	@release_number int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN
	
	declare @release_key int
	       ,@rc int
	select @release_key = [release_key] from [dv_release].[dv_release_master] where [release_number] = @release_number
	set @rc = @@rowcount
	if @rc <> 1 
		RAISERROR('Release Number %i Does Not Exist', 16, 1, @release_number)
	
	INSERT INTO [dbo].[dv_source_system] ([source_system_name], [timevault_name],[release_key])
	SELECT @source_system_name, @timevault_name, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [system_key], [source_system_name], [timevault_name],[release_key]
	FROM   [dbo].[dv_source_system]
	WHERE  [system_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()

