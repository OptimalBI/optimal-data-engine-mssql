

CREATE PROC [dbo].[dv_stage_schema_insert] 
    @stage_database_key int,
    @stage_schema_name varchar(128),
	@is_retired bit,
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

	INSERT INTO [dbo].[dv_stage_schema] ([stage_database_key],[stage_schema_name],[is_retired],[release_key])
    SELECT @stage_database_key, @stage_schema_name, @is_retired, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT *
	FROM   [dbo].[dv_stage_schema]
	WHERE  [stage_schema_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()