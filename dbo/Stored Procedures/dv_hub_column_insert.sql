CREATE PROC [dbo].[dv_hub_column_insert] 
    @hub_key_column_key int,
    @column_key int,
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

	INSERT INTO [dbo].[dv_hub_column] ([hub_key_column_key], [column_key],[release_key])
	SELECT @hub_key_column_key, @column_key, @release_key
	
	-- Begin Return Select <- do not remove
	SELECT [hub_col_key], [hub_key_column_key], [column_key], [version_number], [updated_by], [updated_datetime],[release_key]
	FROM   [dbo].[dv_hub_column]
	WHERE  [hub_col_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()