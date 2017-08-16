
CREATE PROC [dbo].[dv_ref_function_insert] 
     @ref_function_name varchar(128),
	@ref_function [nvarchar](4000),
	@ref_function_type nvarchar(4000),
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

	INSERT INTO [dbo].[dv_ref_function]([ref_function_name],[ref_function],[is_retired],[release_key],[ref_func_type])
	SELECT @ref_function_name, @ref_function,@is_retired,@release_key,@ref_function_type

	
	-- Begin Return Select <- do not remove
	SELECT [ref_function_key],[ref_function_name],[ref_function],[is_retired],[release_key],[version_number],[updated_by],[updated_datetime] ,[ref_func_type]
	FROM [dbo].[dv_ref_function]
	WHERE  [ref_function_key] = SCOPE_IDENTITY()
	-- End Return Select <- do not remove
               
	COMMIT
       RETURN SCOPE_IDENTITY()