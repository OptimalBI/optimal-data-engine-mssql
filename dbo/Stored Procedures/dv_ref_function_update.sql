
CREATE PROC [dbo].[dv_ref_function_update] 
    @ref_function_key int, 
	@ref_function_name varchar(128),
	@ref_function [nvarchar](4000),
    @is_retired bit
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dbo].[dv_ref_function]
	SET    [ref_function_name] = @ref_function_name
	, [ref_function] = @ref_function
	, [is_retired] = @is_retired
	WHERE  [ref_function_key] = @ref_function_key
	
	-- Begin Return Select <- do not remove
	SELECT [ref_function_key],[ref_function_name],[ref_function],[is_retired],[release_key],[version_number],[updated_by],[updated_datetime] 
	FROM [dbo].[dv_ref_function]
	WHERE  [ref_function_key] = @ref_function_key	
	-- End Return Select <- do not remove

	COMMIT