
CREATE PROC [dbo].[dv_ref_function_delete] 
    @ref_function_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_ref_function]
	WHERE  [ref_function_key] = @ref_function_key

	COMMIT