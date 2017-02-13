
CREATE PROC [dbo].[dv_link_key_delete] 
    @link_key_column_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_link_key_column]
	WHERE  [link_key_column_key] = @link_key_column_key

	COMMIT