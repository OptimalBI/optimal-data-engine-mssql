CREATE PROC [dbo].[dv_link_delete] 
    @link_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_link]
	WHERE  [link_key] = @link_key

	COMMIT