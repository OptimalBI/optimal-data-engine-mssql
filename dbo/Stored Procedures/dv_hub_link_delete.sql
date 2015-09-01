CREATE PROC [dbo].[dv_hub_link_delete] 
    @hub_key int
   ,@link_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_hub_link]
	WHERE  [hub_key]   = @hub_key
	  AND  [link_key]  = @link_key

	COMMIT
