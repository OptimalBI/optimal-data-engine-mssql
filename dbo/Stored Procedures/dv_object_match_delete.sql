
CREATE PROC [dbo].[dv_object_match_delete] 
    @match_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_object_match]
	WHERE  [match_key] = @match_key

	COMMIT