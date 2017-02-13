
CREATE PROC [dbo].[dv_column_match_delete] 
    @col_match_key int
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	DELETE
	FROM   [dbo].[dv_column_match]
	WHERE  [col_match_key] = @col_match_key

	COMMIT