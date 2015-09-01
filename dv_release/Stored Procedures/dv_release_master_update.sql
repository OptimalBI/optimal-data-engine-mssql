CREATE PROC [dv_release].[dv_release_master_update] 
	@release_number int,
    @release_description varchar(256) = NULL,
    @reference_number varchar(50) = NULL,
    @reference_source varchar(50) = NULL
  
AS 
	SET NOCOUNT ON 
	SET XACT_ABORT ON  
	
	BEGIN TRAN

	UPDATE [dv_release].[dv_release_master]
	SET    [release_description] = @release_description, [reference_number] = @reference_number, [reference_source] = @reference_source
	WHERE  [release_number] = @release_number
	-- Begin Return Select <- do not remove
	SELECT [release_key], [release_number], [release_description], [reference_number], [reference_source]
	FROM   [dv_release].[dv_release_master]
	WHERE  [release_number] = @release_number	
	-- End Return Select <- do not remove

	COMMIT