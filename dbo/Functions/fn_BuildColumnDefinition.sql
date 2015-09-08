CREATE
FUNCTION [dbo].[fn_BuildColumnDefinition] 
(
	@DataType varchar(50)
   ,@DataLength int
   ,@precision int 
   ,@scale int 
   ,@CollationName varchar(50)
   ,@is_nullable bit
   ,@is_identity bit
)
RETURNS varchar(256)
AS
BEGIN
DECLARE @ResultVar varchar(256)

select @ResultVar = UPPER(@DataType)
             + CASE
--NUMERIC
               WHEN UPPER(@DataType) IN ('decimal','numeric')
               THEN '('
                    + CONVERT(VARCHAR,@precision)
                    + ','
                    + CONVERT(VARCHAR,@scale)
                    + ') '
                    + SPACE(6 - LEN(CONVERT(VARCHAR,@precision)
                    + ','
                    + CONVERT(VARCHAR,@scale)))
                    + SPACE(7)
                    + SPACE(16 - LEN(UPPER(@DataType)))
                    
--FLOAT
               WHEN  UPPER(@DataType) IN ('float', 'datetime2') 
               THEN
                    CASE
                      WHEN @precision = 53
                      THEN SPACE(11 - LEN(CONVERT(VARCHAR,@precision)))
                           + SPACE(7)
                           + SPACE(16 - LEN(UPPER(@DataType)))
                      ELSE '('
						   + CONVERT(VARCHAR,@scale)
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,@precision)))
                           + SPACE(7) + SPACE(16 - LEN(UPPER(@DataType)))
   
                      END

               WHEN  UPPER(@DataType) IN ('char','varchar')
               THEN CASE
                      WHEN  @DataLength = -1
                      THEN  '(max)'
                            + SPACE(6 - LEN(CONVERT(VARCHAR,@DataLength)))
                            + SPACE(7) + SPACE(16 - LEN(UPPER(@DataType)))

                      ELSE '('
                           + CONVERT(VARCHAR,@DataLength)
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,@DataLength)))
                           + SPACE(7) + SPACE(16 - LEN(UPPER(@DataType)))
 
                    END
--NVARCHAR
               WHEN UPPER(@DataType) IN ('nchar','nvarchar')
               THEN CASE
                      WHEN  @DataLength = -1
                      THEN '(max)'
                           + SPACE(6 - LEN(CONVERT(VARCHAR,(@DataLength / 2))))
                           + SPACE(7)
                           + SPACE(16 - LEN(UPPER(@DataType)))
                      ELSE '('
                           + CONVERT(VARCHAR,(@DataLength / 2))
                           + ') '
                           + SPACE(6 - LEN(CONVERT(VARCHAR,(@DataLength / 2))))
                           + SPACE(7)
                           + SPACE(16 - LEN(UPPER(@DataType)))
  
                    END
--datetime
               WHEN UPPER(@DataType) IN ('datetime','money','text','image','real')
               THEN SPACE(18 - LEN(UPPER(@DataType)))
                    + '              '
 
--VARBINARY
              WHEN UPPER(@DataType) = 'varbinary'
              THEN
                CASE
                  WHEN @DataLength = -1
                  THEN '(max)'
                       + SPACE(6 - LEN(CONVERT(VARCHAR,(@DataLength))))
                       + SPACE(7)
                       + SPACE(16 - LEN(UPPER(@DataType)))
   
                  ELSE '('
                       + CONVERT(VARCHAR,(@DataLength))
                       + ') '
                       + SPACE(6 - LEN(CONVERT(VARCHAR,(@DataLength))))
                       + SPACE(7)
                       + SPACE(16 - LEN(UPPER(@DataType)))

                END
--INT
               ELSE SPACE(16 - LEN(UPPER(@DataType)))   
                            + SPACE(2)
                            
               END

IF UPPER(@DataType) IN ('nchar','nvarchar','char','varchar')
   IF isnull(@CollationName, '') <> ''
      set @ResultVar = rtrim(@ResultVar) + ' COLLATE ' + @CollationName

set @ResultVar = rtrim(@ResultVar) + case when isnull (@is_identity, 1) = 1 then ' IDENTITY(1,1) ' else ' ' END 

set @ResultVar = rtrim(@ResultVar) + case when isnull (@is_nullable, 1) = 1 then '' else ' NOT' END + ' NULL'
RETURN @ResultVar

END