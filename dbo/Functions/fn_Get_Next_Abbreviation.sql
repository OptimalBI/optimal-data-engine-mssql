CREATE FUNCTION [dbo].[fn_Get_Next_Abbreviation] 
(
)	
RETURNS char(4)
AS
BEGIN

DECLARE @ResultVar char(4)

-- See Jeff Modem's article The "Numbers" or "Tally" Table: What it is and how it replaces a loop.
-- at http://www.sqlservercentral.com/articles/T-SQL/62867/  .
;WITH 
Tens     (N) AS (SELECT 0 UNION ALL SELECT 0 UNION ALL SELECT 0 UNION ALL 
                 SELECT 0 UNION ALL SELECT 0 UNION ALL SELECT 0 UNION ALL 
                 SELECT 0 UNION ALL SELECT 0 UNION ALL SELECT 0 UNION ALL SELECT 0) 
,Thousands(N) AS (SELECT 1 FROM Tens t1 CROSS JOIN Tens t2 CROSS JOIN Tens t3)
,Millions (N) AS (SELECT 1 FROM Thousands t1 CROSS JOIN Thousands t2) 
,Billions (N) AS (SELECT 1 FROM Millions t1 CROSS JOIN Millions t2) 
,Tally    (N) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT 0)) FROM Billions)
,CTE1     (A) AS (SELECT CHAR(N+96) FROM Tally WHERE N between 1 and 26)
,CTE2     (B) AS (SELECT c1.A + c2.A FROM CTE1 c1 CROSS JOIN CTE1 c2)
,CTE3     (C) AS (SELECT c1.A + c2.A + c3.A FROM CTE1 c1 CROSS JOIN CTE1 c2 CROSS JOIN CTE1 c3)
,CTE4     (D) AS (SELECT c1.A + c2.A + c3.A + c4.A FROM CTE1 c1 CROSS JOIN CTE1 c2 CROSS JOIN CTE1 c3 CROSS JOIN CTE1 c4)
,CTE5     (E) AS (SELECT c1.A + c2.A + c3.A + c4.A + c5.A FROM CTE1 c1 CROSS JOIN CTE1 c2 CROSS JOIN CTE1 c3 CROSS JOIN CTE1 c4 CROSS JOIN CTE1 c5)
,CTE          AS (SELECT A, RN = 1 FROM CTE1 UNION ALL
                 SELECT B, RN = 2  FROM CTE2 UNION ALL
                 SELECT C, RN = 3  FROM CTE3 UNION ALL
				 SELECT D, RN = 4  FROM CTE4 UNION ALL
                 SELECT E, RN = 5  FROM CTE5)
,max_abbreviation as (
select max(abbreviation) as max_abbreviation from (
select hub_abbreviation as abbreviation from dv_hub
union
--select pit_abbreviation from dv_pit
--union
select link_abbreviation from dv_link
union
select satellite_abbreviation from dv_satellite
union select '0000') a)
select @ResultVar = min( A )FROM CTE where RN = 4 and A > (select max_abbreviation from max_abbreviation)
RETURN @ResultVar

END