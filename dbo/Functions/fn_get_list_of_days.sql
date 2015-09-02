

CREATE FUNCTION [dbo].[fn_get_list_of_days]
-- Note that this function works in Local Server Time, as opposed to Offset time as used elsewhere.
-- That is because this function is setting midnight, in User Terms (local).
-- When joining against this function , with Offset columns, be sure to convert them to dateime2(7) to get the required result.
-- provides an inclusive list of dates - midnight on @start_date to midnight on @end_date
(@start_date	date
,@end_date		date
)
RETURNS TABLE 
AS
RETURN 
(select dateadd(ms, -1, dateadd(day, 1, a.Date)) pit_date
from (select dateadd(day,-(a.a + (10 * b.a) + (100 * c.a) + (1000 * d.a) + (10000 * e.a))
	 ,convert(datetime2(7), @end_date)) as Date
    from (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as a
    cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as b
    cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as c
	cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as d
	cross join (select 0 as a union all select 1 union all select 2 union all select 3 union all select 4 union all select 5 union all select 6 union all select 7 union all select 8 union all select 9) as e
) a
where a.Date > dateadd(day, -1, @start_date) 
)