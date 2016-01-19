/*************************************************************************************************/
--</MaintenanceHeader>

CREATE FUNCTION [log4].[FormatElapsedTime]
(
  @StartTime                      datetime
, @EndTime                        datetime  = NULL
, @ShowMillisecsIfUnderNumSecs    tinyint   = NULL
)

RETURNS varchar  (  48 )

AS

--<CommentHeader>
/**************************************************************************************************

Properties
==========
FUNCTION NAME:      [log4].[FormatElapsedTime]
DESCRIPTION:        Returns a string describing the time elapsed between start and end time
DATE OF ORIGIN:		16-FEB-2007
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:         Various
DEPENDENCIES:       None

Additional Notes
================
Builds a string that looks like this: "0 hr(s) 1 min(s) and 22 sec(s)" or "1345 milliseconds"

Revision history
==================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		==============================================================
16-FEB-2007		GML		v0.0.2		Created
------------	------	-------		--------------------------------------------------------------

=================================================================================================
(C) Copyright 2006-12 data-centric solutions ltd. (http://log4tsql.sourceforge.net/)

This library is free software; you can redistribute it and/or modify it under the terms of the
GNU Lesser General Public License as published by the Free Software Foundation (www.fsf.org);
either version 3.0 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along with this
library; if not, you can find it at http://www.opensource.org/licenses/lgpl-3.0.html
or http://www.gnu.org/licenses/lgpl.html

**************************************************************************************************/
--</CommentHeader>

BEGIN
	DECLARE	  @time                     int
			, @hrs                      int
			, @mins                     int
			, @secs                     int
			, @msecs                    int
			, @Duration                 varchar   (   48 )

	IF @StartTime IS NULL AND @EndTime IS NULL
		SET @Duration = 'Start and End Times are both NULL'
	ELSE IF @StartTime IS NULL
		SET @Duration = 'Start Time is NULL'
	ELSE
		BEGIN
			IF @EndTime IS NULL SET @EndTime = GETDATE()

			SET @time = DATEDIFF(ss, @StartTime, @EndTime)

			IF @time > ISNULL(@ShowMillisecsIfUnderNumSecs, 5)
				BEGIN
					SET @hrs        = @time / 3600
					SET @mins       = (@time % 3600) / 60
					SET @secs       = (@time % 3600) % 60
					SET @Duration   = CASE
										WHEN @hrs = 0 THEN ''
										WHEN @hrs = 1 THEN CAST(@hrs AS varchar) + ' hr, '
										ELSE CAST(@hrs AS varchar) + ' hrs, '
									  END
									+ CASE
										WHEN @mins = 1 THEN CAST(@mins AS varchar) + ' min'
										ELSE CAST(@mins AS varchar) + ' mins'
									  END
									+ ' and '
									+ CASE
										WHEN @secs = 1 THEN CAST(@secs AS varchar) + ' sec'
										ELSE CAST(@secs AS varchar) + ' secs'
									  END
				END
			ELSE
				BEGIN
					SET @msecs      = DATEDIFF(ms, @StartTime, @EndTime)
					SET @Duration   = CAST(@msecs AS varchar) + ' milliseconds'
				END
		END

	RETURN @Duration
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Returns a string describing the time elapsed between start and end time', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'FUNCTION', @level1name = N'FormatElapsedTime';

