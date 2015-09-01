

/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[JournalReader]
(
  @StartDate			datetime				= NULL
, @EndDate				datetime				= NULL
, @TimeZoneOffset		smallint				= NULL
, @FunctionName			varchar		(  256 )	= NULL
, @FunctionSearchType	tinyint					= NULL
, @MessageText			varchar		(  512 )	= NULL
, @MessageSearchType	tinyint					= NULL
, @Task					varchar		(  128 )	= NULL
, @SeverityBitMask		smallint				= 8191 -- 8191 All Severities or 7167 to exclude debug
, @ResultSetSize		int						= NULL
)

AS

/**************************************************************************************************

Properties
==========
PROCEDURE NAME:		[log4].[JournalReader]
DESCRIPTION:		Returns all Journal entries matching the specified search criteria
DATE OF ORIGIN:		01-DEC-2006
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:			None
DEPENDENCIES:		None

Inputs
======
@DatabaseName
@FunctionName
@MessageText
@StepInFunction
@ExtraInfo
@Severity

Outputs
=======
None

Returns
=======
@@ERROR - always zero on success

Additional Notes
================
Severity Bits (for bitmask):

   1 -- Showstopper/Critical Failure
   2 -- Severe Failure
   4 -- Major Failure
   8 -- Moderate Failure
  16 -- Minor Failure
  32 -- Concurrency Violation
  64 -- Reserved for future Use
 128 -- Reserved for future Use
 256 -- Informational
 512 -- Success
1024 -- Debug
2048 -- Reserved for future Use
4096 -- Reserved for future Use

Function and Message Search Types:

0 = Exclude from Search
1 = Begins With
2 = Ends With
3 = Contains
4 = Exact Match

Revision history
==================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		==============================================================
01-DEC-2006		GML		v0.0.1		Created
------------	------	-------		--------------------------------------------------------------
03-MAY-2011		GML		v0.0.4		Removed ExtraInfo from result set for performance
									Added @TimeZoneOffset for ease of use in other timezones
------------	------	-------		--------------------------------------------------------------
28-AUG-2011		GML		v0.0.6		Added support for ExceptionId and Task columns
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

BEGIN
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	SET NOCOUNT ON

	--! Working variables
	DECLARE	  @Error            int
			, @RowCount         int

	SET @Error 			= 0
	SET @TimeZoneOffset	= COALESCE(@TimeZoneOffset, 0)
	SET @Task			= COALESCE(@Task, '')

	--!
	--! Format the Function search string according to the required search criteria
	--!
	IF LEN(ISNULL(@FunctionName, '')) = 0 OR @FunctionSearchType = 0
		SET @FunctionName = '%'
	ELSE IF LEN(@FunctionName) < 256
		BEGIN
			IF @FunctionSearchType & 1 = 1 AND SUBSTRING(REVERSE(@FunctionName), 1, 1) != '%'
				SET @FunctionName = @FunctionName + '%'

			IF @FunctionSearchType & 2 = 2 AND SUBSTRING(@FunctionName, 1, 1) != '%'
				SET @FunctionName = '%' + @FunctionName

			--! If @FunctionSearchType = 4, do nothing as we want an exact match
		END

	--!
	--! Format the Message search string according to the required search criteria
	--!
	IF LEN(ISNULL(@MessageText, '')) = 0 OR @MessageSearchType = 0
		SET @MessageText = '%'
	ELSE IF LEN(@MessageText) < 512
		BEGIN
			IF @MessageSearchType & 1 = 1 AND SUBSTRING(REVERSE(@MessageText), 1, 1) != '%'
				SET @MessageText = @MessageText + '%'

			IF @MessageSearchType & 2 = 2 AND SUBSTRING(@MessageText, 1, 1) != '%'
				SET @MessageText = '%' + @MessageText

			--! If @MessageSearchType = 4, do nothing as we want an exact match
		END

	--!
	--! If @ResultSetSize is invalid, just return the last 100 rows
	--!
	IF ISNULL(@ResultSetSize, -1) < 1 SET @ResultSetSize = 100
	IF @StartDate IS NULL SET @StartDate = CONVERT(datetime, CONVERT(char(8), DATEADD(day, -7, GETDATE())) + ' 00:00:00', 112)
	IF @EndDate IS NULL SET @EndDate = CONVERT(datetime, CONVERT(char(8), GETDATE(), 112) + ' 23:59:59', 112)

	--! Reverse any time zone offset so we are searching on system time
	SET @StartDate	= DATEADD(hour, @TimeZoneOffset * -1, @StartDate)
	SET @EndDate	= DATEADD(hour, @TimeZoneOffset * -1, @EndDate)

	--!
	--! Return the required results
	--!
	SELECT TOP (@ResultSetSize)
		  j.JournalId
		, DATEADD(hour, @TimeZoneOffset, j.SystemDate)	AS [LocalTime]
		---------------------------------------------------------------------------------------------------
		, j.Task										AS [TaskOrJobName]
		, j.FunctionName								AS [FunctionName]
		, j.StepInFunction								AS [StepInFunction]
		, j.MessageText									AS [MessageText]
		, s.SeverityName								AS [Severity]
		, j.ExceptionId									AS [ExceptionId]
		---------------------------------------------------------------------------------------------------
		, j.SystemDate
	FROM
		[log4].[Journal] AS [j]
	INNER JOIN
		[log4].[Severity] AS [s]
	ON
		s.SeverityId = j.SeverityId
	WHERE
		j.SystemDate BETWEEN @StartDate AND @EndDate
	AND
		j.SeverityId & @SeverityBitMask = j.SeverityId
	AND
		j.Task = COALESCE(NULLIF(@Task, ''), j.Task)
	AND
		j.FunctionName LIKE @FunctionName
	AND
		j.MessageText LIKE @MessageText
	ORDER BY
		j.JournalId DESC

	SELECT @Error = @@ERROR, @RowCount = @@ROWCOUNT

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	SET NOCOUNT OFF

	RETURN(@Error)

END




GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Returns all Journal entries matching the specified search criteria', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'JournalReader';

