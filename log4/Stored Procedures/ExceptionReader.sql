/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[ExceptionReader]
(
  @StartDate			datetime				= NULL
, @EndDate				datetime				= NULL
, @TimeZoneOffset		smallint				= NULL
, @ErrorProcedure		varchar		(  256 )	= NULL
, @ProcedureSearchType	tinyint					= NULL
, @ErrorMessage			varchar		(  512 )	= NULL
, @MessageSearchType	tinyint					= NULL
, @ResultSetSize		int						= NULL
)

AS

/**************************************************************************************************

Properties
==========
PROCEDURE NAME:		[log4].[ExceptionReader]
DESCRIPTION:		Returns all Exceptions matching the specified search criteria
DATE OF ORIGIN:		01-DEC-2006
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			29-AUG-2011
BUILD VERSION:		0.0.6
DEPENDANTS:			None
DEPENDENCIES:		None

Returns
=======
@@ERROR - always zero on success

Additional Notes
================

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
03-MAY-2011		GML		v0.0.4		Added @TimeZoneOffset for ease of use in other timezones
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
	SET NOCOUNT ON;

	--! Working variables
	DECLARE	  @Error            int
			, @RowCount         int

	SET @Error 			= 0
	SET @TimeZoneOffset	= COALESCE(@TimeZoneOffset, 0)

	--!
	--! Format the Function search string according to the required search criteria
	--!
	IF LEN(ISNULL(@ErrorProcedure, '')) = 0 OR @ProcedureSearchType = 0
		SET @ErrorProcedure = '%'
	ELSE IF LEN(@ErrorProcedure) < 256
		BEGIN
			IF @ProcedureSearchType & 1 = 1 AND SUBSTRING(REVERSE(@ErrorProcedure), 1, 1) != '%'
				SET @ErrorProcedure = @ErrorProcedure + '%'

			IF @ProcedureSearchType & 2 = 2 AND SUBSTRING(@ErrorProcedure, 1, 1) != '%'
				SET @ErrorProcedure = '%' + @ErrorProcedure

			--! If @ProcedureSearchType = 4, do nothing as we want an exact match
		END

	--!
	--! Format the Message search string according to the required search criteria
	--!
	IF LEN(ISNULL(@ErrorMessage, '')) = 0 OR @MessageSearchType = 0
		SET @ErrorMessage = '%'
	ELSE IF LEN(@ErrorMessage) < 512
		BEGIN
			IF @MessageSearchType & 1 = 1 AND SUBSTRING(REVERSE(@ErrorMessage), 1, 1) != '%'
				SET @ErrorMessage = @ErrorMessage + '%'

			IF @MessageSearchType & 2 = 2 AND SUBSTRING(@ErrorMessage, 1, 1) != '%'
				SET @ErrorMessage = '%' + @ErrorMessage

			--! If @MessageSearchType = 4, do nothing as we want an exact match
		END

	--!
	--! If @ResultSetSize is invalid, just return the last 100 rows
	--!
	IF ISNULL(@ResultSetSize, -1) < 1 SET @ResultSetSize = 100
	IF @StartDate IS NULL SET @StartDate = CONVERT(datetime, CONVERT(char(8), DATEADD(day, -10, GETDATE())) + ' 00:00:00', 112)
	IF @EndDate IS NULL SET @EndDate = CONVERT(datetime, CONVERT(char(8), GETDATE(), 112) + ' 23:59:59', 112)

	--! Reverse any time zone offset so we are searching on system time
	SET @StartDate	= DATEADD(hour, @TimeZoneOffset * -1, @StartDate)
	SET @EndDate	= DATEADD(hour, @TimeZoneOffset * -1, @EndDate)

	--!
	--! Return the required results
	--!
	SELECT TOP (@ResultSetSize)
		  ExceptionId
		, DATEADD(hour, @TimeZoneOffset, SystemDate)						AS [LocalTime]
		---------------------------------------------------------------------------------------------------
		, ErrorNumber
		, ErrorContext
		, REPLACE(REPLACE(ErrorMessage, CHAR(13), '  '), CHAR(10), '  ')	AS [ErrorMessage]
		, ErrorSeverity
		, ErrorState
		, ErrorProcedure
		, ErrorLine
		---------------------------------------------------------------------------------------------------
		, SystemDate
		, [SessionId]
		, [ProgramName]
		, [NTDomain]
		, [NTUsername]
		, [LoginName]
	FROM
		[log4].[Exception]
	WHERE
		SystemDate BETWEEN @StartDate AND @EndDate
	AND
		ErrorProcedure LIKE @ErrorProcedure
	AND
		ErrorMessage LIKE @ErrorMessage
	ORDER BY
		ExceptionId DESC

	SELECT @Error = @@ERROR, @RowCount = @@ROWCOUNT

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	SET NOCOUNT OFF

	RETURN(@Error)

END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Returns all Exceptions matching the specified search criteria', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'ExceptionReader';

