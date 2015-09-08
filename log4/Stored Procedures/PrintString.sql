/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[PrintString]
(
  @InputString		nvarchar(max)	= NULL
, @MaxPrintLength	int				= 4000
)

AS

--<CommentHeader>
/**********************************************************************************************************************

Properties
=====================================================================================================================
PROCEDURE NAME:		[log4].[PrintString]
DESCRIPTION:		Prints the supplied string respecting all line feeds and/or carriage returns except where no
					line feeds are found, in which case the output is printed in user-specified lengths
DATE OF ORIGIN:		05-NOV-2011
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:			None
DEPENDENCIES:		None

Inputs
======
@InputString - optional, the string to print
@MaxPrintLength - Max length of string to print before inserting an unnatural break

Outputs
=======
None

Returns
=======
NULL

Additional Notes
================

Revision history
=====================================================================================================================
ChangeDate    Author   Version  Narrative
============  =======  =======  =====================================================================================
05-NOV-2011   GML      v0.0.8   Created
------------  -------  -------  -------------------------------------------------------------------------------------
13-MAR-2012   GML      v0.0.10  Fixed backwards-compatability issue with @LineFeedPos
------------  -------  -------  -------------------------------------------------------------------------------------


=====================================================================================================================
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

**********************************************************************************************************************/
--</CommentHeader>

BEGIN
	SET NOCOUNT ON

	--! CONSTANTS (keep it SQL2005 compatible)
	DECLARE @LF					char		(    1 ); SET @LF			= CHAR(10);
	DECLARE @CR					char		(    1 ); SET @CR			= CHAR(13);
	DECLARE @CRLF				char		(    2 ); SET @CRLF			= CHAR(13) + CHAR(10);
	DECLARE @LINE_BREAK			char		(    3 ); SET @LINE_BREAK	= '%' + @LF + '%';

	--! Working Values
	DECLARE @WorkingLength		bigint
	DECLARE @WorkingString		nvarchar		(  max )
	DECLARE @SubString			nvarchar		(  max )
	DECLARE @SubStringLength	bigint

	--! Validate/correct inputs
	SET @MaxPrintLength = COALESCE(NULLIF(@MaxPrintLength, 0), 4000)

	IF @MaxPrintLength > 4000
		BEGIN
			RAISERROR('The @MaxPrintLength value of %i is greater than the maximum length supported by PRINT for nvarchar strings (4000)', 17, 1, @MaxPrintLength);
			RETURN(60000);
		END

	IF @MaxPrintLength < 1
		BEGIN
			RAISERROR('The @MaxPrintLength must be greater than or equal to 1 but is %i', 17, 2, @MaxPrintLength);
			RETURN(60000);
		END

	--! Working variables
	DECLARE @InputLength bigint; SET @InputLength = LEN(@InputString);

	IF @InputLength = 0
		GOTO OnComplete;

	--!
	--! Our input string may contain either carriage returns, line feeds or both
	--! to separate printing lines so we need to standardise on one of these (LF)
	--!
	SET @WorkingString = REPLACE(REPLACE(@InputString, @CRLF, @LF), @CR, @LF);

	--!
	--! If there are line feeds we use those to break down the text
	--! into individual printed lines, otherwise we print it in
	--! bite-size chunks suitable for consumption by PRINT
	--!
	IF PATINDEX(@LINE_BREAK, @InputString) > 0

		BEGIN --[BREAK_BY_LINE_FEED]

			--! Add a line feed on the end so the final iteration works as expected
			SET @WorkingString	= @WorkingString + @LF;
			SET @WorkingLength	= LEN(@WorkingString);

			DECLARE @LineFeedPos bigint; SET @LineFeedPos = 0;

			WHILE @WorkingLength > 0
				BEGIN
					--!
					--! Get the position of the next line feed
					--!
					SET @LineFeedPos = PATINDEX(@LINE_BREAK, @WorkingString);

					IF @LineFeedPos > 0
						BEGIN
							SET @SubString			= SUBSTRING(@WorkingString, 1, @LineFeedPos - 1);
							SET @SubStringLength	= LEN(@SubString);

							--!
							--! If this string is too long for a single PRINT, we pass it back
							--! to PrintString which will process the string in suitably sized chunks
							--!
							IF LEN(@SubString) > @MaxPrintLength
								EXEC [log4].[PrintString] @InputString = @SubString
							ELSE
								PRINT @SubString;

							--! Remove the text we've just processed
							SET @WorkingLength	= @WorkingLength - @LineFeedPos;
							SET @WorkingString	= SUBSTRING(@WorkingString, @LineFeedPos + 1, @WorkingLength);
						END
				END

		END --[BREAK_BY_LINE_FEED]
	ELSE
		BEGIN --[BREAK_BY_LENGTH]
			--!
			--! If there are no line feeds we may have to break it down
			--! into smaller bit size chunks suitable for PRINT
			--!
			IF @InputLength > @MaxPrintLength
				BEGIN
					SET @WorkingString		= @InputString;
					SET @WorkingLength		= LEN(@WorkingString);
					SET @SubStringLength	= @MaxPrintLength;

					WHILE @WorkingLength > 0
						BEGIN
							SET @SubString			= SUBSTRING(@WorkingString, 1, @SubStringLength);
							SET @SubStringLength	= LEN(@SubString)

							--!
							--! If we still have text to process, set working values
							--!
							IF (@WorkingLength - @SubStringLength + 1) > 0
								BEGIN
									PRINT @SubString;
									--! Remove the text we've just processed
									SET @WorkingString	= SUBSTRING(@WorkingString, @SubStringLength + 1, @WorkingLength);
									SET @WorkingLength	= LEN(@WorkingString);
								END
						END
				END
			ELSE
				PRINT @InputString;

		END --[BREAK_BY_LENGTH]

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	SET NOCOUNT OFF

	RETURN

END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Prints the supplied string respecting all line feeds and/or carriage returns except where no line feeds are found, in which case the output is printed in user-specified lengths', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'PrintString';