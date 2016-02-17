/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[JournalPrinter]
(
  @JournalId		int
)

AS

/**************************************************************************************************

Properties
==========
PROCEDURE NAME:		[log4].[JournalPrinter]
DESCRIPTION:		Prints the contents of JournalDetail for the specified Journal ID respecting all
					line feeds and/or carriage returns
DATE OF ORIGIN:		03-MAY-2011
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:			None
DEPENDENCIES:		None

Inputs
======
@JournalId - if -1, just processes any provided input string
@InputString - optional, the string to print

Outputs
=======
None

Returns
=======
NULL

Additional Notes
================

Revision history
==================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		==============================================================
03-MAY-2011		GML		v0.0.4		Created
------------	------	-------		--------------------------------------------------------------
05-NOV-2011		GML		v0.0.8		Now calls log4.PrintString (which is SQL2005 compatible)
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

	--! Working Values
	DECLARE @WorkingString		varchar		(  max )

	SELECT @WorkingString = ExtraInfo FROM [log4].[JournalDetail] WHERE JournalId = @JournalId

	IF COALESCE(@WorkingString, '') = ''
		BEGIN
			RAISERROR('No Extra Info for Journal ID: %d!', 0, 1, @JournalId);
		END
	ELSE
		BEGIN
			PRINT '';
			PRINT REPLICATE('=', 120);

			EXEC [log4].[PrintString] @WorkingString

			PRINT '';
			PRINT REPLICATE('=', 120);
			RAISERROR('Completed processing journal detail for Journal ID: %d', 0, 1, @JournalId) WITH NOWAIT;
		END

	SET NOCOUNT OFF;

	RETURN;
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Prints the contents of JournalDetail for the specified Journal ID respecting all line feeds and/or carriage returns', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'JournalPrinter';