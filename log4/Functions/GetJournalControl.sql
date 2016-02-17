/*************************************************************************************************/
--</MaintenanceHeader>

CREATE FUNCTION [log4].[GetJournalControl]
(
  @ModuleName		varchar	(  255 )
, @GroupName		varchar	(  255 )
)

RETURNS varchar		(  3 )

AS

--<CommentHeader>
/**************************************************************************************************

Properties
==========
FUNCTION NAME:      [log4].[GetJournalControl]
DESCRIPTION:		Returns the ON/OFF value for the specified Journal Name, or Group Name if
					Module not found or the system default if neither is found
DATE OF ORIGIN:		15-APR-2008
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
15-APR-2008		GML		v0.0.3		Created
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
	RETURN
		(
			SELECT
				TOP 1 OnOffSwitch
			FROM
				(
						SELECT
							  OnOffSwitch
							, 1 AS [Precedence]
						FROM
							[log4].[JournalControl]
						WHERE
							ModuleName = 'SYSTEM_OVERRIDE'
						AND
							OnOffSwitch = 'OFF' -- only care about the override when it's OFF
					UNION
						SELECT
							  OnOffSwitch
							, 10 AS [Precedence]
						FROM
							[log4].[JournalControl]
						WHERE
							ModuleName = @ModuleName
					UNION
						SELECT
							  OnOffSwitch
							, 100 AS [Precedence]
						FROM
							[log4].[JournalControl]
						WHERE
							ModuleName = @GroupName
					UNION
						SELECT
							  OnOffSwitch
							, 200 AS [Precedence]
						FROM
							[log4].[JournalControl]
						WHERE
							ModuleName = 'SYSTEM_DEFAULT'
					UNION
						SELECT
							  'OFF'		AS [OnOffSwitch]
							, 300		AS [Precedence]
				) AS [x]
			ORDER BY
				[Precedence] ASC
		)
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Returns the ON/OFF value for the specified Journal Name, or Group Name if Module not found or the system default if neither is found', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'FUNCTION', @level1name = N'GetJournalControl';