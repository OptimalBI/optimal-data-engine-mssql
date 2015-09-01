

/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[JournalWriter]
(
  @FunctionName			varchar		(  256 )
, @MessageText			varchar		(  512 )
, @ExtraInfo			varchar		(  max )	= NULL
, @DatabaseName			nvarchar	(  128 )	= NULL
, @Task					nvarchar	(  128 )	= NULL
, @StepInFunction		varchar		(  128 )	= NULL
, @Severity				smallint				= NULL
, @ExceptionId			int						= NULL
, @JournalId			int						= NULL OUT
)

AS

/**************************************************************************************************

Properties
==========
PROCEDURE NAME:		[log4].[JournalWriter]
DESCRIPTION:		Adds a journal entry summarising task progress, completion or failure msgs etc.
DATE OF ORIGIN:		01-DEC-2006
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:			Various
DEPENDENCIES:		[log4].[SessionInfoOutput]
					[log4].[ExceptionHandler]

Returns
=======
@@ERROR - always zero on success

Additional Notes
================
Possible options for @Severity

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



Revision history
==================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		==============================================================
01-DEC-2006		GML		v0.0.1		Created
------------	------	-------		--------------------------------------------------------------
15-APR-2008		GML		v0.0.3		Now utilises [log4].[SessionInfoOutput] sproc for session values
------------	------	-------		--------------------------------------------------------------
03-MAY-2011		GML		v0.0.4		Added support for JournalDetail table
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
	SET NOCOUNT ON

	DECLARE @Error int; SET @Error = 0;

	--!
	--! Define input defaults
	--!
	SET @DatabaseName	= COALESCE(@DatabaseName, DB_NAME())
	SET @FunctionName	= COALESCE(@FunctionName, '')
	SET @StepInFunction	= COALESCE(@StepInFunction, '')
	SET @MessageText	= COALESCE(@MessageText, '')
	SET @ExtraInfo		= COALESCE(@ExtraInfo, '')
	SET @Task			= COALESCE(@Task, '')

	--! Make sure the supplied severity fits our bitmask model
	IF ISNULL(@Severity, 0) NOT IN (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096)
		BEGIN
			SET @ExtraInfo  = COALESCE(NULLIF(@ExtraInfo, '') + CHAR(13), '')
							+ '(Severity value: ' + COALESCE(CAST(@Severity AS varchar), 'NULL') + ' is invalid so using 256)'
			SET @Severity   = 256 -- Informational
		END

	--!
	--! Session variables (keep it SQL2005 compatible)
	--!
	DECLARE @SessionId	int					; SET @SessionId		= @@SPID;
	DECLARE @ServerName	nvarchar	( 128 )	; SET @ServerName		= @@SERVERNAME;

	--!
	--! log4.SessionInfoOutput variables
	--!
	DECLARE   @HostName				nvarchar	( 128 )
			, @ProgramName			nvarchar	( 128 )
			, @NTDomain				nvarchar	( 128 )
			, @NTUsername			nvarchar	( 128 )
			, @LoginName			nvarchar	( 128 )
			, @OriginalLoginName	nvarchar	( 128 )
			, @SessionLoginTime		datetime


	--!
	--! Get the details for the current session
	--!
	EXEC log4.SessionInfoOutput
			  @SessionId			= @SessionId
			, @HostName				= @HostName				OUT
			, @ProgramName			= @ProgramName			OUT
			, @NTDomain				= @NTDomain				OUT
			, @NTUsername			= @NTUsername			OUT
			, @LoginName			= @LoginName			OUT
			, @OriginalLoginName	= @OriginalLoginName	OUT
			, @SessionLoginTime		= @SessionLoginTime		OUT

	--! Working variables
	DECLARE @tblJournalId table	(JournalId int NOT NULL UNIQUE);

	BEGIN TRY
		INSERT [log4].[Journal]
		(
		  [Task]
		, [FunctionName]
		, [StepInFunction]
		, [MessageText]
		, [SeverityId]
		, [ExceptionId]
		------------------------
		, [SessionId]
		, [ServerName]
		, [DatabaseName]
		, [HostName]
		, [ProgramName]
		, [NTDomain]
		, [NTUsername]
		, [LoginName]
		, [OriginalLoginName]
		, [SessionLoginTime]
		)
	OUTPUT inserted.JournalId INTO @tblJournalId
	VALUES
		(
		  @Task
		, @FunctionName
		, @StepInFunction
		, @MessageText
		, @Severity
		, @ExceptionId
		------------------------
		, @SessionId
		, @ServerName
		, @DatabaseName
		, @HostName
		, @ProgramName
		, @NTDomain
		, @NTUsername
		, @LoginName
		, @OriginalLoginName
		, @SessionLoginTime
		)

		SELECT @JournalId = JournalId FROM @tblJournalId;

		INSERT [log4].[JournalDetail]
		(
		  JournalId
		, ExtraInfo
		)
		VALUES
		(
		  @JournalId
		, @ExtraInfo
		)

	END TRY
	BEGIN CATCH
		--!
		--! If we have an uncommitable transaction (XACT_STATE() = -1), if we hit a deadlock
		--! or if @@TRANCOUNT > 0 AND XACT_STATE() != 1, we HAVE to roll back.
		--! Otherwise, leaving it to the calling process
		--!
		IF (@@TRANCOUNT > 0 AND XACT_STATE() != 1) OR (XACT_STATE() = -1) OR (ERROR_NUMBER() = 1205)
			BEGIN
				ROLLBACK TRAN

				SET @MessageText    = 'Failed to write journal entry: '
									+ CASE
										WHEN LEN(@MessageText) > 440
											THEN '"' + SUBSTRING(@MessageText, 1, 440) + '..."'
										ELSE
											COALESCE('"' + @MessageText + '"', 'NULL')
										END
									+ ' (Forced roll back of all changes)'
			END
		ELSE
			BEGIN
				SET @MessageText    = 'Failed to write journal entry: '
									+ CASE
										WHEN LEN(@MessageText) > 475
											THEN '"' + SUBSTRING(@MessageText, 1, 475) + '..."'
										ELSE
											COALESCE('"' + @MessageText + '"', 'NULL')
										END
			END

		--! Record any failure info
		EXEC [log4].[ExceptionHandler]
				  @ErrorContext = @MessageText
				, @ErrorNumber  = @Error OUT
	END CATCH

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	SET NOCOUNT OFF

	RETURN(@Error)

END




GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Adds a journal entry summarising task progress, completion or failure msgs etc.', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'JournalWriter';

