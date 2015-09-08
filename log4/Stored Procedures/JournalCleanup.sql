/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[JournalCleanup]
(
  @DaysToKeepJournal            int
, @DaysToKeepException			int
)

AS

/**************************************************************************************************

Properties
==========
PROCEDURE NAME:		[log4].[JournalCleanup]
DESCRIPTION:		Deletes all Journal and Exception entries older than the specified days
DATE OF ORIGIN:		16-FEB-2007
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

Revision history
==================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		==============================================================
16-FEB-2007		GML		v0.0.2		Created
------------	------	-------		--------------------------------------------------------------
29-AUG-2011		GML		v0.0.7		Added support for ExceptionId (now ensures that Exception
									deleted date is greater than Journa delete date)
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

	--! Standard/common variables
	DECLARE	  @_Error					int
			, @_RowCount				int
			, @_DatabaseName			nvarchar	(  128 )
			, @_DebugMessage			varchar		( 2000 )
			, @_SprocStartTime			datetime
			, @_StepStartTime			datetime

	--! WriteJournal variables
	DECLARE   @_FunctionName			varchar		(  256 )
			, @_Message					varchar		(  512 )
			, @_ProgressText			nvarchar	(  max )
			, @_Step					varchar		(  128 )
			, @_Severity				smallint

	--! ExceptionHandler variables
	DECLARE   @_CustomErrorText			varchar		(  512 )
			, @_ErrorMessage			varchar		( 4000 )
			, @_ExceptionId				int

	--! Common Debug variables
	DECLARE	  @_LoopStartTime			datetime
			, @_StepEndTime				datetime
			, @_CRLF					char		(    1 )

	--! Populate the common variables
	SET @_SprocStartTime	= GETDATE()
	SET @_FunctionName		= OBJECT_NAME(@@PROCID)
	SET @_DatabaseName		= DB_NAME()
	SET @_Error				= 0
	SET @_Severity			= 256 -- Informational
	SET @_CRLF				= CHAR(10)
	SET @_DebugMessage		= @_FunctionName + ' starting at ' + CONVERT(char(23), @_SprocStartTime, 121) + ' with inputs: '
							+ @_CRLF + '    @DaysToKeepJournal     : ' + COALESCE(CAST(@DaysToKeepJournal AS varchar), 'NULL')
							+ @_CRLF + '    @DaysToKeepException   : ' + COALESCE(CAST(@DaysToKeepException AS varchar), 'NULL')
	SET @_ProgressText		= @_DebugMessage

	--! Define our working values
	DECLARE @_DaysToKeepJournal		int;		SET @_DaysToKeepJournal = COALESCE(@DaysToKeepJournal, 30)
	DECLARE @_DaysToKeepException	int;		SET @_DaysToKeepException = COALESCE(@DaysToKeepException, @_DaysToKeepJournal + 1)
	DECLARE @_JournalArchiveDate	datetime;	SET @_JournalArchiveDate = CONVERT(char(11), DATEADD(day, - @_DaysToKeepJournal, GETDATE()), 113)
	DECLARE @_ExceptionArchiveDate	datetime;	SET @_ExceptionArchiveDate = CONVERT(char(11), DATEADD(day, - @_DaysToKeepException, GETDATE()), 113)

	SET @_ProgressText		= @_ProgressText
							+ @_CRLF + 'and working values...'
							+ @_CRLF + '    @_DaysToKeepJournal     : ' + COALESCE(CAST(@_DaysToKeepJournal AS varchar), 'NULL')
							+ @_CRLF + '    @_DaysToKeepException   : ' + COALESCE(CAST(@_DaysToKeepException AS varchar), 'NULL')
							+ @_CRLF + '    @_JournalArchiveDate   : ' + COALESCE(CONVERT(char(19), @_JournalArchiveDate, 120), 'NULL')
							+ @_CRLF + '    @_ExceptionArchiveDate : ' + COALESCE(CONVERT(char(19), @_ExceptionArchiveDate, 120), 'NULL')

	--!
	--!
	--!
	BEGIN TRY
		SET @_Step = 'Validate inputs';

		--!
		--! There is an FK between Journal and Exception so we can't delete more from Exception
		--! than we do from Journal
		--!
		IF @_JournalArchiveDate >= @_ExceptionArchiveDate
			BEGIN
				SET @_Message	= 'Failed to clean up Journal and Exception tables as Journal delete Date: '
								+ COALESCE(CONVERT(char(19), @_JournalArchiveDate, 120), 'NULL')
								+ ' must be less than Exception delete date: '
								+ COALESCE(CONVERT(char(19), @_ExceptionArchiveDate, 120), 'NULL')
				RAISERROR(@_Message, 16, 1);
			END

		SET @_Step = 'Delete old Journal entries';
		SET @_StepStartTime = GETDATE();

		BEGIN TRAN

		--! Don't need to DELETE JournalDetail as FK cascades
		DELETE
			[log4].[Journal]
		WHERE
			SystemDate < @_JournalArchiveDate

		SET @_RowCount		= @@ROWCOUNT;
		SET @_DebugMessage	= 'Completed step: "' +  COALESCE(@_Step, 'NULL') + '"'
							+ ' in ' + [log4].[FormatElapsedTime](@_StepStartTime, NULL, 3)
							+ ' ' + COALESCE(CAST(@_RowCount AS varchar), 'NULL') + ' row(s) affected'
		SET @_ProgressText	= @_ProgressText + @_CRLF + @_DebugMessage

		IF  @@TRANCOUNT > 0 COMMIT TRAN
	END TRY
	BEGIN CATCH
		IF ABS(XACT_STATE()) = 1 OR @@TRANCOUNT > 0 ROLLBACK TRAN;

		SET @_CustomErrorText	= 'Failed to cleanup Journal and Exception at step: ' + COALESCE(@_Step, 'NULL')

		EXEC [log4].[ExceptionHandler]
				  @DatabaseName    = @_DatabaseName
				, @ErrorContext    = @_CustomErrorText
				, @ErrorProcedure  = @_FunctionName
				, @ErrorNumber     = @_Error OUT
				, @ReturnMessage   = @_Message OUT
				, @ExceptionId     = @_ExceptionId OUT

		GOTO OnComplete;
	END CATCH

	--!
	--!
	--!
	BEGIN TRY
		SET @_Step = 'Delete old Exception entries';
		SET @_StepStartTime = GETDATE();

		BEGIN TRAN

		DELETE
			[log4].[Exception]
		WHERE
			SystemDate < @_ExceptionArchiveDate

		SET @_RowCount		= @@ROWCOUNT;
		SET @_DebugMessage	= 'Completed step: "' +  COALESCE(@_Step, 'NULL') + '"'
							+ ' in ' + [log4].[FormatElapsedTime](@_StepStartTime, NULL, 3)
							+ ' ' + COALESCE(CAST(@_RowCount AS varchar), 'NULL') + ' row(s) affected'
		SET @_ProgressText	= @_ProgressText + @_CRLF + @_DebugMessage

		IF  @@TRANCOUNT > 0 COMMIT TRAN

		SET @_Message		= 'Completed all Journal and Exception cleanup activities;'
							+ ' retaining ' + COALESCE(CAST(@DaysToKeepJournal AS varchar), 'NULL') + ' days'' Journal entries'
							+ ' and ' + COALESCE(CAST(@DaysToKeepException AS varchar), 'NULL') + ' days'' Exception entries'
	END TRY
	BEGIN CATCH
		IF ABS(XACT_STATE()) = 1 OR @@TRANCOUNT > 0 ROLLBACK TRAN;

		SET @_CustomErrorText	= 'Failed to cleanup Journal and Exception at step: ' + COALESCE(@_Step, 'NULL')

		EXEC [log4].[ExceptionHandler]
				  @DatabaseName    = @_DatabaseName
				, @ErrorContext    = @_CustomErrorText
				, @ErrorProcedure  = @_FunctionName
				, @ErrorNumber     = @_Error OUT
				, @ReturnMessage   = @_Message OUT
				, @ExceptionId     = @_ExceptionId OUT

		GOTO OnComplete;
	END CATCH


--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	IF @_Error = 0
		BEGIN
			SET @_Step			= 'OnComplete'
			SET @_Severity		= 512 -- Success
			SET @_Message		= COALESCE(@_Message, @_Step) + ' in a total run time of ' + [log4].[FormatElapsedTime](@_SprocStartTime, NULL, 3)
		END
	ELSE
		BEGIN
			SET @_Step			= COALESCE(@_Step, 'OnError')
			SET @_Severity		= 2 -- Severe Failure
			SET @_Message		= COALESCE(@_Message, @_Step) + ' after a total run time of ' + [log4].[FormatElapsedTime](@_SprocStartTime, NULL, 3)
		END

	--! Always log completion of this call
	EXEC [log4].[JournalWriter]
			  @FunctionName		= @_FunctionName
			, @StepInFunction	= @_Step
			, @MessageText		= @_Message
			, @ExtraInfo		= @_ProgressText
			, @DatabaseName		= @_DatabaseName
			, @Severity			= @_Severity
			, @ExceptionId		= @_ExceptionId

	--! Finaly, throw an exception that will be detected by SQL Agent
	IF @_Error > 0 RAISERROR(@_Message, 16, 1);

	SET NOCOUNT OFF;

	RETURN (@_Error);
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Deletes all Journal and Exception entries older than the specified days', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'JournalCleanup';