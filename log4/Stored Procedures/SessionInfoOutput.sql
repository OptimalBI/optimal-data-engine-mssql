

/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[SessionInfoOutput]
(
  @SessionId          int
, @HostName           nvarchar ( 128 ) = NULL  OUT
, @ProgramName        nvarchar ( 128 ) = NULL  OUT
, @NTDomain           nvarchar ( 128 ) = NULL  OUT
, @NTUsername         nvarchar ( 128 ) = NULL  OUT
, @LoginName          nvarchar ( 128 ) = NULL  OUT
, @OriginalLoginName  nvarchar ( 128 ) = NULL  OUT
, @SessionLoginTime   datetime         = NULL  OUT
)

AS

--<CommentHeader>
/**********************************************************************************************************************

Properties
=====================================================================================================================
PROCEDURE NAME:  SessionInfoOutput
DESCRIPTION:     Outputs session info from master.sys.dm_exec_sessions for the current @@SPID
DATE OF ORIGIN:  15-APR-2008
ORIGINAL AUTHOR: Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:      13-MAR-2012
BUILD VERSION:   0.0.10
DEPENDANTS:      log4.ExceptionHandler
                 log4.JournalWriter
DEPENDENCIES:    Called functions

Returns
=====================================================================================================================
@@ERROR - always zero on success

Additional Notes
=====================================================================================================================


Revision history
=====================================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		=================================================================================
15-APR-2008		GML		vX.Y.z		Created
------------	------	-------		---------------------------------------------------------------------------------


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

	BEGIN TRY
		SELECT
			  @HostName				= s.[host_name]
			, @ProgramName			= s.[program_name]
			, @NTDomain				= s.nt_domain
			, @NTUsername			= s.nt_user_name
			, @LoginName			= s.login_name
			, @OriginalLoginName	= s.original_login_name
			, @SessionLoginTime		= s.login_time
		FROM
			master.sys.dm_exec_sessions AS [s] WITH (NOLOCK)
		WHERE
			s.session_id = @SessionId
	END TRY
	BEGIN CATCH
		--! Make sure we return non-null values
		SET @SessionId			= 0
		SET @HostName			= ''
		SET @ProgramName		= 'log4.SessionInfoOutput Error!'
		SET @NTDomain			= ''
		SET @NTUsername			= ''
		SET @LoginName			= 'log4.SessionInfoOutput Error!'
		SET @OriginalLoginName	= ''

		DECLARE @context nvarchar(512); SET @context = 'log4.SessionInfoOutput failed to retrieve session info';

		--! Only rollback if we have an uncommitable transaction
		IF (XACT_STATE() = -1)
		OR (@@TRANCOUNT > 0 AND XACT_STATE() != 1)
			BEGIN
				ROLLBACK TRAN;
				SET @context = @context + ' (Forced rolled back of all changes due to uncommitable transaction)';
			END

		--! Log this error directly
		--! Don't call ExceptionHandler in case we get another
		--! SessionInfoOutput error and and up in a never-ending loop)
		INSERT [log4].[Exception]
		(
		  [ErrorContext]
		, [ErrorNumber]
		, [ErrorSeverity]
		, [ErrorState]
		, [ErrorProcedure]
		, [ErrorLine]
		, [ErrorMessage]
		, [SessionId]
		, [ServerName]
		, [DatabaseName]
		)
		SELECT
			  @context
			, ERROR_NUMBER()
			, ERROR_SEVERITY()
			, ERROR_STATE()
			, ERROR_PROCEDURE()
			, ERROR_LINE()
			, ERROR_MESSAGE()
			, @@SPID
			, @@SERVERNAME
			, DB_NAME()
	END CATCH

	SET NOCOUNT OFF
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Outputs session info from master.sys.dm_exec_sessions for the current @@SPID', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'SessionInfoOutput';

