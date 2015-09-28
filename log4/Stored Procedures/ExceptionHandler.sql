/*************************************************************************************************/
--</MaintenanceHeader>

CREATE PROCEDURE [log4].[ExceptionHandler]
(
  @ErrorContext		nvarchar	(  512 )	= NULL
, @DatabaseName		nvarchar	(  128 )	= NULL	OUT
, @ErrorProcedure	nvarchar	(  128 )	= NULL	OUT
, @ErrorNumber		int						= NULL	OUT
, @ErrorSeverity	int						= NULL	OUT
, @ErrorState		int						= NULL	OUT
, @ErrorLine		int						= NULL	OUT
, @ErrorMessage		nvarchar	( 4000 )	= NULL	OUT
, @ReturnMessage	nvarchar	( 1000 )	= NULL	OUT
, @ExceptionId		int						= NULL	OUT
)
AS

--<CommentHeader>
/**********************************************************************************************************************

Properties
=====================================================================================================================
PROCEDURE NAME:		log4.ExceptionHandler
DESCRIPTION:		Returns error info as output parameters and writes info to Exception table
DATE OF ORIGIN:		01-DEC-2006
ORIGINAL AUTHOR:	Greg M. Lucas (data-centric solutions ltd. http://www.data-centric.co.uk)
BUILD DATE:			13-MAR-2012
BUILD VERSION:		0.0.10
DEPENDANTS:			Various
DEPENDENCIES:		log4.SessionInfoOutput

Outputs
=====================================================================================================================
Outputs all values collected within the CATCH block plus a formatted error message built from context and error msg

Returns
=====================================================================================================================
- @@ERROR - always zero on success


Additional Notes
=====================================================================================================================
-

Revision history
=====================================================================================================================
ChangeDate		Author	Version		Narrative
============	======	=======		=================================================================================
01-DEC-2006		GML		v0.0.1		Created
------------	------	-------		---------------------------------------------------------------------------------
15-APR-2008		GML		v0.0.3		Now utilises SessionInfoOutput sproc for session values
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
	SET NOCOUNT ON;

	SET @ErrorContext		= COALESCE(@ErrorContext, '');
	SET @DatabaseName		= COALESCE(@DatabaseName, DB_NAME());
	SET @ErrorProcedure		= COALESCE(NULLIF(@ErrorProcedure, ''), ERROR_PROCEDURE(), '');
	SET @ErrorNumber		= COALESCE(ERROR_NUMBER(), 0);
	SET @ErrorSeverity		= COALESCE(ERROR_SEVERITY(), 0);
	SET @ErrorState			= COALESCE(ERROR_STATE(), 0);
	SET @ErrorLine			= COALESCE(ERROR_LINE(), 0);
	SET @ErrorMessage		= COALESCE(ERROR_MESSAGE()
								, 'ERROR_MESSAGE() Not Found for @@ERROR: '
									+ COALESCE(CAST(ERROR_NUMBER() AS varchar(16)), 'NULL'));

	--!
	--! Generate a detailed, nicely formatted error message to return to the caller
	--!
	DECLARE @context nvarchar(512); SET @context = COALESCE(NULLIF(@ErrorContext, '') + ' due to ', 'ERROR! ');
	SET @ReturnMessage	= @context
						+ CASE
							WHEN LEN(ERROR_MESSAGE()) > (994 - LEN(@context))
								THEN '"' + SUBSTRING(@ErrorMessage, 1, (994 - LEN(@context))) + '..."'
							ELSE
								'"' + @ErrorMessage + '"'
						  END;

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

	--! Working variables
	DECLARE @tblExceptionId         table	(ExceptionId int NOT NULL UNIQUE);

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

	--!
	--! Record what we have
	--!
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
	, [HostName]
	, [ProgramName]
	, [NTDomain]
	, [NTUsername]
	, [LoginName]
	, [OriginalLoginName]
	, [SessionLoginTime]
	)
	OUTPUT inserted.ExceptionId INTO @tblExceptionId
	VALUES
	(
	  @ErrorContext
	, @ErrorNumber
	, @ErrorSeverity
	, @ErrorState
	, @ErrorProcedure
	, @ErrorLine
	, @ErrorMessage
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
	);

	SELECT @ExceptionId = ExceptionId FROM @tblExceptionId;

--/////////////////////////////////////////////////////////////////////////////////////////////////
OnComplete:
--/////////////////////////////////////////////////////////////////////////////////////////////////

	SET NOCOUNT OFF;

	RETURN;
END
GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Returns error info as output parameters and writes info to Exception table', @level0type = N'SCHEMA', @level0name = N'log4', @level1type = N'PROCEDURE', @level1name = N'ExceptionHandler';