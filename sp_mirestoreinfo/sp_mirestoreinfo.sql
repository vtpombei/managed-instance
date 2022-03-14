/*
Description:
dbo.sp_mirestoreinfo is a stored procedure that returns information regarding restores from the error logs on an MI instance.
The procedure is based on the sp_readmierrorlog.sql stored procedure that can be found here: https://github.com/dimitri-furman/managed-instance/tree/master/sp_readmierrorlog.
The full error log remains available using the sys.sp_readerrorlog stored procedure.
Usage examples:
-- Return detailed restore information from all the databases regarding all the error log 
EXEC dbo.sp_mirestoreinfo
-- Return basic restore information from AdventureWorks database
EXEC dbo.sp_mirestoreinfo 'AdventureWorks', 'basic'
-- Return basic restore information from all databases after March 10,2021
EXEC dbo.sp_mirestoreinfo '', 'basic' , '2022-03-14 00:00:01.000', null
-- Return details restore information from AdventureWorks database between March 10,2021 and March 17, 2021
EXEC dbo.sp_mirestoreinfo 'AdventureWorks', 'detailed' , '2022-03-13 00:00:01.000', '2022-03-15 00:00:01.000'
*/



CREATE OR ALTER PROCEDURE [dbo].[sp_mirestoreinfo]
    @dbname nvarchar(4000) = NULL,
	@level varchar(8) = 'DETAILED',
	@startdate datetime = NULL,
	@enddate datetime = NULL
AS

SET NOCOUNT ON;

DECLARE @service_broker_guid NVARCHAR(4000) = NULL;

-- Parameter @level validation
IF UPPER(@level) != 'BASIC' AND UPPER(@level) != 'DETAILED'
BEGIN
 PRINT 'The Level parameter should be "BASIC" or "DETAILED".';
 RETURN(1);
END
ELSE
BEGIN
	SET @level = UPPER(@level);
END

--Dates parameters validation
IF @startdate IS NOT NULL AND @enddate IS NOT NULL AND @enddate < @startdate
BEGIN
 PRINT 'The end date parameter should be higher or equal than start date.';
 RETURN(1);
END

DECLARE @ErrorLog TABLE (
                        LogID INT NOT NULL IDENTITY(1,1),
                        LogDate DATETIME NOT NULL,
                        ProcessInfo NVARCHAR(50) NOT NULL,
                        LogText NVARCHAR(4000) NOT NULL,
                        PRIMARY KEY (LogDate, LogID)
                        );

DECLARE @output TABLE (
                        LogID INT NOT NULL,
                        LogDate DATETIME NOT NULL,
                        ProcessInfo NVARCHAR(50) NOT NULL,
                        LogText NVARCHAR(4000) NOT NULL,
						DatabaseName VARCHAR(100),
						RestoreType VARCHAR(4),
                        PRIMARY KEY (LogDate, LogID,DatabaseName)
                        );

IF (NOT IS_SRVROLEMEMBER(N'securityadmin') = 1) AND (NOT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') = 1)
BEGIN
    RAISERROR(27219,-1,-1);
    RETURN (1);
END;

-- If the database name is provided collect the service_broker_guid to be used to filter the error log
IF @dbname IS NOT NULL AND @dbname != ''
BEGIN
	SELECT @service_broker_guid = service_broker_guid FROM sys.databases WHERE [name] = @dbname
END

-- Get log filtered by "Restore(", the other parameters can be null
INSERT INTO @ErrorLog (LogDate, ProcessInfo, LogText)
EXEC sys.xp_readerrorlog 0, 1, N'Restore(', NULL, @startdate, @enddate;

INSERT INTO @ErrorLog (LogDate, ProcessInfo, LogText)
EXEC sys.xp_readerrorlog 0, 1, N'Calls Management Service to create a managed database', NULL, @startdate, @enddate;

INSERT INTO @output (LogID,LogDate,ProcessInfo,LogText,DatabaseName,RestoreType)
SELECT el.LogID, el.LogDate,
		el.ProcessInfo,
		LogText = IIF(d.name IS NULL, el.LogText, REPLACE(el.LogText COLLATE Latin1_General_100_CI_AS, d.physical_database_name, d.name)),
		DatabaseName = IIF(d.name IS NULL, 
						IIF(LogText LIKE 'Calls Management Service to create a managed database%',
							SUBSTRING(LogText, 55 , CHARINDEX('on a managed server:', LogText)-56),
							SUBSTRING(LogText, CHARINDEX('(', LogText)+1, CHARINDEX(')', LogText) - CHARINDEX('(', LogText)-1))
						, d.name),
		RestoreType = IIF(LogText LIKE 'Calls Management Service to create a managed database%','URL','')
	FROM @ErrorLog AS el
	LEFT JOIN sys.databases d 
	ON el.LogText COLLATE Latin1_General_100_CI_AS LIKE '%'+d.physical_database_name+'%'

IF UPPER(@level) = 'DETAILED'
BEGIN
	IF @dbname IS NOT NULL AND @dbname != ''
	BEGIN
		SELECT *
		FROM @output
		WHERE DatabaseName = @dbname
		ORDER BY LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
	ELSE
	BEGIN
		SELECT *
		FROM @output
		ORDER BY LogDate DESC, LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
END

IF UPPER(@level) = 'BASIC'
BEGIN
	IF @dbname IS NOT NULL AND @dbname != ''
	BEGIN
		SELECT *
		FROM @output
		WHERE DatabaseName = @dbname AND
		(LogText like '%RESTORE DATABASE started'
			OR CHARINDEX('%Acquiring',LogText) > 0
			OR CHARINDEX('%Effective options',LogText) > 0
			OR CHARINDEX('%Beginning OFFLINE restore',LogText) > 0
			OR CHARINDEX('%Attached database',LogText) > 0
			OR CHARINDEX('%Transferring',LogText) > 0
			OR CHARINDEX('%Waiting for log',LogText) > 0
			OR CHARINDEX('%Log zeroing',LogText) > 0
			OR CHARINDEX('%Backup set',LogText) > 0
			OR CHARINDEX('Processing',LogText) > 0
			OR CHARINDEX('LSN',LogText) > 0
			OR LogText like '%RESTORE DATABASE finished'
			OR CHARINDEX('Estimated total size',LogText) > 0
			OR CHARINDEX('percent',LogText) > 0)
		ORDER BY LogDate DESC, LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
	ELSE
	BEGIN
		SELECT *
		FROM @output
		WHERE LogText like '%RESTORE DATABASE started'
			OR CHARINDEX('%Acquiring',LogText) > 0
			OR CHARINDEX('%Effective options',LogText) > 0
			OR CHARINDEX('%Beginning OFFLINE restore',LogText) > 0
			OR CHARINDEX('%Attached database',LogText) > 0
			OR CHARINDEX('%Transferring',LogText) > 0
			OR CHARINDEX('%Waiting for log',LogText) > 0
			OR CHARINDEX('%Log zeroing',LogText) > 0
			OR CHARINDEX('%Backup set',LogText) > 0
			OR CHARINDEX('Processing',LogText) > 0
			OR CHARINDEX('LSN',LogText) > 0
			OR LogText like '%RESTORE DATABASE finished'
			OR CHARINDEX('Estimated total size',LogText) > 0
			OR CHARINDEX('percent',LogText) > 0
		ORDER BY LogDate DESC, LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
END
