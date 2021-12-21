/*
Description:
dbo.sp_mibackupinfo is a stored procedure that returns information regarding backups from the error logs on an MI instance.
The procedure is based on the sp_readmierrorlog.sql stored procedure that can be found here: https://github.com/dimitri-furman/managed-instance/tree/master/sp_readmierrorlog.
The full error log remains available using the sys.sp_readerrorlog stored procedure.
Usage examples:
-- Return detailed backup information from all the databases regarding all the error log 
EXEC dbo.sp_mibackupinfo
-- Return basic backup information from AdventureWorks database
EXEC dbo.sp_mibackupinfo 'AdventureWorks', 'basic'
-- Return basic backup information from all databases after March 10,2021
EXEC dbo.sp_mibackupinfo '', 'basic' , '2021-03-10 00:00:01.000', null
-- Return details backup information from AdventureWorks database between March 10,2021 and March 17, 2021
EXEC dbo.sp_mibackupinfo 'AdventureWorks', 'detailed' , '2021-03-10 00:00:01.000', '2021-03-17 00:00:01.000'
*/

CREATE OR ALTER PROCEDURE dbo.sp_mibackupinfo
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
						BackupType VARCHAR(4),
						RequestedBy VARCHAR(15),
                        PRIMARY KEY (LogDate, LogID)
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

-- Get log filtered by "Backup(", the other parameters can be null
INSERT INTO @ErrorLog (LogDate, ProcessInfo, LogText)
EXEC sys.xp_readerrorlog 0, 1, N'Backup(', NULL, @startdate, @enddate;

INSERT INTO @output (LogID,LogDate,ProcessInfo,LogText,DatabaseName,BackupType,RequestedBy)
SELECT el.LogID, el.LogDate,
		el.ProcessInfo,
		LogText = IIF(d.name IS NULL, el.LogText, REPLACE(el.LogText COLLATE Latin1_General_100_CI_AS, d.physical_database_name, d.name)),
		DatabaseName = IIF(d.name IS NULL, SUBSTRING(LogText, CHARINDEX('(', LogText)+1, CHARINDEX(')', LogText) - CHARINDEX('(', LogText)-1), d.name),
		DatabaseType = IIF(el.LogText LIKE '%BACKUP DATABASE started','FULL',IIF(el.LogText LIKE '%BACKUP DATABASE WITH DIFFERENTIAL started','DIFF',IIF(el.LogText LIKE '%BACKUP LOG started','LOG',''))),
		RequestedBy = IIF(el.LogText LIKE '%BACKUP DATABASE started',IIF(d.name IS NULL, 'User Requested', 'Automated'),'')
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
		ORDER BY LogID DESC
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
		(LogText like '%BACKUP LOG started'
			OR LogText like '%BACKUP LOG finished'
			OR LogText like '%BACKUP DATABASE WITH DIFFERENTIAL started'
			OR LogText like '%BACKUP DATABASE WITH DIFFERENTIAL finished'
			OR LogText like '%BACKUP DATABASE started'
			OR LogText like '%BACKUP DATABASE finished'
			OR CHARINDEX('Estimated total size',LogText) > 0
			OR CHARINDEX('percent',LogText) > 0)
		ORDER BY LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
	ELSE
	BEGIN
		SELECT *
		FROM @output
		WHERE LogText like '%BACKUP LOG started'
			OR LogText like '%BACKUP LOG finished'
			OR LogText like '%BACKUP DATABASE WITH DIFFERENTIAL started'
			OR LogText like '%BACKUP DATABASE WITH DIFFERENTIAL finished'
			OR LogText like '%BACKUP DATABASE started'
			OR LogText like '%BACKUP DATABASE finished'
			OR CHARINDEX('Estimated total size',LogText) > 0
			OR CHARINDEX('percent',LogText) > 0
		ORDER BY LogID DESC
		OPTION (RECOMPILE, MAXDOP 1);
	END
END
