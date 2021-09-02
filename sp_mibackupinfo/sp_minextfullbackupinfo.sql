/*
Description:
dbo.sp_minextfullbackupinfo is a stored procedure that returns information regarding when the next full backup will probably happen based on the error logs of the MI instance.
The NextFullBackupDate is calculated based on the previous full backup obtained from the errorLog
Startdate and Enddate specify the period the query will return full backups than happen in it
The name of deleted database will show as a GUID and the NextFullBackupDate will be null
The procedure is based on the sp_readmierrorlog.sql stored procedure that can be found here: https://github.com/dimitri-furman/managed-instance/tree/master/sp_readmierrorlog.
The full error log remains available using the sys.sp_readerrorlog stored procedure.

Usage examples:
-- Return all the last full backups for all the databases and the next probable full backup date
EXEC dbo.sp_minextfullbackupinfo
-- Return all the last full backups for AdventureWorks the database and the next probable full backup date
EXEC dbo.sp_minextfullbackupinfo 'AdventureWorks'
-- Return all the full backups taken after August 25, 2021 for all the databases and the next probable full backup date
EXEC dbo.sp_minextfullbackupinfo '', '2021-08-25 00:00:01.000', null
-- Return all the full backups from AdventureWorks database taken between August 01,2021 and September 01, 2021
EXEC dbo.sp_minextfullbackupinfo 'AdventureWorks', '2021-08-01 00:00:01.000', '2021-09-01 00:00:01.000'
*/

CREATE OR ALTER PROCEDURE dbo.sp_minextfullbackupinfo
    @dbname nvarchar(4000) = NULL,
	@startdate datetime = NULL, 
	@enddate datetime = NULL
AS

SET NOCOUNT ON;

DECLARE @service_broker_guid NVARCHAR(4000) = NULL;

DECLARE @ErrorLog TABLE (
                        LogID INT NOT NULL IDENTITY(1,1),
                        LogDate DATETIME NOT NULL,
                        ProcessInfo NVARCHAR(50) NOT NULL,
                        LogText NVARCHAR(4000) NOT NULL,
                        PRIMARY KEY (LogDate, LogID)
                        );

IF (NOT IS_SRVROLEMEMBER(N'securityadmin') = 1) AND (NOT HAS_PERMS_BY_NAME(NULL, NULL, 'VIEW SERVER STATE') = 1)
BEGIN
    RAISERROR(27219,-1,-1);
    RETURN (1);
END;

-- If startdate is null assign the date 7 days ago to get all the full backups
IF @startdate IS NULL
BEGIN
	SET @startdate = DATEADD(d, -7, GETDATE());
END

-- If the database name is provided collect the service_broker_guid to be used to filter the error log
IF @dbname IS NOT NULL or @dbname = ''
BEGIN
	SELECT @service_broker_guid = service_broker_guid FROM sys.databases WHERE [name] = @dbname
END

-- Get log filtered by "Backup(", the other parameters can be null
INSERT INTO @ErrorLog (LogDate, ProcessInfo, LogText)
EXEC sys.xp_readerrorlog 0,1,N'Backup(',@service_broker_guid,@startdate,@enddate;

SELECT FullBackupDate = el.LogDate,
	DatabaseName = IIF(d.name IS NULL, SUBSTRING(LogText, CHARINDEX('(', LogText)+1, CHARINDEX(')', LogText) - CHARINDEX('(', LogText)-1), d.name),
	NextFullBackupDate = IIF(d.name IS NULL, NULL, DATEADD(d,7,el.LogDate))
FROM @ErrorLog AS el
LEFT JOIN sys.databases d 
ON el.LogText COLLATE Latin1_General_100_CI_AS LIKE '%'+d.physical_database_name+'%'
WHERE LogText like '%BACKUP DATABASE started'
ORDER BY el.LogDate DESC,
	el.LogID
OPTION (RECOMPILE, MAXDOP 1);
