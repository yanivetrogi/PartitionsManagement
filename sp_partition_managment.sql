USE [master]
GO
/****** Object:  StoredProcedure [dbo].[sp_partition_managment]    Script Date: 16/02/2023 18:11:06 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
ALTER PROCEDURE [dbo].[sp_partition_managment]
(
  @partition_function sysname
 ,@days_to_keep_data int 
 ,@num_future_partitions int 
 ,@file_group_physical_path varchar(1024)
 ,@is_bcp bit 
 ,@bcp_path varchar(1024) 
 ,@partition_boundry_unit sysname
 ,@create_empty_table bit 
 ,@create_file_group bit
 ,@is_merge bit = 1
 ,@is_split bit = 1
 ,@debug bit = 1
)
AS

/*
Sliding Window Datetime Partitions Range Write supporting 6 different date parts: 
1 - Yearly
2 - Quarterly
3 - Monthly
4 = Weekly
5 - Daily
5 - Hourly


The script does the following steps:
1.	Verifies the correct number of partitions exist in the tail as defined by @days_to_keep_data
2.	Verifies the partitons have no gaps in the tail (partitions older than the current partition), each partition is a single boundry a head of the previouse partition 
		a.	Detected gaps in the tail are not being handled since these partitions are populated with historical data and creating 
				a new empty partition to close the gap will have no effect and result in data movement

3.	Loop and Switch partitions older than the @retention_date (current_time - @days_to_keep_data)
4.	Loop and Merge the partitions we have Switched Out
5.	Remove data files and file groups of the partitions we Switched out and Merged

6.	Verifies the correct number of partitions exist in the head (partitions newer than the current partition) as defined by the @num_future_partitions input parameter
7.	Verifies the partitons have no gaps, each partition is a single boudry unit a head of the previouse partition 
		a.	A detected gap is not being closed but only printed as information 

8.	Create a file group and a data file for the next partiton Split if @create_file_group is True
9.	Loop and Split partitions so that we always have the required number of partitons as defined by the @num_future_partitions input parameter a head of the current time


Input parameters:
	@days_to_keep_data			- the number of days to keep data before it is purged
	@num_future_partitions	- the number of future empty prtitions to maintain a head
	@debug = 0 will execute and print; @debug = 1 will print only

  @schema sysname - table schema
 ,@table sysname  - the table to process
 ,@empty_table sysname - the empty table we use for the switch out
 ,@days_to_keep_data int - number of days to keep data in table before we purge
 ,@num_future_partitions int - the number of future partrtions we maintain in the fead
 ,@file_group_physical_path varchar(1024) - the location to create new data files
 ,@is_bcp bit  - when this option is on we bcp out the data prior to the purge
 ,@bcp_path varchar(1024) - the path to export the data using bcp out
 ,@partition_boundry_unit sysname - i.e. Monthly, Daily
 ,@create_empty_table bit - if an empty table cannot be created (when there are read_only file groups) then we drop and create an empty table per each file group
 ,@create_file_group - when this option is on we create a dedicated data file and file group per each partition
 ,@is_merge - when this option is on we Merge partitions whos data we Switched Out and truncated. This option was added to allow data purge in enviroments where the Merge statment which is a DDL command and requires a schema stability lock that has lead to blocking
 ,@is_split - when this option is on we Split new partitions in the head is derived from the @num_future_partitions 
 ,@debug bit - when on then print only and nothing gets executed 
							 when off then prints and executes and logs	



Execution Example:
USE db;
DECLARE
  @partition_function sysname
 ,@days_to_keep_data int 
 ,@num_future_partitions int 
 ,@file_group_physical_path varchar(1024)
 ,@is_bcp bit 
 ,@bcp_path varchar(1024) 
 ,@partition_boundry_unit sysname
 ,@create_empty_table bit 
 ,@create_file_group bit
 ,@is_merge bit
 .@is_spit bit
 ,@debug bit;

 SELECT
  @partition_function				= 'pf_name'
 ,@file_group_physical_path	=	'W:\Data'
 ,@partition_boundry_unit		= 'DAY'
 ,@days_to_keep_data				= 1700 --< edit here
 ,@num_future_partitions		= 0
 ,@create_empty_table				= 1
 ,@create_file_group				= 1
 ,@is_bcp										= 0
 ,@bcp_path									= 'Z:\bcp'
 ,@is_merge									= 0
 ,@is_split									= 1
 ,@debug										= 1 ;


Histrory
28/01/2016: Modify the SWITCH command to use partition number instead of $PARTITION

09/05/2016: Add the condition "AND prv.value IS NOT NULL;" to the part that asigns the variable @part_key_len
						Add the condition "AND value IS NOT NULL" at the Split section when asigning the @command variable based on #partitions_for_split



02/11/2016	Add SET LOCK_TIMEOUT to prevent a case where the procedure waits to get a schema stability lock required for the MERGE and SPLIT commands
						and while it is waiting in line to acquire that lock on the required resource in order to proceed it blocks all other processes
						waiting also to acquire a lock on the same resource
						Add input parameter @PartitionNumber to the storde procedure sp_generate_table_script executed by sp_partition_managment
						This is to allow retrieval of the DATA_COMPRESSION property at the partition value

29/11/2016	Add 2 input parameters: @is_merge + @is_split to allow flexibility and control if we Merge and Split the partition function.
						The trigger behind is that I have come across a case where in a very busy OLTP (online gaming) the Merge blocked many processes 
						resulting in a severe blocking chain.
						This is because the Merge and Split commands which are DDL commands acquire a schema lock (LCK_M_SCH_S) and therefore processes 
						running in the READ UNCOMMITTED (NOLOCK) isolation level still get blocked.
						This flexibility allows to maintain the data purge policy (SWITCH OUT and TRUNCATE) on a daily basis while performing 
						the Merge and Split in a manually controlled execution (or possibly during a downtime)

07/12/2016	Add THROW to the CATCH block so that when the procedure fails the calling batch also fails and causes the job complete with error

08/02/2017	Add the -1 to the following at 3 locations: ignore the last partition (MAX(id) -1) in order to prevent data movement
						SET DEADLOCK_PRIORITY LOW;
						Modify the @counter parameter from 3 to 5 for the Merge command so that there will be 5 attempts to MERGE instead of 3

25/04/2017	Add logging at the Mereg and Split loops

04/09/2018 Yaniv Etrogi 
			1.	Add a predicate to ignore the max id in table #partitions_newer_than_current_time when the datetime vaue is far in the future
						We Disregard the last row in the table as it resulted in an invalid future date (2100-01-01 00:00:00.000)
						Modified code:						
						SELECT @current_max_boundry = CASE 
						WHEN (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time ) > DATEADD(DAY, 1000, CURRENT_TIMESTAMP) THEN (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time WHERE id = (SELECT MAX(id)-1 FROM dbo.#partitions_newer_than_current_time ))
						ELSE (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time ) END;

			2.	If there are no rows in table #partitions_newer_than_current_time resulting in a NULL then we set the next day as the earliest boundry to start for the SPLIT operation
						We add an extra day to prevent data movment in case the last partition is already full. 
						Modified code:		
						IF @current_max_boundry IS NULL SELECT @current_max_boundry = CURRENT_TIMESTAMP + 1


*/

SET NOCOUNT ON;
SET TRAN ISOLATION LEVEL READ UNCOMMITTED;
SET DEADLOCK_PRIORITY LOW;
SET LOCK_TIMEOUT 4000;

DECLARE @ErrorNumber int, @ErrorLine int, @Msg xml, @ErrorProc sysname, @CurrentTime datetime, @ErrorMessage nvarchar(2048),@ErrorSeverity int, @ErrorState int; 

DECLARE @retention_date datetime, @current_time datetime, @partition_schema sysname, @server sysname, @database sysname, @bcp_value sysname;
DECLARE @file_group_name_wildcard sysname, @rc int, @command varchar(max), @ident int, @value sysname, @data_file sysname, @physical_name varchar(2048), @value_datetime datetime, @new_value sysname;
DECLARE @max_id int, @min_id int, @table_lock bit = 0;
DECLARE @operation varchar(64), @size_mb bigint, @partition_number varchar(5), @files_spaceused_mb bigint, @partition_number_int int;
DECLARE  @schema sysname, @table sysname, @empty_table sysname, @counter int = 1;


--SELECT  @file_group_name_wildcard = 'FG_' + @table + '_*'; Yaniv Etrogi 20160327 - Moved this asinmnet latter on after the @table is initiated because it resulted in null
SELECT  @server = @@SERVERNAME, @database = DB_NAME();
 

BEGIN TRY;


IF NOT EXISTS(SELECT * FROM sys.partition_functions WHERE name = @partition_function)
BEGIN;	
	RAISERROR('The input parameter @partition_function defines a value: ''%s'' that does not exist in the database', 16, 1, @partition_function);
END;

-- Add a back slash if needed
IF (RIGHT(@bcp_path, 1) <> '\') SELECT @bcp_path = @bcp_path + '\';

-- Get the date part only
SELECT @current_time = DATEADD(DAY, DATEDIFF(DAY , 0, CURRENT_TIMESTAMP), 0);

-- We maintain xx partitons back before purging
SELECT @retention_date = DATEADD(DAY, -@days_to_keep_data, @current_time);


--SELECT @retention_date
PRINT	'-- Current_time = ' + + CONVERT(varchar(23), @current_time, 121) + '  | ' + 'Retention_date = ' + CONVERT(varchar(23), @retention_date, 121) + '  |   partition_boundry_unit: ' +  @partition_boundry_unit;

-- Log process start
IF @debug = 0
BEGIN;
	SELECT @operation = 'Start';
	INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
	SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, NULL,NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;
END;

/*
-- Aquire a table lock to prevent blocking and deadlocking
IF @table_lock = 1
BEGIN;
	SELECT @command = 'DECLARE @rc int; SELECT TOP 1 @rc = 0 FROM [' + @schema + '].[' + @table + '] WITH (TABLOCKX, HOLDLOCK);';
	PRINT @command;
	IF @debug = 0 
	BEGIN;
			WHILE 1=1
				BEGIN;
					--SET LOCK_TIMEOUT 1000
					EXEC (@command);

					-- If we are granted the required lock then exit here
					IF EXISTS (SELECT * FROM sys.dm_tran_locks 
						WHERE request_session_id = @@SPID AND resource_type IN (N'OBJECT', N'LOCK') AND request_mode = N'X' AND request_status = N'GRANT' ) 
					BEGIN; PRINT '-- Table Lock aquired' BREAK; END;

					WAITFOR DELAY '00:00:05';
			END;
	END;
END;
*/


-- Get the partition function and partition schema names based on the table schema and name
SELECT
	  @partition_schema		= ps.name
FROM sys.partition_schemes ps 
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
WHERE pf.name = @partition_function;
--SELECT @partition_function, @partition_schema

-- Get all tables using the partition function and partition schema
IF OBJECT_ID('tempdb.dbo.#partitioned_tables ', 'U') IS NOT NULL DROP TABLE dbo.#partitioned_tables;
CREATE TABLE dbo.#partitioned_tables(id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, [schema] sysname, [table] sysname);

INSERT #partitioned_tables([schema], [table])
SELECT SCHEMA_NAME(o.schema_id), o.name 
FROM sys.objects o
JOIN sys.indexes i ON (i.object_id = o.object_id AND i.index_id IN (0,1))
JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
WHERE pf.name = @partition_function
AND o.name NOT LIKE '%_Empty'-- Reserved naming convention
AND o.is_ms_shipped = 0 -- eliminate objects belonging to the sys schema such as: xml_index_nodes_90483401_256000
--SELECT * FROM #partitioned_tables;

/*add by saby 09/10/2016*/; 
-- check if there any tables ???
IF NOT EXISTS(SELECT 1 FROM #partitioned_tables)
BEGIN;	
	RAISERROR('The input parameter @partition_function defines a value: ''%s'' that does not have any tables connected to it', 16, 1, @partition_function);
END;

SELECT @table = (SELECT [table] FROM dbo.#partitioned_tables WHERE id = 1);
SELECT @file_group_name_wildcard = 'FG_' + @table + '_*';


-- Get the partitioning column data type and length
-- datetime = 0, int = 1, bigint = 2
DECLARE @part_key_data_type tinyint, @part_key_len tinyint;
SELECT TOP 1
	 @part_key_data_type = CASE WHEN ty.name IN ('int') THEN 1 WHEN ty.name IN ('bigint') THEN 2 WHEN ty.name IN ('datetime', 'datetime2', 'smalldatetime', 'date') THEN 0 ELSE 999 END
FROM sys.indexes i 
INNER JOIN sys.partitions p ON p.index_id = i.index_id AND p.object_id = i.object_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
INNER JOIN sys.types ty ON pp.system_type_id = ty.system_type_id AND pp.user_type_id = ty.user_type_id
WHERE pf.name = @partition_function;
--SELECT @part_key_data_type


--SELECT @part_key_data_type;
IF (@part_key_data_type NOT IN (0,1,2) OR @part_key_data_type IS NULL)
BEGIN; 
PRINT 'The table is not partitioned or Unsupported partitioning key data type: ' + ISNULL(CAST(@part_key_data_type AS sysname), 'NULL') + '. Supported types are datetime + smalldatetime + date (0), int (1), bigint (2). Terminating. '; 
RETURN; 
END;
--BEGIN; RAISERROR (N'Unsupported partitioning key data type: %d. Supported types are int, bigint, datetime. Terminating.', 17,1, @part_key_data_type) WITH NOWAIT;  END;


-- Get the partitioning column length
SELECT TOP 1
	@part_key_len = CASE WHEN @part_key_data_type IN (1, 2) THEN LEN(CAST(prv.value AS int)) WHEN @part_key_data_type = 0 THEN LEN(CAST(prv.value AS datetime)) END
FROM sys.objects o
INNER JOIN sys.indexes i ON (i.object_id = o.object_id AND i.index_id	IN (0, 1) )
INNER JOIN sys.partitions p ON p.index_id = i.index_id AND p.object_id = i.object_id
INNER JOIN sys.partition_schemes ps ON i.data_space_id = ps.data_space_id
INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND p.partition_number = prv.boundary_id
INNER JOIN sys.partition_parameters pp ON pf.function_id = pp.function_id
INNER JOIN sys.types ty ON pp.system_type_id = ty.system_type_id AND pp.user_type_id = ty.user_type_id
WHERE pf.name = @partition_function
AND prv.value IS NOT NULL;
--SELECT @part_key_len



/****************************************************************************************************/
/*	Begin section older than @current_time	*/
/****************************************************************************************************/
-- Get all the values of partions older than current time 
IF OBJECT_ID('tempdb.dbo.#partitions_older_than_current_time ', 'U') IS NOT NULL DROP TABLE dbo.#partitions_older_than_current_time;
CREATE TABLE dbo.#partitions_older_than_current_time (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, value datetime);

INSERT dbo.#partitions_older_than_current_time (value)	
SELECT 
	CASE WHEN @part_key_data_type = 0			 THEN CAST(prv.value AS datetime) 
			 WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime(CAST(prv.value AS int)) END 	
FROM sys.partition_functions pf
INNER JOIN sys.partition_range_values prv ON prv.function_id = pf.function_id
WHERE pf.name = @partition_function 
AND CASE WHEN @part_key_data_type = 0			 THEN CAST(prv.value AS datetime) 
				 WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime(CAST(prv.value AS int)  )
		END < @retention_date
--SELECT * FROM #partitions_older_than_current_time


-- Verify that the diff between each value is 1 boundry
IF EXISTS (SELECT * FROM dbo.#partitions_older_than_current_time t1 
								INNER JOIN dbo.#partitions_older_than_current_time t2 ON t1.id + 1 = t2.id 
																										--AND DATEDIFF(DAY, t1.value, t2.value) <> 1)
	AND CASE  WHEN @partition_boundry_unit = 'YEAR'			THEN DATEDIFF(YEAR, t1.value, t2.value) 
						WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEDIFF(QUARTER, t1.value, t2.value)
						WHEN @partition_boundry_unit = 'MONTH'		THEN DATEDIFF(MONTH, t1.value, t2.value) 
						WHEN @partition_boundry_unit = 'WEEK'			THEN DATEDIFF(WEEK, t1.value, t2.value) 
						WHEN @partition_boundry_unit = 'DAY'			THEN DATEDIFF(DAY, t1.value, t2.value) 
			END <> 1)
BEGIN;
	PRINT '-- There are gaps in the tail (information only)'
END;

-- Check that we have the expected number of parttions
DECLARE @partition_count int = (SELECT COUNT(*) FROM dbo.#partitions_older_than_current_time);
IF @partition_count <> @days_to_keep_data
BEGIN;
	PRINT '-- The number of partitions in the tail: ' + CAST(@partition_count AS sysname) + ' | The number of required partitions: ' + CAST(@days_to_keep_data AS sysname);	
END;
/****************************************************************************************************/
/*	End section older than @current_time	*/
/****************************************************************************************************/



/****************************************************************************************************/
/*	Begin section Switch	*/
/****************************************************************************************************/

-- Partitions to be Switched and Merged; 
-- Parttions older than our defined retention period
IF OBJECT_ID('tempdb.dbo.#partitions_for_merge', 'U') IS NOT NULL DROP TABLE dbo.#partitions_for_merge;
CREATE TABLE dbo.#partitions_for_merge (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, value datetime, file_group sysname, is_read_only bit, partition_number int, rows bigint, size_mb int /*, data_file sysname, physical_name varchar(2000) */);

INSERT dbo.#partitions_for_merge (value, file_group, is_read_only, partition_number /*, rows, size_mb */ )
SELECT 
	CASE WHEN @part_key_data_type = 0	THEN CAST(prv.value AS datetime) 
			 WHEN @part_key_data_type IN (1, 2) THEN dbo.IntToDatetime(CAST(prv.value AS int)) END 	
 --,LEAD(fg.name, 1, 0) OVER (ORDER BY dds.destination_id) 
 ,fg.name
 ,fg.is_read_only
 ,dds.destination_id 
 --,(SELECT SUM(p.rows) FROM sys.partitions p  WHERE p.index_id IN (0,1) AND  p.partition_number = dds.destination_id AND p.object_id = OBJECT_ID(@schema + '.' + @table, 'U'))
 --,(SELECT SUM(au.used_pages/128) FROM sys.partitions p INNER JOIN sys.allocation_units au ON au.container_id = p.partition_id AND p.index_id IN (0,1) AND  p.partition_number = dds.destination_id AND p.object_id =   OBJECT_ID(@schema + '.' + @table, 'U'))
FROM sys.partition_schemes AS ps
INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
INNER JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id <= pf.fanout
INNER JOIN sys.filegroups AS fg ON fg.data_space_id = dds.data_space_id
LEFT JOIN sys.partition_range_values prv ON pf.function_id = prv.function_id AND prv.boundary_id = dds.destination_id
WHERE pf.name = @partition_function /*AND ps.name = @partition_schema*/
AND CASE WHEN @part_key_data_type = 0			 THEN CAST(prv.value AS datetime) 
				 WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime(CAST(prv.value AS int) ) END < @retention_date -- <= instead of < so that we get the extra boubdry for the LEAD function
ORDER BY dds.destination_id;
--SELECT * FROM dbo.#partitions_for_merge




-- Keep information of the file groups to be removed before we perform the Switch (that modifies the partition function and therefore the partition state is changed)
IF OBJECT_ID('tempdb.dbo.#files_to_remove', 'U') IS NOT NULL DROP TABLE dbo.#files_to_remove;
CREATE TABLE dbo.#files_to_remove (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, data_file sysname, file_group sysname, physical_name varchar(2000), size_mb bigint, spaceused_mb bigint);

INSERT dbo.#files_to_remove (file_group, data_file, physical_name, size_mb, spaceused_mb )
SELECT 
   FILEGROUP_NAME(f.data_space_id)  
	,f.name   
	,f.physical_name  
	,f.size/128
	,CAST(FILEPROPERTY(f.name, 'SpaceUsed') AS int) / 128 spaceused_mb  
FROM sys.database_files f  
WHERE 
	--  File groups that the merge partition uses
	FILEGROUP_NAME(f.data_space_id) IN (SELECT file_group FROM dbo.#partitions_for_merge)
	
	-- File groups in other partitions (not in the merge partition), add by saby 06/09/2016	
	AND FILEGROUP_NAME(f.data_space_id) NOT IN 
							(
									SELECT 
										DISTINCT fg.name 
									FROM sys.partition_schemes ps
									INNER JOIN sys.partition_functions pf ON ps.function_id = pf.function_id
									INNER JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id <= pf.fanout
									INNER JOIN sys.filegroups fg ON fg.data_space_id = dds.data_space_id
									WHERE pf.name = @partition_function
									AND dds.destination_id > (SELECT MAX(partition_number) FROM dbo.#partitions_for_merge)
							)
	ORDER BY f.data_space_id;
--SELECT * FROM #files_to_remove;



-- Print read_only file groups that needs to be opened before the Merge
IF EXISTS(SELECT * FROM dbo.#partitions_for_merge WHERE is_read_only = 1)
BEGIN;
	IF OBJECT_ID('tempdb.dbo.#read_only_file_groups', 'U') IS NOT NULL DROP TABLE dbo.#read_only_file_groups;
	CREATE TABLE dbo.#read_only_file_groups (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, file_group sysname );

	INSERT dbo.#read_only_file_groups (file_group )
	SELECT 
		 file_group 
	FROM dbo.#partitions_for_merge WHERE is_read_only = 1;
	--SELECT * FROM #read_only_file_groups

	DECLARE @read_only_file_group sysname;
	SELECT @min_id = 1, @max_id = (SELECT MAX(id) FROM dbo.#read_only_file_groups);

	/* DECLARE @max_zize_mb int = (SELECT SUM(size_mb) FROM dbo.#read_only_file_groups); */
	PRINT CHAR(10) + '-- ' + CAST(@max_id AS sysname) + ' File groups that are read_only needs to be set read_write.' 

	-- Loop through the read_only file groups and print the names
	WHILE	@min_id <= @max_id
	BEGIN;		
		SELECT 
			 @read_only_file_group = file_group 
		FROM dbo.#read_only_file_groups WHERE id = @min_id;
		--SELECT * FROM #read_only_file_groups
		
		SELECT @command = 'ALTER DATABASE [' + @database + '] MODIFY FILEGROUP [' + @read_only_file_group + '] READ_WRITE;';
		PRINT @command;
		IF @debug = 0 
		BEGIN;
			SELECT @operation = 'READ_WRITE';
			INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
			SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, NULL,@read_only_file_group, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @command;
			EXEC (@command);
		END;

		--PRINT '-- File group: ' + @read_only_file_group + '  |  data_file: ' + @data_file + ' |  physical_name: ' + @physical_name ;
		SELECT @min_id += 1;
	END;
	SELECT @command = '';
END;



DECLARE @file_group sysname, @bcp_command varchar(4000), @truncate_command varchar(1000), @rows bigint, @print_bcp bit = 0;
IF EXISTS(SELECT * FROM dbo.#partitions_for_merge)
BEGIN;
	DECLARE @min_id_partitioned_tables int, @max_id_partitioned_tables int;
	SELECT  @min_id_partitioned_tables = 1, @max_id_partitioned_tables = (SELECT MAX(id) FROM dbo.#partitioned_tables);
	SELECT  @min_id = 1, @max_id = (SELECT MAX(id) FROM dbo.#partitions_for_merge);

	PRINT CHAR(10) + '-- Need to Switch out ' + CAST(@max_id AS sysname) + ' partitions' ;
	
		-- Outer Loop: Iterate through the partitions boundries
		WHILE @min_id <= @max_id
		BEGIN;

				-- Get the partitions boundry 
				IF @part_key_data_type = 0 -- datetime
				BEGIN;
					SELECT
						 @value = '''' + CONVERT(varchar(30), value, 121) + ''''
						,@file_group = file_group
						,@rows = rows
						,@size_mb = size_mb
						,@partition_number = CAST(partition_number AS varchar(5))
					FROM dbo.#partitions_for_merge WHERE id = @min_id;
				END;
				IF @part_key_data_type IN (1, 2)
				BEGIN;
					SELECT 
						 @value = dbo.DatetimeToInt(value, @part_key_len) 
						,@file_group = file_group
						,@rows = rows
						,@size_mb = size_mb
						,@partition_number = CAST(partition_number AS varchar(5))
					FROM dbo.#partitions_for_merge WHERE id = @min_id;
				END;
				--PRINT '-- ' + CAST(@value AS sysname) + ' | ' + @schema + ' | ' + @table + ' | ' + @file_group;
				--SELECT * FROM #partitions_for_merge

				-- Inner Loop: Iterate through the partitioned tables sharing the same partition function
				-- We have to Switch out all tables before the Merge in order to prevent data movment
				WHILE @min_id_partitioned_tables <= @max_id_partitioned_tables
				BEGIN;
								SELECT 
									 @schema			= [schema]
									,@table				= [table]
								FROM dbo.#partitioned_tables WHERE id = @min_id_partitioned_tables;
								--SELECT * FROM #partitioned_tables

								-- Remove the appostrophy for the INSERT to the log table
								SELECT @new_value = REPLACE(@value,'''',''); 
								SELECT @empty_table = @table + '_Empty';
														
								-- Use an existing Empty table for the Switch that resides on the same ps
								IF @create_empty_table = 0
								BEGIN;
										SELECT @command = 
									--'TRUNCATE TABLE [' + @schema + '].[' + @table + '_Empty' + '];' + CHAR(10) + 'ALTER TABLE [' + @schema + '].[' + @table + '] SWITCH PARTITION $PARTITION.[' + @partition_function + '](' + @value + ') TO [' + @schema + '].[' + @table + '_Empty' + '] PARTITION $PARTITION.[' + @partition_function + '](' + @value + ');'
									'TRUNCATE TABLE [' + @schema + '].[' + @empty_table + '];' + CHAR(10) + 
										'ALTER TABLE [' + @schema + '].[' + @table + '] SWITCH PARTITION ' + @partition_number + ' TO [' + @schema + '].[' + @empty_table + '] PARTITION ' + @partition_number + ';';
										PRINT @command;
										IF @debug = 0 
										BEGIN;
											SELECT @operation = 'SWITCH';
											INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
											SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @command;
											EXEC (@command);
										END;
								END; --IF @create_empty_table = 0
				
								-- Create an Empty table for the Switch in cases where there is no Empty tbale available on the partition schema
								IF @create_empty_table = 1
								BEGIN;
											SELECT @partition_number_int = CAST(@partition_number AS int);

										 -- Moved this code to: sp_generate_table_script 
											SELECT @command = 'IF OBJECT_ID(''[' + @schema + '].[' + @empty_table + ']'', ''U'') IS NOT NULL DROP TABLE [' + @schema + '].[' + @empty_table +'];';
											PRINT @command;							
											IF @debug = 0 
											BEGIN;
												SELECT @operation = 'DROP TABLE';
												INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
												SELECT CURRENT_TIMESTAMP, @operation, @database, @schema,  @empty_table, @partition_function, @partition_schema, @read_only_file_group, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, NULL, NULL, NULL, @command;
												EXEC (@command);
											END;

											-- Generate the create table command
											SELECT @command = '';
											--PRINT @schema = @schema, @table = @table, @empty_table = @empty_table, @file_group = @file_group,  @cmd = @command OUTPUT;
											EXEC dbo.sp_generate_table_script @DBName	= @database, @SchemaName = @schema, @TableName = @table, @NewTableName = @empty_table, @FileGroup = @file_group, @TableCommand = @command OUTPUT ,@PartitionNumber = @partition_number_int;
											--PRINT 'EXEC dbo.sp_generate_table_script @DBName	= ''' + @database + ''', @SchemaName = ''' + @schema + ''', @TableName = ''' + @table + ''', @NewTableName = ''' + @empty_table + ''', @FileGroup = ''' + @file_group + ''', @TableCommand =  @command OUTPUT ,@PartitionNumber = ' + CAST(@partition_number_int AS sysname )+ '';

											PRINT  @command + CHAR(10);
											IF @debug = 0 
											BEGIN;
												SELECT @operation = 'CREATE TABLE';
												INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
												SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @empty_table, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @command;
												EXEC (@command);
											END;
											SELECT @command = '';	

											-- Generate the create indexe(s) command
											EXEC dbo.sp_generate_indexes_script @DBName = @database, @SchemaName	= @schema, @TableName = @table, @NewTableName = @empty_table , @FileGroup = @file_group , @PartitionNumber = @partition_number_int, @IndexesCommand = @command OUTPUT;
											PRINT  @command + CHAR(10);
											IF @debug = 0 
											BEGIN;
												SELECT @operation = 'CREATE INDEXES';
												INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
												SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @empty_table, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @command;
												EXEC (@command);
											END;

											-- Use a different syntax for the Switch since we switch to a none partitioned table
											SELECT @command = 
											--	'ALTER TABLE [' + @schema + '].[' + @table + '] SWITCH PARTITION $PARTITION.[' + @partition_function + '](' + @value + ') TO [' + @schema + '].[' + @table + '_Empty];';
											'ALTER TABLE [' + @schema + '].[' + @table + '] SWITCH PARTITION ' + @partition_number + ' TO [' + @schema + '].[' + @empty_table + '];';
											PRINT @command;
											IF @debug = 0 
											BEGIN;
												SELECT @operation = 'SWITCH';
												INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
												SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, @partition_schema, @file_group, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @command;
												EXEC (@command);
											END;
								END; -- End: IF @create_empty_table = 1

				
								-- Export the data we stwitched out to the file system using bcp out
								IF @is_bcp = 1
								BEGIN;
									-- A one time print of the @rc variable
									-- IF @print_bcp = 0 BEGIN; SELECT @print_bcp = 1; PRINT 'DECLARE @rc int = 0;'; END;

									/*
									-- If the partitioning key is datetime then we convert to int for the bcp file name 
									IF @part_key_data_type = 0 SELECT @bcp_value = CAST(dbo.DatetimeToInt(CAST(REPLACE(@value,'''','') AS datetime), @part_key_len) AS sysname)
									ELSE 
									IF @part_key_data_type IN (1,2) SELECT @bcp_value = @value;
									*/

									-- Remove any char that is not an alpha numeric char
									SELECT @bcp_value = dbo.fn_RemoveChars(@value, '0-9a-z '); 
									-- Replcae the space 
									SELECT @bcp_value = REPLACE(@bcp_value, ' ', '_');
									DECLARE @bcp_file nvarchar(4000) =  @bcp_path + @table + '_' + @bcp_value + '.bcp';
											
									IF (SELECT DBA.dbo.SQLIO_fnFileExists(@bcp_file) ) = 1
									BEGIN
										PRINT '-- File exists: ' + @bcp_file + char(10);
										IF @debug = 0 
										BEGIN;
											SELECT @operation = 'BCP File exists';
											INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
											SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, NULL, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @bcp_file;
											--EXEC (@bcp_command); Yaniv Etrogi 20230214. This is not to overide the existing file.
										END;
										BREAK; -- Skip this iteration.
									END;


									-- If the partition has no rows skipt it so we do not create files with 0 bytes (potentially overriding an existing file).
									-- This was added as a preventive step to cases where an existing file was overiden.
									DECLARE @partition_rows_count_command nvarchar(max), @params nvarchar(500), @partition_rows_count int; 
									SELECT @params = N'@partition_rows_count int OUTPUT, @partition_number int, @empty_table sysname';
									SELECT @partition_rows_count_command = 
									N'SELECT @partition_rows_count = (SELECT rows FROM sys.partitions p WHERE p.object_id = OBJECT_ID(''' + @empty_table + ''') AND index_id IN (0,1) AND p.partition_number = ' + CAST(@partition_number AS sysname) + ');';
									
									--PRINT @partition_rows_count_command;
									EXEC sp_executesql @partition_rows_count_command, @params
										, @partition_rows_count = @partition_rows_count OUTPUT, @empty_table = @empty_table, @partition_number = @partition_number;
									--PRINT '@partition_rows_count: ' + cast(@partition_rows_count as sysname) + ' | @partition_number: ' + cast(@partition_number as sysname);
									
									IF @partition_rows_count = 0
									BEGIN;
										print '-- Empty partition. Skipping partiton number ' + CAST(@partition_number AS sysname) + ' since it is empty.';
										IF @debug = 0 
										BEGIN;
											SELECT @operation = 'BCP Empty Partition';
											INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
											SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, NULL, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @bcp_command;
										END;
										BREAK; -- Skip this iteration.
									END;
									 
									-- 20230215 Yaniv Etrogi: Add transaction block for the bcp out.
									-- If there is an error in the bcp out we Switch the data back in to the source base table
									SELECT @bcp_command = 
									'BEGIN TRAN; DECLARE @rc int = 0; EXEC @rc = sys.xp_cmdshell ''bcp ' + @database + '.dbo.' + @empty_table + ' OUT ' + @bcp_path + @table + '_' + @bcp_value + '.bcp -k -n -T -E -S' + @server + ''' COMMIT TRAN;
		IF (@@ERROR <> 0 OR @rc <> 0) BEGIN; PRINT ERROR_MESSAGE();
		ALTER TABLE [' + @schema + '].[' + @empty_table + '] SWITCH PARTITION ' + @partition_number + ' TO [' + @schema + '].[' + @table + '] PARTITION ' + @partition_number + ';
		PRINT ''-- bcp failed on partition number: ' + @partition_number + ' partiton value: ' + @bcp_value + ' Terminating.'' IF @@TRANCOUNT > 0 ROLLBACK TRAN; RAISERROR (''Error raised in the bcp block at partition number: %s partiton value: %s '', 16, 1, ''' + @partition_number + ''', ''' + @bcp_value + '''); END;';
									PRINT @bcp_command + CHAR(10) + 'GO';
											
									IF @debug = 0 
									BEGIN;
										SELECT @operation = 'BCP';
										INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
										SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, NULL, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @bcp_command;
										EXEC (@bcp_command);
									END;
								END;

								-- Truncate command out side the inner loop as it serves all cases
								SELECT @truncate_command = 'TRUNCATE TABLE [' + @schema + '].[' + @empty_table + '];' + CHAR(10);
								PRINT  @truncate_command;
								IF @debug = 0 
								BEGIN;
									SELECT @operation = 'TRUNCATE';
									INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
									SELECT CURRENT_TIMESTAMP, @operation, @database, @schema, @table, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL,CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @truncate_command;
									EXEC (@truncate_command);
								END;

								-- Drop the dynamically created Empty table so that the file group can be removed
								IF @create_empty_table = 1
								BEGIN;
									SELECT @command = 'IF OBJECT_ID(''[' + @schema + '].[' + @table + '_Empty]'', ''U'') IS NOT NULL DROP TABLE [' + @schema + '].[' + @table + '_Empty];';
									PRINT  @command;
									IF @debug = 0 
									BEGIN;
										EXEC (@command);
									END;
								END;
						SELECT @command = NULL, @bcp_command = NULL, @truncate_command = NULL
						SELECT @min_id_partitioned_tables += 1;
				END;
			SELECT @min_id_partitioned_tables = 1; --Asign a value of 1 for cases where there is more than a single table so that the next iteration over a new tables starts at 1
			SELECT @min_id += 1;
		END;
END;
--SELECT * FROM dbo.#partitions_for_merge
/****************************************************************************************************/
/*	End section Switch */
/****************************************************************************************************/



/****************************************************************************************************/
/*	Begin section Merge	*/
/****************************************************************************************************/
IF @is_merge = 1
BEGIN;
	IF EXISTS(SELECT * FROM dbo.#partitions_for_merge)
	BEGIN;
		SELECT @min_id = 1, @max_id = (SELECT MAX(id) -1 FROM dbo.#partitions_for_merge); -- ignore the last partition (MAX(id) -1) in order to prevent data movement
		PRINT CHAR(10) + '-- Need to Merge ' + CAST(@max_id AS sysname ) + ' partitions';

		WHILE @min_id <= @max_id
		BEGIN;

			IF @part_key_data_type = 0
			BEGIN;
				SELECT 
						@value = '''' +	CONVERT(varchar(30), value, 121) + ''''
				FROM dbo.#partitions_for_merge WHERE id = @min_id; 
			END;

			IF @part_key_data_type IN (1, 2)
			BEGIN;
				SELECT 
						@value = dbo.DatetimeToInt(value, @part_key_len)
				FROM dbo.#partitions_for_merge WHERE id = @min_id; 
			END;

			SELECT @command = 
				'ALTER PARTITION FUNCTION [' + @partition_function + ']() MERGE RANGE(' + @value + ');' 

			IF @debug = 1 PRINT @command;

			IF @debug = 0 
			BEGIN;
				SELECT @operation = 'MERGE';
				-- Remove the appostrophy for the INSERT
				SELECT @new_value = REPLACE(@value,'''',''); 

				-- Loop while executing the MERGE command.
				-- If the command is blocked waiting to aquire a lock on a resource it will fail to the CATCH block when crossing the LOCK TIME_OUT settings
				SELECT @counter = 1;
				BEGIN TRY;	
					WHILE @counter <= 5
					BEGIN;
							INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
							SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, NULL, NULL, NULL, @command;

							PRINT @command;
							EXEC (@command);
						BREAK;
					END;
				END TRY	
				BEGIN CATCH;
					 IF @@TRANCOUNT > 0 ROLLBACK TRAN;
				  
						SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber = ERROR_NUMBER(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE(), @ErrorLine = ERROR_LINE(), @ErrorProc = ISNULL(ERROR_PROCEDURE(), CONVERT(sysname, OBJECT_NAME(@@PROCID)));					
						SELECT @CurrentTime	 = CURRENT_TIMESTAMP;
  
						SELECT @Msg = (SELECT  @CurrentTime		AS 'EventTime'
																	,@ErrorProc			AS 'ObjectName'
																	,@ErrorNumber		AS 'Error/Number'
																	,@ErrorMessage	AS 'Error/Message'
																	,@ErrorSeverity AS 'Error/Severity'
																	,@ErrorState		AS 'Error/State'
																	,@ErrorLine			AS 'Error/Line'
														FOR XML PATH('Event'));  
					--PRINT CAST(@Msg AS varchar(8000));
  
					INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [schema], [table], [database], file_group, data_file,physical_name,partition_function ,partition_schema,partition_number, partition_boundry, [rows], size_mb, error, command)
					SELECT CURRENT_TIMESTAMP, @operation, @schema, @table, @database, @file_group, @data_file, @physical_name,@partition_function, @partition_schema, @partition_number_int, NULL, NULL, NULL, CAST(@Msg AS varchar(max)), NULL;
					
					PRINT 'Retrying ' + @operation + ' command attempt number: ' + CAST(@counter AS sysname);
					SELECT @counter += 1;
				END CATCH;
			END;
		
			SELECT @min_id +=1;
		END;
END;
END; --@is_merge
/****************************************************************************************************/
/*	End section Merge	*/
/****************************************************************************************************/



/****************************************************************************************************/
/*	Bgin section Drop files and files groups*/
/****************************************************************************************************/
--SELECT * FROM dbo.#files_to_remove
IF OBJECT_ID('tempdb.dbo.#file_groups_to_remove ', 'U') IS NOT NULL DROP TABLE dbo.#file_groups_to_remove;
CREATE TABLE dbo.#file_groups_to_remove (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED /* remark by saby 06/09/2016 , value datetime */, file_group sysname);

IF EXISTS(SELECT * FROM dbo.#files_to_remove) /* remark by saby 09/06/2016 @create_file_group = 1*/
BEGIN;

	IF EXISTS(SELECT * FROM dbo.#files_to_remove)
	BEGIN;
	
		-- Remove data files
		DECLARE @files_size_mb bigint;
		SELECT 
			  @files_size_mb			= SUM(size_mb)
			 ,@files_spaceused_mb = SUM(spaceused_mb)
			--,@rows					= SUM(CAST(rows AS bigint))
		FROM dbo.#files_to_remove;
		

		SELECT @min_id = 1, @max_id = (SELECT MAX(id) -1 FROM dbo.#files_to_remove); -- ignore the last partition (MAX(id) -1) in order to prevent data movement
		PRINT CHAR(10) + '-- Need to drop ' +  CAST(@max_id AS sysname) + ' data files with a total size of: ' + CAST(@files_size_mb AS sysname) + ' mb (space used ' + CAST(@files_spaceused_mb AS sysname) + + ' mb )';-- + CAST(@rows AS sysname) + ' rows) ' ;

		WHILE @min_id <= @max_id
		BEGIN;

			SELECT 
				 @data_file = data_file
				,@physical_name = physical_name
				--,@partition_number = partition_number
				--,@rows = rows
				,@size_mb = size_mb
			FROM dbo.#files_to_remove		
			WHERE id = @min_id;

			SELECT @command = 
				'ALTER DATABASE [' + @database + '] REMOVE FILE [' + @data_file + '];'
			PRINT @command;

			IF @debug = 0 
			BEGIN;
				SELECT @new_value = REPLACE(@value,'''',''); 
				SELECT @operation = 'REMOVE FILE';
				INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file, physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
				SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, @data_file, @physical_name, @partition_number, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END, @rows, @size_mb, NULL, @command;
				EXEC (@command);
			END;
		
			SELECT @min_id += 1;
		END;
		--SELECT * FROM dbo.#files_to_remove

		-- Remove file_groups
		-- Get the files DISTINCT file groups as a file_group can appear multiple times if it has more than one data file
		INSERT dbo.#file_groups_to_remove ( file_group)
		SELECT DISTINCT file_group FROM dbo.#files_to_remove 
		WHERE id NOT IN (SELECT MAX(id) FROM dbo.#files_to_remove)	-- we do not work on the last file group as it is still in use by the earliest populated partition	
		ORDER BY file_group;

		SELECT @min_id = 1, @max_id = (SELECT MAX(id) -1 FROM #file_groups_to_remove); -- ignore the last partition (MAX(id) -1) in order to prevent data movement
		PRINT CHAR(10) + '-- Need to drop ' +  CAST(@max_id AS sysname) + ' file groups';

		WHILE @min_id <= @max_id
		BEGIN;

			SELECT
				@file_group = file_group
			FROM dbo.#file_groups_to_remove		
			WHERE id = @min_id;

			SELECT @command = 
				'ALTER DATABASE [' + @database + '] REMOVE FILEGROUP [' + @file_group + '];';
			PRINT @command;

			IF @debug = 0 
			BEGIN;
				SELECT @operation = 'REMOVE FILEGROUP';
				INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file, physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
				SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, NULL, NULL, NULL, NULL, NULL, NULL, NULL, @command;
				EXEC (@command);
			END;
		
			SELECT @min_id += 1;
		END;
	END;
END; -- @create_file_group
--SELECT * FROM dbo.#file_groups_to_remove
/****************************************************************************************************/
/*	End section Drop files and files groups	*/
/****************************************************************************************************/



/****************************************************************************************************/
/*	Begin section newer than @current_time	*/
/****************************************************************************************************/
-- Get all the values of partitions newer than current time 
IF OBJECT_ID('tempdb.dbo.#partitions_newer_than_current_time ', 'U') IS NOT NULL DROP TABLE dbo.#partitions_newer_than_current_time;
CREATE TABLE dbo.#partitions_newer_than_current_time (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, value datetime);

INSERT dbo.#partitions_newer_than_current_time(value)	
SELECT 
	CASE WHEN @part_key_data_type = 0			 THEN CAST(prv.value AS datetime) 
			 WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime(CAST(prv.value AS int)  ) END 
FROM sys.partition_functions AS pf
INNER JOIN sys.partition_range_values AS prv ON prv.function_id = pf.function_id
WHERE pf.name = @partition_function 
AND CASE WHEN @part_key_data_type = 0			 THEN CAST(prv.value AS datetime) 
				 WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime(CAST(prv.value AS int)  )  END > @current_time;
--SELECT * FROM #partitions_newer_than_current_time

-- Verify that there are no gaps. The diff between each value should be 1 hour
IF OBJECT_ID('tempdb.dbo.#head_gaps', 'U') IS NOT NULL DROP TABLE dbo.#head_gaps;
CREATE TABLE dbo.#head_gaps (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, value datetime);
INSERT #head_gaps (value)
SELECT t1.value
FROM dbo.#partitions_newer_than_current_time AS t1 
INNER JOIN dbo.#partitions_newer_than_current_time AS t2 ON t1.id + 1 = t2.id 
																						--AND DATEDIFF(DAY, t1.value, t2.value)
AND CASE  WHEN @partition_boundry_unit = 'YEAR'			THEN DATEDIFF(YEAR, t1.value, t2.value) 
		WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEDIFF(QUARTER, t1.value, t2.value)
		WHEN @partition_boundry_unit = 'MONTH'		THEN DATEDIFF(MONTH, t1.value, t2.value) 
		WHEN @partition_boundry_unit = 'WEEK'			THEN DATEDIFF(WEEK, t1.value, t2.value) 
		WHEN @partition_boundry_unit = 'DAY'			THEN DATEDIFF(DAY, t1.value, t2.value) 
		END <> 1
--SELECT * FROM #head_gaps

IF EXISTS (SELECT * FROM #head_gaps)
BEGIN;
	PRINT '-- There are gaps in the head (information only)';
END;


-- Check that we have the expected number of parttions
SELECT @partition_count = (SELECT COUNT(*) FROM dbo.#partitions_newer_than_current_time);

PRINT '-- The number of partitions in the head: ' + CAST(@partition_count AS sysname) + ' | The number of required partitions: ' + CAST(@num_future_partitions AS sysname);

DECLARE @required_boundry datetime, @current_max_boundry datetime; 

-- Get the date boundry we need as defined by the @num_future_partitions parameter
SELECT @required_boundry = CASE WHEN @partition_boundry_unit = 'YEAR'			THEN DATEADD(year, DATEDIFF(year, 0, CURRENT_TIMESTAMP ) + @num_future_partitions, 0)			
																WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEADD(QUARTER, DATEDIFF(QUARTER, 0, CURRENT_TIMESTAMP ) + @num_future_partitions, 0)
																WHEN @partition_boundry_unit = 'MONTH'		THEN DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP ) + @num_future_partitions, 0)		
																WHEN @partition_boundry_unit = 'WEEK'			THEN DATEADD(week, DATEDIFF(week, -1, CURRENT_TIMESTAMP )  + @num_future_partitions, -1)		
																WHEN @partition_boundry_unit = 'DAY'			THEN DATEADD(day, DATEDIFF(day, 0, CURRENT_TIMESTAMP ) + @num_future_partitions, 0)		
																WHEN @partition_boundry_unit = 'HOUR'			THEN DATEADD(hour, DATEDIFF(hour, 0, CURRENT_TIMESTAMP ) + @num_future_partitions, 0)				
														END;


-- Get the current most recent boundry
IF EXISTS (SELECT * FROM #partitions_newer_than_current_time)
BEGIN;
	-- Yaniv Etrogi 20180904: Add the predicate to ignore the max id when the dattime vaue is far in the future
	-- Disregard the last id in the table as it resulted in an invalid future date (2100-01-01 00:00:00.000)
	SELECT @current_max_boundry = CASE 
		WHEN (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time ) > DATEADD(DAY, 1000, CURRENT_TIMESTAMP) THEN (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time WHERE id = (SELECT MAX(id)-1 FROM dbo.#partitions_newer_than_current_time ))
		ELSE (SELECT MAX(value) FROM dbo.#partitions_newer_than_current_time ) END;

	-- Yaniv Etrogi 20180904
	-- If there are no rows in table #partitions_newer_than_current_time resulting in a NULL then we set the next day is the boundry to start for the SPLIT operation
	-- We add an extra day to prevent data movment in case the last partition is already full. 
	IF @current_max_boundry IS NULL SELECT @current_max_boundry = CURRENT_TIMESTAMP + 1

END;
	ELSE
BEGIN;
	-- Asingn a value to the @current_max_boundry based on the @partition_boundry_unit
	-- When there are no rows in table it means that there are no boundries for future dates and we therefore need to define the next boudary value.
	-- This value will be the @current_max_boundry and will act as the starting point for the boundaries to Split
	SELECT @current_max_boundry = 
	CASE  WHEN @partition_boundry_unit		 = 'YEAR'		THEN DATEADD(year, DATEDIFF(year, 0, CURRENT_TIMESTAMP )+1, 0)				-- first day of next year
				WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEADD(quarter, DATEDIFF(quarter, 0, CURRENT_TIMESTAMP )+1, 0)	-- first day of next quarter
				WHEN @partition_boundry_unit = 'MONTH'		THEN DATEADD(month, DATEDIFF(month, 0, CURRENT_TIMESTAMP )+1, 0)			-- first day of next month
				WHEN @partition_boundry_unit = 'WEEK'		THEN DATEADD(week, DATEDIFF(week, -1, CURRENT_TIMESTAMP ) +1, -1)			-- first day of next week, substruct 1 to have the first day of week on Sun. instead of Mon.
				WHEN @partition_boundry_unit = 'DAY'		THEN DATEADD(day, DATEDIFF(day, 0, CURRENT_TIMESTAMP )+1, 0)					-- first day of next day
				WHEN @partition_boundry_unit = 'HOUR'		THEN DATEADD(hour, DATEDIFF(hour, 0, CURRENT_TIMESTAMP )+1, 0)				-- first hour of the day
		END;
END;

PRINT '-- Required_boundry: ' + CONVERT(varchar(30), @required_boundry, 121) + ' | Current_max_boundry: ' + CONVERT(varchar(30), @current_max_boundry, 121);

-- Prepare the boundries for the Split
IF OBJECT_ID('tempdb.dbo.#partitions_for_split', 'U') IS NOT NULL DROP TABLE dbo.#partitions_for_split;
CREATE TABLE dbo.#partitions_for_split (id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, value datetime NOT NULL);
	
IF @partition_count <> @num_future_partitions
BEGIN;
	--PRINT '-- The number of partitions in the head: ' + CAST(@partition_count AS sysname) + ' is different than the defined number: ' + CAST(@num_future_partitions AS sysname);
		
	-- Populate the #partitions_for_split table with the boundries required for the split
	WHILE @current_max_boundry < @required_boundry
	BEGIN;
		INSERT dbo.#partitions_for_split (value) 
		SELECT																		-- DATEADD(DAY, 1, @current_max_boundry);
		CASE  WHEN @partition_boundry_unit = 'YEAR'			THEN DATEADD(YEAR, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEADD(QUARTER, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'MONTH'		THEN DATEADD(MONTH, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'WEEK'			THEN DATEADD(WEEK, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'DAY'			THEN DATEADD(DAY, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'HOUR'			THEN DATEADD(HOUR, 1, @current_max_boundry)
		END;

		-- increment by 1
		SELECT @current_max_boundry =							-- DATEADD(DAY, 1, @current_max_boundry);
		CASE  WHEN @partition_boundry_unit = 'YEAR'			THEN DATEADD(YEAR, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'QUARTER'	THEN DATEADD(QUARTER, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'MONTH'		THEN DATEADD(MONTH, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'WEEK'			THEN DATEADD(WEEK, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'DAY'			THEN DATEADD(DAY, 1, @current_max_boundry)
					WHEN @partition_boundry_unit = 'HOUR'			THEN DATEADD(HOUR, 1, @current_max_boundry)
		END;
	END;
END;
--SELECT * FROM #partitions_for_split
/****************************************************************************************************/
/*	End section newer than @current_time	*/
/****************************************************************************************************/



/****************************************************************************************************/
/*	Begin section Split	*/
/****************************************************************************************************/
IF @is_split = 1
BEGIN;
	PRINT CHAR(10) + '-- Need to Split ' + CAST(@num_future_partitions - @partition_count AS sysname) + ' partitions';

	IF EXISTS(SELECT * FROM dbo.#partitions_for_split)
	BEGIN;
		DECLARE @file_group_command varchar(512), @data_file_command varchar(1024), @start_date datetime, @end_date datetime, @file_group_type int, @data_file_size_mb int ;
		SELECT @file_group_type = CASE WHEN @partition_boundry_unit = 'YEAR' THEN 1 WHEN @partition_boundry_unit = 'QUARTER' THEN 2 WHEN @partition_boundry_unit = 'MONTH' THEN 3 WHEN @partition_boundry_unit = 'WEEK' THEN 4 WHEN @partition_boundry_unit = 'DAY' THEN 5 END;
		
		-- Get the size of the last partition (excluding the current partition) to be used when vcreating new data files
		EXEC @data_file_size_mb = dbo.sp_get_last_partition_size @table = @table;


		SELECT @min_id = 1, @max_id = (SELECT MAX(id) FROM dbo.#partitions_for_split );
		WHILE  @min_id <= @max_id
		BEGIN;
			
				SELECT  @start_date = CONVERT(varchar(30), value, 121)
								,@end_date	=	CASE  WHEN @partition_boundry_unit = 'YEAR'			THEN CONVERT(varchar(30), DATEADD(YEAR, 1, value), 121)
																		WHEN @partition_boundry_unit = 'QUARTER'	THEN CONVERT(varchar(30), DATEADD(QUARTER, 1, value), 121)
																		WHEN @partition_boundry_unit = 'MONTH'		THEN CONVERT(varchar(30), DATEADD(MONTH, 1, value), 121)
																		WHEN @partition_boundry_unit = 'WEEK'			THEN CONVERT(varchar(30), DATEADD(WEEK, 1, value), 121)
																		WHEN @partition_boundry_unit = 'DAY'			THEN CONVERT(varchar(30), DATEADD(DAY, 1, value), 121) 
																		WHEN @partition_boundry_unit = 'HOUR'			THEN CONVERT(varchar(30), DATEADD(HOUR, 1, value), 121) 
															END

								,@value		=		CASE	WHEN @part_key_data_type = 0 THEN CONVERT(varchar(30), value, 121) 
																		WHEN @part_key_data_type = 1 THEN  CAST(dbo.DatetimeToInt(CONVERT(varchar(30), value, 121), @part_key_len) AS sysname) 
																	--WHEN @part_key_data_type = 2 THEN  CAST(dbo.DatetimeToDateIdBigint(CONVERT(varchar(30), value, 121)) AS sysname) 
															END

				FROM dbo.#partitions_for_split WHERE id = @min_id;

				--PRINT '-- @start_date: ' + CONVERT(varchar(30),@start_date, 121) + ' | ' + '@end_date: ' + CONVERT(varchar(30),@end_date, 121);

				IF @create_file_group = 1
				BEGIN;
						DECLARE @fg_type int = CASE WHEN @partition_boundry_unit = 'YEAR'			THEN 1
																				WHEN @partition_boundry_unit = 'QUARTER'	THEN 2
																				WHEN @partition_boundry_unit = 'MONTH'		THEN 3
																				WHEN @partition_boundry_unit = 'WEEK'			THEN 4
																				WHEN @partition_boundry_unit = 'DAY'			THEN 5
																				WHEN @partition_boundry_unit = 'HOUR'			THEN 5 
																				ELSE 5 
																		END;

						-- Generate the file group and data file commands
						EXEC @rc = sp_generate_file_group_command 
														 @partition_function			= @partition_function
														,@partition_schema				= @partition_schema
														,@fg_name_wildcard				= @file_group_name_wildcard
														,@fg_physical_path				= @file_group_physical_path
														,@fg_file_size_mb					= @data_file_size_mb
														,@fg_file_growth_mb				= 128
														,@fg_filecount						= 1
														,@fg_type									= @fg_type -- 1=Yearly, 2=Quarterly, 3=Monthly, 4=Weekly, 5=Daily
														,@new_file_group					= @file_group OUTPUT
														,@new_file_group_command	= @file_group_command OUTPUT
														,@new_data_file_command		= @data_file_command OUTPUT
														,@start_date							= @start_date
														,@end_date								= @end_date
														,@debug										= @debug;
		
						PRINT @file_group_command;
						PRINT @data_file_command;

						IF @debug = 0 
						BEGIN;
							SELECT @operation = 'ADD FILEGROUP';
							INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file, physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
							SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, NULL, @file_group_physical_path, NULL, NULL, NULL, NULL, NULL, @file_group_command;
				
							SELECT @operation = 'ADD FILE';
							INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file, physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
							SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, NULL, @file_group_physical_path, NULL, NULL, NULL, NULL, NULL, @data_file_command;
				
							-- Add file group with a data file
							EXEC @rc = dbo.sp_create_file_group 
														 @file_group_command	= @file_group_command
														,@data_file_command		= @data_file_command;					
						END;
					END; -- IF @create_file_group = 1
						ELSE IF @create_file_group = 0
					BEGIN;
							-- Get the file group name to be used for the Split
							-- Use the last file group (head partition file group) for this function 
							SELECT @file_group = 
							(SELECT name 
													FROM 
													(
														SELECT DISTINCT TOP 1 fg.name, dds.data_space_id
														FROM sys.partition_schemes AS ps
														INNER JOIN sys.partition_functions AS pf ON ps.function_id = pf.function_id
														INNER JOIN sys.destination_data_spaces AS dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id <= pf.fanout
														INNER JOIN sys.filegroups AS fg ON fg.data_space_id = dds.data_space_id
														WHERE pf.name = @partition_function
														ORDER BY dds.data_space_id DESC 
													) AS derived
								);
							 
								IF @file_group IS NULL
								BEGIN;
										IF @debug = 0
										BEGIN;
											SELECT @operation = 'Get FileGroup';
											INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [schema], [table], [database], partition_function, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
											SELECT CURRENT_TIMESTAMP, @operation, NULL, NULL, @database, @partition_function, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;
										END;

										-- Exit here as we cannot SPLIT
										RAISERROR('Failed to obtain the file group to be used for the SPLIT command', 16, 1);
								END;
					END; --@create_file_group
			
			
			
					-- Split
					-- Get the partitions boundry 
					IF @part_key_data_type = 0 -- datetime
					BEGIN;
						SELECT
							 @value = '''' + CONVERT(varchar(30), value, 121) + ''''
						FROM dbo.#partitions_for_split WHERE id = @min_id AND value IS NOT NULL;
					END;
					IF @part_key_data_type IN (1, 2)
					BEGIN;
						SELECT 
							 @value = dbo.DatetimeToInt(value, @part_key_len) 	
						FROM dbo.#partitions_for_split WHERE id = @min_id AND value IS NOT NULL;				
					END;
				--SELECT * FROM #partitions_for_split

			SELECT @command = 
			'ALTER PARTITION SCHEME [' + @partition_schema + '] NEXT USED [' + @file_group + '];
	ALTER PARTITION FUNCTION [' + @partition_function + ']() SPLIT RANGE(' + @value + ');' + CHAR(10);
	
		IF @debug = 1 PRINT @command;

		IF @debug = 0 
		BEGIN;
				SELECT @operation = 'SPLIT';
				-- Remove the appostrophy for the INSERT
				SELECT @new_value = REPLACE(@value,'''',''); 

				-- Loop while executing the SPLIT command.
				-- If the command is blocked waiting to aquire a lock on a resource it will fail to the CATCH block when crossing the LOCK TIME_OUT settings
				SELECT @counter = 1;
				BEGIN TRY;	
					WHILE @counter <= 3
					BEGIN;
							INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [database], [schema], [table], partition_function, partition_schema, file_group, data_file, physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
							SELECT CURRENT_TIMESTAMP, @operation, @database, NULL, NULL, @partition_function, @partition_schema, @file_group, NULL, NULL, @partition_number, CASE WHEN @part_key_data_type = 0 THEN @new_value WHEN @part_key_data_type IN (1,2) THEN dbo.IntToDatetime (@new_value) END , NULL, NULL, NULL, @command;

							PRINT @command;
							EXEC (@command);
						BREAK;
					END;
				END TRY	
				BEGIN CATCH;
						IF @@TRANCOUNT > 0 ROLLBACK TRAN;
				  
						SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber = ERROR_NUMBER(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE(), @ErrorLine = ERROR_LINE(), @ErrorProc = ISNULL(ERROR_PROCEDURE(), CONVERT(sysname, OBJECT_NAME(@@PROCID)));					
						SELECT @CurrentTime	 = CURRENT_TIMESTAMP;
  
						SELECT @Msg = (SELECT  @CurrentTime		AS 'EventTime'
																	,@ErrorProc			AS 'ObjectName'
																	,@ErrorNumber		AS 'Error/Number'
																	,@ErrorMessage	AS 'Error/Message'
																	,@ErrorSeverity AS 'Error/Severity'
																	,@ErrorState		AS 'Error/State'
																	,@ErrorLine			AS 'Error/Line'
														FOR XML PATH('Event'));  
					--PRINT CAST(@Msg AS varchar(8000));
  
					INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [schema], [table], [database], file_group, data_file,physical_name,partition_function ,partition_schema,partition_number, partition_boundry, [rows], size_mb, error, command)
					SELECT CURRENT_TIMESTAMP, @operation, @schema, @table, @database, @file_group, @data_file, @physical_name,@partition_function, @partition_schema, @partition_number_int, NULL, NULL, NULL, CAST(@Msg AS varchar(max)), NULL;
					
					PRINT 'Retrying ' + @operation + ' command attempt number: ' + CAST(@counter AS sysname);
					SELECT @counter += 1;
				END CATCH;
		END; -- IF @debug = 0 

		SELECT @min_id += 1;	
		END; -- WHILE  @min_id <= @max_id
	END; --IF EXISTS(SELECT * FROM dbo.#partitions_for_split)
END; --@is_split
--SELECT * FROM #partitions_for_split
/****************************************************************************************************/
/*	End section Split	*/
/****************************************************************************************************/

/*
SELECT * FROM #partitioned_tables
SELECT * FROM #partitions_older_than_current_time
SELECT * FROM #partitions_for_merge
SELECT * FROM #files_to_remove
SELECT * FROM #file_groups_to_remove
SELECT * FROM #partitions_newer_than_current_time
SELECT * FROM #head_gaps
SELECT * FROM #partitions_for_split
*/

-- Log end of process
IF @debug = 0
BEGIN;
	SELECT @operation = 'End';
	INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [schema], [table], [database], partition_function, file_group, data_file,physical_name, partition_number, partition_boundry, [rows], size_mb, error, command)
	SELECT CURRENT_TIMESTAMP, @operation, NULL, NULL, @database, @partition_function, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL;
END;


END TRY
BEGIN CATCH;
					IF @@TRANCOUNT > 0 ROLLBACK TRAN;

						--SELECT @ErrorState = CASE WHEN @ErrorState BETWEEN 1 AND 127 THEN @ErrorState ELSE 1 END;
						SELECT @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber = ERROR_NUMBER(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE(), @ErrorLine = ERROR_LINE(), @ErrorProc = ISNULL(ERROR_PROCEDURE(), CONVERT(sysname, OBJECT_NAME(@@PROCID)));
						SELECT @CurrentTime	 = CURRENT_TIMESTAMP;
  
					SELECT @Msg = (SELECT  @CurrentTime		AS 'EventTime'
																,@ErrorProc			AS 'ObjectName'
																,@ErrorNumber		AS 'Error/Number'
																,@ErrorMessage	AS 'Error/Message'
																,@ErrorSeverity AS 'Error/Severity'
																,@ErrorState		AS 'Error/State'
																,@ErrorLine			AS 'Error/Line'
													FOR XML PATH('Event'));  
				PRINT CAST(@Msg AS varchar(8000));
  
				INSERT DBA.dbo.PartitionsMaintenanceLog (insert_time, operation, [schema], [table], [database], file_group, data_file,physical_name,partition_function ,partition_schema,partition_number, partition_boundry, [rows], size_mb, error, command)
				SELECT CURRENT_TIMESTAMP, @operation, @schema, @table, @database, @file_group, @data_file, @physical_name,@partition_function, @partition_schema, @partition_number_int, NULL, NULL, NULL, CAST(@Msg AS varchar(max)), NULL;

        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState); 
END CATCH;
