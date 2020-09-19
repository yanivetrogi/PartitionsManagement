USE DBA;
SET NOCOUNT ON;
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

--SELECT * FROM dbo.PartitionsManagment 
--SELECT * FROM PartitionsMaintenanceLog 


DECLARE @debug bit = 1 --<---- Edite here

IF OBJECT_ID('tempdb.dbo.#errors', 'U') IS NOT NULL DROP TABLE dbo.#errors;
CREATE TABLE dbo.#errors(id int IDENTITY(1,1) PRIMARY KEY CLUSTERED, [partition_function] sysname NULL, [error_message] varchar(MAX) NULL );


IF OBJECT_ID('tempdb.dbo.#data', 'U') IS NOT NULL DROP TABLE #data;
CREATE TABLE dbo.#data
(
 id int IDENTITY(1, 1) PRIMARY KEY NOT NULL
,[insert_time] [datetime] NOT NULL
,[database] [varchar](64) NOT NULL
,partition_function sysname NOT NULL
,days_to_keep_data int NOT NULL
,num_future_partitions int NOT NULL
,is_bcp bit NOT NULL
,bcp_path varchar(1024) 
,partition_boundry_unit sysname NOT NULL
,create_empty_table bit NOT NULL
,create_file_group bit NOT NULL
,file_group_physical_path varchar(1024)
,is_merge bit NOT NULL
,is_split bit NOT NULL
);

INSERT dbo.#data
(
 insert_time
,[database]
,partition_function
,days_to_keep_data
,num_future_partitions
,is_bcp
,bcp_path
,partition_boundry_unit
,create_empty_table
,create_file_group
,file_group_physical_path
,is_merge
,is_split
)
SELECT 
 insert_time
,[database]
,partition_function
,days_to_keep_data
,num_future_partitions
,is_bcp
,bcp_path	
,partition_boundry_unit
,create_empty_table
,create_file_group
,file_group_physical_path
,is_merge
,is_split
FROM DBA.dbo.PartitionsManagment WHERE 1=1
AND is_active = 1
--AND partition_function = ''
ORDER BY id; 
--SELECT * FROM #data

DECLARE
	@database sysname
 ,@partition_function sysname
 ,@days_to_keep_data int 
 ,@num_future_partitions int 
 ,@file_group_physical_path varchar(1024)
 ,@is_bcp bit 
 ,@bcp_path varchar(1024) 
 ,@partition_boundry_unit sysname
 ,@create_empty_table bit 
 ,@create_file_group bit
 ,@is_merge bit
 ,@is_split bit;

DECLARE @command varchar(max), @min_id int = 1, @max_id int = (SELECT MAX(id) FROM dbo.#data);


WHILE @min_id <= @max_id
BEGIN;
	-- Parameters assignment
	SELECT
		@database									= [database]
	 ,@partition_function				= partition_function
	 ,@file_group_physical_path	=	ISNULL(file_group_physical_path, 'NONE'	)
	 ,@days_to_keep_data				= days_to_keep_data
	 ,@num_future_partitions		= num_future_partitions
	 ,@partition_boundry_unit		= partition_boundry_unit
	 ,@create_empty_table				= create_empty_table
	 ,@create_file_group				= create_file_group	
	 ,@is_bcp										= is_bcp
	 ,@bcp_path									= ISNULL(bcp_path,'NONE')
	 ,@is_merge									= is_merge
	 ,@is_split									= is_split
 FROM dbo.#data WHERE id = @min_id;
 
 PRINT CHAR(10) + '-- ' + @database + ' | ' + @partition_function --+ ' ' + @empty_table--+ ' ' + cast(@days_to_keep_data as sysname) + ' ' + cast(@num_future_partitions as sysname) + ' ' + @partition_boundry_unit;
 --select @table, @empty_table, @schema

-- Execute
SELECT @command = 'EXEC [' + @database + '].dbo.sp_partition_managment 
		 @partition_function			= ''' + @partition_function + '''
		,@days_to_keep_data				= '		+ CAST(@days_to_keep_data AS varchar(10)) + '
		,@num_future_partitions			= '		+ CAST(@num_future_partitions AS varchar(10))+ '
		,@file_group_physical_path		= '''	+ CAST(@file_group_physical_path AS varchar(150)) + '''
		,@is_bcp						= '		+ CAST(@is_bcp AS char(1))+ '
		,@bcp_path						= ''' + @bcp_path + '''
		,@partition_boundry_unit		= ''' + @partition_boundry_unit + '''
		,@create_empty_table			= '		+ CAST(@create_empty_table AS char(1))	+ '
		,@create_file_group				= '		+ CAST(@create_file_group AS char(1))+ '
		,@is_merge						= '		+ CAST(@is_merge AS char(1))	+ '
		,@is_split						= '		+ CAST(@is_split AS char(1))	+ '
		,@debug							= '		+ CAST(@debug AS char(1))	+ ';';

BEGIN TRY;
	--PRINT 'USE [' + @database + ']';
	--PRINT @command + CHAR(10);
	
	EXEC (@command);
END TRY
BEGIN CATCH;
	DECLARE @ErrorNumber int, @ErrorLine int, @Msg xml, @ErrorProc nvarchar(126), @CurrentTime datetime, @ErrorMessage nvarchar(2048),@ErrorSeverity INT, @ErrorState int; 
	SELECT  @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber = ERROR_NUMBER(), @ErrorSeverity = ERROR_SEVERITY(), @ErrorState = ERROR_STATE(), @ErrorLine = ERROR_LINE(), @ErrorProc = ERROR_PROCEDURE();  

					IF @@TRANCOUNT > 0 ROLLBACK TRAN;
					
					SELECT @ErrorProc = ISNULL(@ErrorProc, CONVERT(NVARCHAR(126), OBJECT_NAME(@@PROCID)));
					SELECT @CurrentTime = CURRENT_TIMESTAMP;
  
					SELECT @Msg = (SELECT  @CurrentTime		AS 'EventTime'
																,@ErrorProc			AS 'ObjectName'
																,@ErrorNumber		AS 'Error/Number'
																,@ErrorMessage	AS 'Error/Message'
																,@ErrorSeverity AS 'Error/Severity'
																,@ErrorState		AS 'Error/State'
																,@ErrorLine			AS 'Error/Line'
													FOR XML PATH('Event'));  

				PRINT	'  ***********  ' + CAST(@Msg AS nvarchar(max));

				INSERT #errors ([partition_function] ,[error_message]) SELECT @partition_function, @ErrorMessage;
				
END CATCH;

	SELECT @min_id += 1;
END;

--IF EXISTS(SELECT * FROM #errors) 	RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine);  


-- If there were any failures we raise error to "fail" the job step
IF EXISTS (SELECT * FROM #errors)
BEGIN;
	DECLARE @failed_partition_function varchar(max) = '';
	
	SELECT @failed_partition_function = @failed_partition_function + partition_function + ', '
	FROM dbo.#errors 
	ORDER BY id;

RAISERROR
    (	N'Failed on the following partition_functions: %s.',
			16, -- Severity.
			1, -- State.
			@failed_partition_function -- First substitution argument.
    ); 
END;
