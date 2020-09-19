USE master;
IF OBJECT_ID('sp_generate_file_group_command', 'P') IS NOT NULL DROP PROCEDURE sp_generate_file_group_command;
GO
CREATE PROCEDURE sp_generate_file_group_command
(
	 @partition_function sysname
	,@partition_schema sysname
	,@fg_name_wildcard nvarchar(256)
	,@fg_physical_path sysname
	,@fg_file_size_mb int
	,@fg_file_growth_mb int
	,@fg_filecount int 
	,@fg_type int --1=Yearly,2=Quarterly,3=Monthly,4=Weekly,5=Daily
	,@new_file_group sysname OUTPUT
	,@new_file_group_command varchar(512) OUTPUT
	,@new_data_file_command varchar(1024) OUTPUT
	,@start_date datetime
	,@end_date datetime
  ,@debug bit	 = 0
)
AS
SET NOCOUNT ON;


BEGIN TRY;
  DECLARE @database sysname = DB_NAME();


--DECLARE @partition_function sysname,  @partition_schema sysname, @fg_name_wildcard nvarchar(256), @fg_physical_path sysname, @fg_file_size_mb int, @fg_file_growth_mb int, @fg_filecount int;
	IF OBJECT_ID('tempdb.dbo.#file_groups_dates', 'U') IS NOT NULL DROP TABLE #file_groups_dates;
	CREATE TABLE #file_groups_dates (StartDate datetime, EndDate datetime);


--PRINT '--Scrip to create ' + CASE @fg_type WHEN 1 THEN 'yearly' WHEN 2 THEN 'quarterly' WHEN 3 THEN 'monthly' WHEN 4 THEN 'weekly' WHEN 5 THEN 'daily' END + ' filegroups with files to database ' + @database;
--PRINT '--Created on ' + QUOTENAME(@@SERVERNAME) + ' at ' + QUOTENAME(CONVERT(VARCHAR(10), GETDATE(),101) + ' ' + CONVERT(VARCHAR(10),GETDATE(),108))
 

  DECLARE @partition_key_type sysname = 'int';
  WITH cte
          AS (
              SELECT @start_date StartDate
               ,CASE @fg_type
                  WHEN 1 THEN DATEADD(yy, DATEDIFF(yy, 0, @start_date), 365)
                  WHEN 2 THEN DATEADD(qq, DATEDIFF(qq, 0, @start_date), 4)
                  WHEN 3 THEN DATEADD(mm, DATEDIFF(mm, 0, @start_date) + 1, 0)
                  WHEN 4 THEN DATEADD(wk, DATEDIFF(wk, 0, @start_date), 8)
                  WHEN 5 THEN DATEADD(dd, DATEDIFF(dd, 0, @start_date), 1)
                END EndDate
              UNION ALL
              SELECT
                CASE @fg_type
                  WHEN 1 THEN DATEADD(yy, 1, StartDate)
                  WHEN 2 THEN DATEADD(qq, 1, StartDate)
                  WHEN 3 THEN DATEADD(mm, 1, StartDate)
                  WHEN 4 THEN DATEADD(wk, 1, StartDate)
                  WHEN 5 THEN DATEADD(dd, 1, StartDate)
                END
               ,CASE @fg_type
                  WHEN 1 THEN DATEADD(yy, 1, EndDate)
                  WHEN 2 THEN DATEADD(qq, 1, EndDate)
                  WHEN 3 THEN DATEADD(mm, 1, EndDate)
                  WHEN 4 THEN DATEADD(wk, 1, EndDate)
                  WHEN 5 THEN DATEADD(dd, 1, EndDate)
                END

              FROM cte WHERE
                (CASE @fg_type
                   WHEN 1 THEN DATEADD(yy, 1, StartDate)
                   WHEN 2 THEN DATEADD(qq, 1, StartDate)
                   WHEN 3 THEN DATEADD(mm, 1, StartDate)
                   WHEN 4 THEN DATEADD(wk, 1, StartDate)
                   WHEN 5 THEN DATEADD(dd, 1, StartDate)
                 END) < @end_date
             )
    INSERT  INTO #file_groups_dates SELECT * FROM cte OPTION (MAXRECURSION 2000);
		--SELECT * FROM #file_groups_dates;

  
  DECLARE @add_fFile_group_command varchar(2000), @add_file_to_file_group_command varchar(2000)

  SELECT @add_fFile_group_command = 'ALTER DATABASE ' + QUOTENAME(@database) + ' ADD FILEGROUP ';
  SELECT @add_file_to_file_group_command = 'ALTER DATABASE ' + QUOTENAME(@database) + ' ADD FILE (NAME = 1*, FILENAME = ''2*'', SIZE = 3*mb, MAXSIZE = UNLIMITED, FILEGROWTH = 4*MB) TO FILEGROUP [5*];';
  --PRINT @add_file_to_file_group_command

   --LOOP VARS
  DECLARE @cur_StartDate datetime, @cur_EndDate datetime;
  DECLARE @Y int,@Q int ,@M int,@W int,@D int;
	DECLARE @FG_DATE_STR varchar(10)
				 ,@FG_NAME varchar(256)
				 ,@FG_FILE_COUNTER int
				 ,@FG_FILE_NAME varchar(256)
				 ,@FG_FILE_PHYSICAL_NAME varchar(1256)
				 ,@FG_FILE_NAME_IN varchar(256)
				 ,@FG_FILE_PHYSICAL_NAME_IN varchar(1256)
				 ,@FG_ADD varchar(2000)
				 ,@FG_FILE_ADD varchar(2000)
   
   
   --loop through dates
  DECLARE PrintCur CURSOR STATIC LOCAL FORWARD_ONLY FOR
		SELECT * FROM #file_groups_dates ORDER BY 1;
  OPEN PrintCur;
  FETCH NEXT FROM PrintCur INTO @cur_StartDate, @cur_EndDate;
  WHILE @@FETCH_STATUS = 0
    BEGIN;
      SELECT
        @Y = DATEPART(yy, @cur_StartDate)
       ,@Q = DATEPART(qq, @cur_StartDate)
       ,@M = DATEPART(mm, @cur_StartDate)
       ,@W = DATEPART(wk, @cur_StartDate)
       ,@D = DATEPART(dd, @cur_StartDate);
      SELECT @FG_DATE_STR = CAST(@Y AS char(4));
      SELECT @FG_DATE_STR = @FG_DATE_STR + CASE @fg_type
                                          WHEN 2 THEN '_Q' + CAST(@Q AS varchar(2)) WHEN 3 THEN '_' + CASE WHEN @M < 10 THEN '0' + CAST(@M AS varchar(2)) ELSE CAST(@M AS varchar(2)) END
                                          WHEN 4 THEN '_W' + CASE WHEN @W < 10 THEN '0' + CAST(@W AS varchar(2)) ELSE CAST(@W AS varchar(2)) END 
																					WHEN 5 THEN '_'  + CASE WHEN @M < 10 THEN '0' + CAST(@M AS varchar(2)) ELSE CAST(@M AS varchar(2)) END 
																					+ '_' + CASE WHEN @D < 10 THEN '0' + CAST(@D AS varchar(2)) ELSE CAST(@D AS varchar(2)) END ELSE '' END;
		
			-- Add file group      
      SET  @FG_NAME = REPLACE( REPLACE(@fg_name_wildcard, '*', @FG_DATE_STR), ' ', '');		
      SET @FG_ADD = @add_fFile_group_command + QUOTENAME(@FG_NAME) + ';';
      

      SELECT @new_file_group = @FG_NAME, @new_file_group_command = @FG_ADD;

			-- Add file to file group
      SET @FG_FILE_PHYSICAL_NAME = CASE RIGHT(@fg_physical_path, 1)
                                     WHEN '\' THEN @fg_physical_path
                                     ELSE @fg_physical_path + '\'
                                   END + CASE LEFT(@FG_NAME, 3)
                                           WHEN 'FG_' THEN SUBSTRING(@FG_NAME, 4, LEN(@FG_NAME))
                                           ELSE @FG_NAME
                                         END;
      SET @FG_FILE_NAME = CASE LEFT(@FG_NAME, 3)
                            WHEN 'FG_' THEN SUBSTRING(@FG_NAME, 4, LEN(@FG_NAME))
                            ELSE @FG_NAME
                          END;
      IF @fg_filecount > 1
        BEGIN;
          SET @FG_FILE_COUNTER = 1;		
          WHILE @FG_FILE_COUNTER <= @fg_filecount
            BEGIN;
              SELECT @FG_FILE_NAME_IN = @FG_FILE_NAME + '_file' + CAST(@FG_FILE_COUNTER AS varchar(2));
              SELECT @FG_FILE_PHYSICAL_NAME_IN = @FG_FILE_PHYSICAL_NAME + '_file' + CAST(@FG_FILE_COUNTER AS varchar(2)) + '.ndf';
              SELECT @FG_FILE_ADD = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@add_file_to_file_group_command, '1*', @FG_FILE_NAME_IN), '2*', @FG_FILE_PHYSICAL_NAME_IN), '3*', CAST(@fg_file_size_mb AS varchar(11))), '4*', CAST(@fg_file_growth_mb AS varchar(11))), '5*',@FG_NAME);
              SELECT @FG_FILE_COUNTER = @FG_FILE_COUNTER + 1;
            END;	
        END;
      ELSE
        BEGIN;
          SELECT @FG_FILE_NAME_IN = @FG_FILE_NAME;
          SELECT @FG_FILE_PHYSICAL_NAME_IN = @FG_FILE_PHYSICAL_NAME + '.ndf';
          SELECT @FG_FILE_ADD = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(@add_file_to_file_group_command, '1*', @FG_FILE_NAME_IN), '2*', @FG_FILE_PHYSICAL_NAME_IN), '3*',CAST(@fg_file_size_mb AS varchar(11))), '4*', CAST(@fg_file_growth_mb AS varchar(11))), '5*', @FG_NAME);
        END;      
				SELECT @new_data_file_command = @FG_FILE_ADD;
      FETCH NEXT FROM PrintCur INTO @cur_StartDate, @cur_EndDate;
    END;
  CLOSE PrintCur; DEALLOCATE PrintCur;  

  RETURN; --(0);
END TRY

BEGIN CATCH;
 DECLARE @ErrorNumber int, @ErrorLine int, @Msg xml, @ErrorProc nvarchar(126), @CurrentTime datetime
				, @ErrorMessage nvarchar(2048),@ErrorSeverity INT, @ErrorState int; 
  
	SELECT  @ErrorMessage = ERROR_MESSAGE(), @ErrorNumber = ERROR_NUMBER(), @ErrorSeverity = ERROR_SEVERITY()
				, @ErrorState = ERROR_STATE(), @ErrorLine = ERROR_LINE(), @ErrorProc = ERROR_PROCEDURE();  

					IF @@TRANCOUNT > 0 ROLLBACK TRAN;

					SELECT @ErrorState = CASE WHEN @ErrorState BETWEEN 1 AND 127 THEN @ErrorState ELSE 1 END;
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
  
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine);  
END CATCH;
GO
EXEC sp_ms_marksystemobject 'sp_generate_file_group_command';


