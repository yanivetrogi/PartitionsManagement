USE master;
IF OBJECT_ID('sp_create_file_group', 'P') IS NOT NULL DROP PROCEDURE sp_create_file_group;
GO
CREATE PROCEDURE sp_create_file_group
(
	 @file_group_command varchar(1024)
	,@data_file_command varchar(1024)
)
AS
SET NOCOUNT ON;


BEGIN TRY;
	EXEC(@file_group_command);
	EXEC(@data_file_command);

	RETURN 0;
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
																,@ErrorMessage		AS 'Error/Message'
																,@ErrorSeverity		AS 'Error/Severity'
																,@ErrorState		AS 'Error/State'
																,@ErrorLine			AS 'Error/Line'
													FOR XML PATH('Event'));  
  
        RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState, @ErrorLine);  
		RETURN(1);
END CATCH;
go
EXEC sp_ms_marksystemobject 'sp_create_file_group';
