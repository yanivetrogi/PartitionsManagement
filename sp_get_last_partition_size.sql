USE master
IF OBJECT_ID('dbo.sp_get_last_partition_size', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_get_last_partition_size
go
CREATE PROCEDURE dbo.sp_get_last_partition_size
(
	@table sysname
)
/*
	returns the size in mb of the last partition while excluding the current active partition
*/
AS
SET NOCOUNT ON;
SET TRAN ISOLATION LEVEL READ UNCOMMITTED;


DECLARE @max_partition_number int, @size_mb bigint;


SELECT 
	@max_partition_number = MAX(p.partition_number)	
FROM  sys.partitions p 
WHERE OBJECT_NAME(p.object_id) LIKE @table
AND p.index_id IN (0, 1)
AND p.rows > 0;
--SELECT @max_partition_numberm

SELECT @size_mb = au.used_pages /128 + 64	-- increment 64 for cases where the returned value is 0
FROM  sys.partitions p 
INNER JOIN sys.allocation_units au ON au.container_id = p.partition_id
WHERE OBJECT_NAME(p.object_id) LIKE @table
AND au.type_desc = N'IN_ROW_DATA'
AND p.index_id IN (0, 1)
AND p.partition_number = @max_partition_number -1;

RETURN ISNULL(@size_mb, 64);
GO

EXEC sp_ms_marksystemobject 'sp_get_last_partition_size';
