/*
DECLARE  @table sysname ,@schema sysname,@file_group sysname,@empty_table sysname,@cmd varchar(MAX) ,@command varchar(MAX	)
SELECT @table = '', @schema = 'dbo', @file_group = 'fg', @empty_table = 'empty_table';

EXEC dbo.sp_generate_table_script
  @DBName = ''
 ,@SchemaName = @schema
 ,@TableName = @table
 ,@NewTableName = @empty_table
 ,@FileGroup  = @file_group
 ,@TableCommand  = @command OUTPUT;

PRINT @command;

-----
DECLARE  @table sysname ,@schema sysname,@file_group sysname,@empty_table sysname,@cmd varchar(MAX) ,@command varchar(MAX	);
DECLARE  @DBName sysname, @SchemaName sysname, @TableName sysname, @NewTableName sysname, @FileGroup  sysname, @TableCommand varchar(MAX);

SELECT @table = '', @schema = 'dbo', @file_group = 'fg', @empty_table = 'empty_table';
SELECT  @DBName = DB_NAME(),@SchemaName = @schema,@TableName = @table,@NewTableName = @empty_table,@FileGroup  = @file_group,@TableCommand  = @command ;

*/

USE master; 
SET ANSI_NULLS ON; SET QUOTED_IDENTIFIER ON;

IF OBJECT_ID('dbo.sp_generate_table_script', 'P') IS NOT NULL DROP PROCEDURE dbo.sp_generate_table_script;
GO
CREATE PROCEDURE dbo.sp_generate_table_script
  @DBName varchar(1000)
 ,@SchemaName varchar(1000)
 ,@TableName varchar(1000)
 ,@NewTableName varchar(1000)
 ,@FileGroup sysname
 ,@TableCommand nvarchar(MAX) OUTPUT
 ,@PartitionNumber INT
AS
SET NOCOUNT ON;


BEGIN TRY;
	DECLARE @v_proc_name varchar(100)='sp_generate_table_script';
	DECLARE @v_curr_message varchar(4000);
	DECLARE	@v_step	varchar(100) = 'start';
	DECLARE @data_compression_desc NVARCHAR(60);
	DECLARE @is_ansi_padding bit, @ansi_padding nvarchar(MAX);


-- Get the ANSI PADDING settings. 
-- IF there are no columns of the relevant data types then the results is NULL and in such a case we default to 1
SELECT TOP 1 
	@is_ansi_padding = ISNULL(c.is_ansi_padded, 1)
FROM sys.tables t
INNER JOIN sys.columns c ON c.object_id = t.object_id
INNER JOIN sys.types tp ON tp.user_type_id = c.user_type_id
WHERE t.name = @TableName AND SCHEMA_NAME(t.schema_id) = @SchemaName
AND tp.name IN ('varchar', 'char', 'binary', 'varbinary');

SELECT @ansi_padding = CASE @is_ansi_padding WHEN 1 THEN N'SET ANSI_PADDING ON;' ELSE N'SET ANSI_PADDING OFF;' END;


	DECLARE @g char(1) =CHAR(39);
	DECLARE 
		 @v_fullTableName varchar(256) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName), @v_filegroupOrPFName varchar(128), @v_partitionColumnName  varchar(128)
		
		--@v_sqlStart nvarchar(MAX) = N'IF OBJECT_ID(''' + @SchemaName + N'.' + @NewTableName + N''', ''U'') IS NOT NULL DROP TABLE ' + @SchemaName + '.' + @NewTableName + N';' + CHAR(10) +
		,@v_sqlStart nvarchar(MAX) = @ansi_padding + CHAR(10) + N'CREATE TABLE '+ QUOTENAME(@SchemaName) + '.' + QUOTENAME(@NewTableName) + CHAR(13) + '(' + CHAR(13)
		,@v_params nvarchar(MAX) = N'@SchemaName NVARCHAR(1000), @TableName NVARCHAR(1000), @v_filegroupOrPFName VARCHAR(128) OUTPUT, @v_partitionColumnName  VARCHAR(128) OUTPUT'
		,@v_columnsSQL nvarchar(MAX) = 
		N'USE ' + QUOTENAME(@DBName) +
		N'SELECT @v_filegroupOrPFName = d.name , @v_partitionColumnName = pc.PartitionColumnName
	FROM
		sys.indexes i
		INNER JOIN sys.objects o ON i.[object_id] = o.[object_id]
		INNER JOIN sys.data_spaces d ON d.data_space_id = i.data_space_id
		OUTER APPLY
		(
			SELECT TOP 1 c.name as PartitionColumnName
			FROM
				sys.index_columns ic
				INNER JOIN sys.columns c ON ic.[object_id] = c.[object_id] AND ic.column_id = c.column_id
			WHERE 
				ic.[object_id] = i.[object_id] AND
				ic.index_id = i.index_id AND
				ic.partition_ordinal >= 1
		) AS pc
	WHERE
		SCHEMA_NAME(o.[schema_id]) COLLATE DATABASE_DEFAULT = @SchemaName AND
		o.name COLLATE DATABASE_DEFAULT = @TableName AND
		i.index_id IN (0, 1);

	SELECT
		col.COLUMN_NAME COLLATE DATABASE_DEFAULT as ColumnName,
		prop.isPersisted,
		prop.ComputedColumnDefinition COLLATE DATABASE_DEFAULT,
		col.DATA_TYPE COLLATE DATABASE_DEFAULT as DataType,
		col.CHARACTER_MAXIMUM_LENGTH as CharacterMaximumLength,
		col.NUMERIC_PRECISION as NumericPrecision,
		col.NUMERIC_SCALE as NumericScale,
		CASE 
			WHEN EXISTS( 
				SELECT scol.id FROM syscolumns scol WHERE OBJECT_NAME(scol.id) COLLATE DATABASE_DEFAULT = @TableName AND scol.name = col.COLUMN_NAME AND COLUMNPROPERTY(scol.id, scol.name, ' + @g + N'IsIdentity' + @g + N') = 1 ) 
					THEN ' + @g + N' IDENTITY(' + @g + N' + CAST(IDENT_SEED('''+@SchemaName+'.'+@TableName+''') as VARCHAR(32)) + ' + @g + N',' + @g + N' + CAST(IDENT_INCR('''+@SchemaName+'.'+@TableName+''') as VARCHAR(32)) + ' + @g + N')' + @g + N' 
			ELSE ' + @g + @g + N' 
		END COLLATE DATABASE_DEFAULT as IdentityDefinition,
		prop.DefaultValue COLLATE DATABASE_DEFAULT,
		col.IS_NULLABLE COLLATE DATABASE_DEFAULT as IsNullable
	FROM 
		INFORMATION_SCHEMA.COLUMNS col
		CROSS APPLY
		(
			SELECT 
				so.name as TableName, sc.name as ColumnName, sm.[text] as DefaultValue, cc.column_id as ColumnID, 
				cc.[definition] as ComputedColumnDefinition, 
				CASE WHEN cc.is_persisted = 1 THEN 1 ELSE 0 END isPersisted,
				CASE WHEN cc.is_computed = 1 THEN 1 ELSE 0 END isComputed
			FROM 
				sys.objects so
				INNER JOIN sys.syscolumns sc ON sc.id = so.object_id
				LEFT JOIN sys.syscomments sm ON sm.id = sc.cdefault
				LEFT JOIN sys.computed_columns cc ON cc.object_id = so.object_id AND cc.column_id = sc.colid
			WHERE 
				so.[type] = ' + @g + N'U' + @g + N' AND 
				SCHEMA_NAME(so.[schema_id] ) COLLATE DATABASE_DEFAULT= @SchemaName AND 
				so.name COLLATE DATABASE_DEFAULT = @TableName AND
				sc.name = col.COLUMN_NAME AND 
				so.name = col.TABLE_NAME
		) prop  
	WHERE col.TABLE_SCHEMA COLLATE DATABASE_DEFAULT = @SchemaName AND col.TABLE_NAME COLLATE DATABASE_DEFAULT = @TableName
	ORDER BY col.ORDINAL_POSITION;';

	

	IF OBJECT_ID('tempdb.dbo.#TableColumns', 'U') IS NOT NULL DROP TABLE dbo.#TableColumns;
	CREATE TABLE dbo.#TableColumns 
	(
		RowNum int NOT NULL IDENTITY(1,1) PRIMARY KEY,
		ColumnName varchar(128) NOT NULL,
		IsPersisted bit,
		ComputedColumnDefinition varchar(MAX),
		DataType varchar(128),
		CharacterMaximumLength int,
		NumericPrecision tinyint,
		NumericScale int,
		IdentityDefinition varchar(MAX),
		DefaultValue varchar(MAX),
		IsNullable varchar(3)
	);

	SET @v_step = 'fill @TableColumns'

	INSERT dbo.#TableColumns
	( 
		ColumnName, 
		IsPersisted,
		ComputedColumnDefinition, DataType, CharacterMaximumLength, NumericPrecision, NumericScale, IdentityDefinition, DefaultValue, IsNullable 
	)
	EXEC sp_executesql
		@v_columnsSQL, @v_params,
			@SchemaName = @SchemaName, @TableName = @TableName ,@v_filegroupOrPFName = @v_filegroupOrPFName OUTPUT, @v_partitionColumnName = @v_partitionColumnName OUTPUT;
	

	SET @v_step = 'get cmd1'

	SELECT @TableCommand = ISNULL(@TableCommand, N'')
		+ QUOTENAME(ColumnName) 
		+ CASE WHEN IsPersisted = 1 AND ComputedColumnDefinition IS NOT NULL THEN N' AS ' + ComputedColumnDefinition + N' PERSISTED ' WHEN IsPersisted = 0 AND ComputedColumnDefinition IS NOT NULL THEN N' AS ' + ComputedColumnDefinition + N' ' ELSE N' ' + UPPER(DataType) END
		+ CASE WHEN IsPersisted = 1 OR CharacterMaximumLength IS NULL OR DataType IN (N'xml', N'sql_variant', N'text', N'ntext', N'geometry', N'geography', N'money', N'hierarchyid') THEN N'' ELSE N'(' + CASE WHEN CharacterMaximumLength > 0 THEN CAST(CharacterMaximumLength AS varchar(10)) ELSE N'MAX' END + ')' END 
		+ CASE WHEN IsPersisted = 1 OR NumericPrecision IS NULL OR DataType IN (N'Int', N'tinyint', N'bigint', N'smallint', N'float', N'geometry', N'geography', N'money', N'hierarchyid') THEN N'' ELSE N'(' + CAST(NumericPrecision AS varchar(10)) + N',' + CAST(NumericScale AS varchar(10)) + N')' END 
		+ IdentityDefinition
		+ CASE WHEN DefaultValue IS NOT NULL THEN ' DEFAULT ' + SUBSTRING(DefaultValue, 2, LEN(DefaultValue) - 2) + N' ' ELSE N'' END 
		+ CASE WHEN IsNullable = N'NO' THEN N' NOT NULL' ELSE '' END + CHAR(13) + N','
	FROM dbo.#TableColumns;

	SET @v_step = 'get cmd fg'

	-- get table data compression
	SELECT 
		 @data_compression_desc = data_compression_desc
	FROM sys.partitions p 
	INNER JOIN sys.tables t ON t.object_id = p.object_id 
	INNER JOIN sys.indexes i ON p.object_id = i.object_id and p.index_id = i.index_id
	WHERE i.index_id IN (0, 1)
	AND SCHEMA_NAME(t.[schema_id] ) COLLATE DATABASE_DEFAULT= @SchemaName 
	AND t.name COLLATE DATABASE_DEFAULT = @TableName 
	AND p.partition_number = @PartitionNumber;

		
	SELECT @TableCommand = @v_sqlStart + SUBSTRING(@TableCommand, 0, LEN(@TableCommand) /*- 1*/ ) + N'
	) ON [' + @FileGroup /*@v_filegroupOrPFName*/+ N']' +  /* ISNULL('(' + QUOTENAME(@v_partitionColumnName) + ')', '') +*/  
	N' WITH ( DATA_COMPRESSION = ' + @data_compression_desc + N' );';


END TRY
BEGIN CATCH;
	IF XACT_STATE() = -1 ROLLBACK;
	DECLARE @errmsg AS varchar(1000)
	DECLARE @errline AS int 
	DECLARE @errnum AS int
	SELECT @errmsg=ERROR_MESSAGE() , @errline=ERROR_LINE() , @errnum=ERROR_NUMBER() ;
--	select @errmsg as errmsg, @errline as errline, @errnum as errnum;

	SET @v_curr_message = 'Failed ' + @v_proc_name + 
			' step: ' + @v_step +
			' ErrMsg: '+ @errmsg + 
			' ErrLine: '+CAST(@errline AS varchar(100))+
			' ErrNum: '+CAST(@errnum AS varchar(100));
	--exec write_log  
	--	@in_wf_name = @in_wf_name, 
	--	@in_program = @v_proc_name,
	--	@in_severity =  'ERROR', 
	--	@in_message = @v_curr_message;

	RAISERROR (@v_curr_message, 16, 1)
END CATCH
GO
EXEC sp_ms_marksystemobject 'sp_generate_table_script';
GO
