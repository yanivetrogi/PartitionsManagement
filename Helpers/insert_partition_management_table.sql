USE DBA;
--SELECT * FROM dbo.PartitionsManagment

INSERT	dbo.PartitionsManagment
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
 ,is_active
 ,file_group_physical_path
 ,is_split
 ,is_merge
)
VALUES
( 
	 GETDATE() -- insert_time - datetime
	,'my_db'   -- database - varchar(64)
	,'my_pf'   -- partition_function - varchar(64)
	,180       -- days_to_keep_data - int
	,30        -- num_future_partitions - int
	,0			-- is_bcp - bit
	,NULL      -- bcp_path - varchar(1024)
	,'Day'     -- partition_boundry_unit - varchar(10)
	,1			-- create_empty_table - bit
	,0			-- create_file_group - bit
	,1			-- is_active - bit
	,NULL       -- file_group_physical_path - varchar(1024)
	,1			-- is_split - bit
	,1			-- is_merge - bit
)
