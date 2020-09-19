/*
	Thhe functions in this scrip[t are required for any user database that is managed by the PartionManagmnent.
*/

USE StackOverflow2010;


-- 1. IntToDatetime: returns datetime
IF OBJECT_ID('dbo.IntToDatetime', 'FN') IS NOT NULL DROP FUNCTION dbo.IntToDatetime;
GO
CREATE FUNCTION [dbo].[IntToDatetime] 
(
	@DateID bigint 
)
/*
	Yaniv Etrogi 20151201
	Convert numeric values to datetime 

	Support cases where the input numeric value is 4, 8 or more figures
*/
RETURNS datetime 
AS
BEGIN;
	DECLARE @Result datetime;


IF LEN(@DateID) > 8 
BEGIN;
		SELECT @Result = CAST( CAST( @DateID/1000000 as varchar(4) ) + '-' + CAST( (@DateID/10000)%100 as varchar(2) ) + '-' 
			+ CAST( (@DateID/100)%100 as varchar(2) ) + ' ' + CAST( @DateID%100 as varchar(2) ) + ':00:00.000' as DATETIME );
END;

IF LEN(@DateID) = 8 
BEGIN;
	SELECT @DateID = @DateID*100
	SELECT @Result = CAST( CAST( @DateID/1000000 as varchar(4) ) + '-' + CAST( (@DateID/10000)%100 as varchar(2) ) + '-' 
			+ CAST( (@DateID/100)%100 as varchar(2) ) + ' ' + CAST( @DateID%100 as varchar(2) ) + ':00:00.000' as DATETIME );
END;


IF LEN(@DateID) = 6
BEGIN;
	SELECT @Result =  CAST(CAST( @DateID/100 as varchar(4) ) + '-' + CAST( (@DateID)%100 as varchar(2) ) + '-01 '  + '00:00:00.000'  AS datetime);
END;


	RETURN @Result;
END;
GO
-----------------------------------------------------------------------------------------------------------------------------------


-- 2. DatetimeToInt: returns int
IF OBJECT_ID('dbo.DatetimeToInt', 'FN') IS NOT NULL DROP FUNCTION dbo.DatetimeToInt;
GO
CREATE FUNCTION dbo.DatetimeToInt
(
	 @DateVal datetime
	,@ReturnLength tinyint
)
/*
	Yaniv Etrogi 20151208
	Accepts a datetime value and returns a numeric datetime value based on the required length
*/
RETURNS int
AS
BEGIN;
	DECLARE @Result int;

	IF @ReturnLength = 6
	BEGIN;
		SELECT @Result = (( YEAR(@DateVal) * 100 + MONTH(@DateVal)) );
	END;

	IF @ReturnLength = 8
	BEGIN;
		SELECT @Result = (( YEAR(@DateVal) * 100 + MONTH(@DateVal)) * 100 + DAY(@DateVal)) 
	END;

	IF @ReturnLength = 10
	BEGIN;
		SELECT @Result = (( YEAR(@DateVal) * 100 + MONTH(@DateVal)) * 100 + DAY(@DateVal)) * 100 + datepart(hour,@DateVal);
	END;

	IF @ReturnLength = 12
	BEGIN;
		SELECT @Result = CAST ((( YEAR(@DateVal) * 100 + MONTH(@DateVal)) * 100 + DAY(@DateVal)) * 100 + datepart(hour,@DateVal)AS bigint)* 100 + DATEPART(minute,@DateVal);
	END;
	
	
	RETURN @Result;
END;
GO
-----------------------------------------------------------------------------------------------------------------------------------








/*
-----------------------------------------------------------------------------------------------------------------------------------

IF OBJECT_ID('dbo.fn_RemoveChars', 'FN') IS NOT NULL DROP FUNCTION dbo.fn_RemoveChars;
GO
CREATE FUNCTION [dbo].[fn_RemoveChars](@inputString VARCHAR(8000), @ValidChars VARCHAR(1000))
RETURNS VARCHAR(8000) 
AS
BEGIN;
  
    WHILE @inputString like '%[^' + @validChars + ']%'
        SELECT @inputString = REPLACE(@inputString,SUBSTRING(@inputString,PATINDEX('%[^' + @validChars + ']%',@inputString),1),'');
 
    RETURN @inputString;
END;
GO



-- returns int
go
CREATE FUNCTION dbo.DatetimeToDateIdInt
(
	@DateVal datetime
)
RETURNS int
AS
BEGIN;
	DECLARE @Result int;
	
	SELECT @Result = (( YEAR(@DateVal) * 100 + MONTH(@DateVal)) * 100 + DAY(@DateVal)) * 100+ datepart(hour,@DateVal);

	RETURN @Result;
END;
GO
-----------------------------------------------------------------------------------------------------------------------------------


-- returns bigint
go
CREATE FUNCTION dbo.DatetimeToDateIdBigint
(
	@DateVal datetime 
)
RETURNS bigint 
AS
BEGIN;
	DECLARE @Result bigint;
	
	SELECT @Result = CAST ((( YEAR(@DateVal) * 100 + MONTH(@DateVal)) * 100 + DAY(@DateVal)) * 100+ datepart(hour,@DateVal)AS BIGINT)*100+datepart(minute,@DateVal);
	RETURN @Result;
END;
GO
-----------------------------------------------------------------------------------------------------------------------------------


*/