/* 
This script will create stored procedure to create risk table for each input variable 

input parameters:
@name = the name of the variable to generate risk table for
@table_name = the name of the output risk table
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS CreateRiskTable
GO

create procedure CreateRiskTable 
@name varchar(max),
@table_name varchar(max)
as
begin
declare @filltablesql nvarchar(max)
declare @droptablesql nvarchar(max)
declare @removenullconstrain nvarchar(max)
declare @addprimarykey nvarchar(max)

/* drop corresponding table if it already exists */
set @droptablesql = 'DROP TABLE IF EXISTS ' + @table_name
exec sp_executesql @droptablesql

/* create risk table */
set @filltablesql = 'select ' + @name + ' , log(odds/(1-odds)) as risk 
            into .dbo.' + @table_name + 
			' from (select distinct ' + @name + ' ,cast((sum(label)+10) as float)/cast((sum(label)+sum(1-label)+100) as float) as odds 
			from Tagged_Training_Processed group by ' + @name + ' ) temp'

/* example: when @name=localHour, @table_name=Risk_LocalHour, @sql is the following:
select localHour , log(odds/(1-odds)) as risk 
            into Risk_LocalHour from (select distinct localHour ,cast((sum(label)+10) as float)/cast((sum(label)+sum(1-label)+100) as float) as odds 
			from Tagged_Training group by localHour ) temp
*/

exec sp_executesql @filltablesql
end