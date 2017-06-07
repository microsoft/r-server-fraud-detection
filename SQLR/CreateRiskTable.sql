/* 
This script will create stored procedure to create risk table for each input variable 

input parameters:
@name = the name of the variable to generate risk table for
@table_name = the name of the output risk table
*/

use [OnlineFraudDetection]
go

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
			' from (select distinct ' + @name + ' ,cast((sum(Label)+10) as float)/cast((sum(Label)+sum(1-Label)+100) as float) as odds 
			from sql_tagged_training_processed group by ' + @name + ' ) temp'

/* example: when @name=localHour, @table_name=sql_risk_localHour, @sql is the following:
select localHour , log(odds/(1-odds)) as risk 
            into sql_risk_localHour from (select distinct localHour ,cast((sum(Label)+10) as float)/cast((sum(Label)+sum(1-Label)+100) as float) as odds 
			from sql_tagged_training group by localHour ) temp
*/

exec sp_executesql @filltablesql
end