/*
This script will create stored procedure to 
1. create transactionDateTime column for Account_Info table
2. sort the table in account, transactionDateTime with descent order
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS sortAcctTable
GO

create procedure sortAcctTable @table nvarchar(max)
as
begin

declare @dropTable nvarchar(max) 
set @dropTable = '
drop table if exists ' + @table + '_Sort'
exec sp_executesql @dropTable

declare @sortAcctTableQuery nvarchar(max) 
set @sortAcctTableQuery = '
select *,
convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as transactionDateTime
into ' + @table + '_Sort from ' + @table + '
order by accountID, transactionDateTime desc
'
exec sp_executesql @sortAcctTableQuery
end