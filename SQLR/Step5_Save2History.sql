/*
This script will create stored procedure to do the followings:
1. truncate historical table if truncateflag = '1'
2. save transactions to historical table

input parameters:
@table = table of transactions wanted to save into historical table
@truncateflag = indicate whether the historical table need to be truncated: '1'=yes, '0'=no
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS Save2TransactionHistory
GO

create procedure Save2TransactionHistory @table nvarchar(max), 
                                         @truncateflag nvarchar(max) 
as
begin

/* truncate historical table if truncateflag = '1' */
declare @truncatetable nvarchar(max) = '';
set @truncatetable = 'if cast(' + @truncateflag + ' as int) = 1 truncate table Transaction_History'
exec sp_executesql @truncatetable

/* insert transactions into historical table */
declare @sql_save2history nvarchar(max) = '';
set @sql_save2history ='
insert into Transaction_History
select accountID, transactionID, transactionDateTime, transactionAmountUSD from ' + @table + ';'
exec sp_executesql @sql_save2history

end