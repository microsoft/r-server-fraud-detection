/*
This script will create stored procedure to merge untagged transactions with account level infomations
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS MergeAcctInfo
GO

create procedure MergeAcctInfo @table nvarchar(max)
as
begin

declare @droptable nvarchar(max) 
set @droptable = 'drop table if exists ' + @table + '_Acct'
exec sp_executesql @droptable

/* Merge with AccountInfo_Sort table */
declare @MergeQuery nvarchar(max) 
set @MergeQuery =  
'
select t1.*,
       t2.accountOwnerName,
	   t2.accountAddress,
	   t2.accountPostalCode,
	   t2.accountCity,
	   t2.accountState,
	   t2.accountCountry,
	   t2.accountOpenDate,
	   t2.accountAge,
	   t2.isUserRegistered,
	   t2.paymentInstrumentAgeInAccount,
	   t2.numPaymentRejects1dPerUser
into ' + @table + '_Acct ' +
'from 
 (select *, 
       convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as transactionDateTime
  from ' + @table + ') as t1
 outer apply 
 (select top 1 * -- the top 1 is the maximum transactionDateTime up to current transactionDateTime
  from Account_Info_Sort as t
  where t.accountID = t1.accountID and t.transactionDateTime <= t1.transactionDateTime) as t2
where t1.accountID = t2.accountID
'

exec sp_executesql @MergeQuery
end