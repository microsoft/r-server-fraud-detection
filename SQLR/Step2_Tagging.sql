/*
This script will create stored procedure to do the followings: 
1. uniform transactionTime to 6 digits
2. remove duplicated rows
3. tagging on account level

input parameters:
@untaggedtable = table of untagged transactions
@fraudtable = table of fraud transactions
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS Tagging
GO

create procedure Tagging
@untaggedtable varchar(max),
@fraudtable varchar(max)
as
begin

DROP TABLE IF EXISTS sql_taggedData;

/***********************************************************************/
/* reformat transactionTime and create TransDateTime for fraud transactions*/
/**********************************************************************/
/* ##table is a global temporary table which will be written only once to temporary database */ 
declare @makeTransDateTime nvarchar(max)
set @makeTransDateTime = 
'
select *,
  convert(datetime,stuff(stuff(stuff(concat(transactionDate,dbo.FormatTime(transactionTime)), 9, 0, '' ''), 12, 0, '':''), 15, 0, '':'')) as TransDateTime
into ##sql_formatted_fraud
from ' + @fraudtable

exec sp_executesql @makeTransDateTime
/*****************************************************************************************************************/
/* remove duplicate based on keys: transactionID, accountID, transactionDate, transactionDate, transactionAmount */
/*****************************************************************************************************************/
/* sometimes an entire transaction might be divided into multiple sub-transactions. thus, even transactionID, accountID, transactionDate/Time are same, the amount might be different */
declare @removeduplicates1 nvarchar(max)
set @removeduplicates1 = 
';WITH cte_1
     AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, TransDateTime, transactionAmount
                                       ORDER BY transactionID ASC) RN 
         FROM ' + @untaggedtable + ')
DELETE FROM cte_1
WHERE  RN > 1;'
exec sp_executesql @removeduplicates1

;WITH cte_2
     AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDate, TransDateTime, transactionAmount
                                       ORDER BY transactionID ASC) RN 
         FROM ##sql_formatted_fraud)
DELETE FROM cte_2
WHERE  RN > 1;


/*********************************************************************************************************************/
/* tagging on account level:  
   if accountID can't be found in fraud dataset => tag as 0, non fraud
   if accountID found in fraud dataset but transactionDateTime is out of the fraud time range => tag as 2, pre-fraud
   if accountID found in fraud dataset and transactionDateTime is within the fraud time range => tag as 1, fraud */
/**********************************************************************************************************************/
/* convert fraud to account level and create start and end date time */
select accountID, min(TransDateTime) as startDateNTime,  max(TransDateTime) as endDateNTime
into ##sql_fraud_account
from ##sql_formatted_fraud 
group by accountID


/* Tagging */ 
declare @tagging nvarchar(max)
set @tagging = 
'select t.*, 
	   case 
         when (sDT is not null and tDT >= sDT and tDT <= eDT) then 1
		 when (sDT is not null and tDT < sDT) then 2
		 when (sDT is not null and tDT > eDT) then 2
		 when sDT is null then 0
	   end as Label
into sql_taggedData
from 
(select t1.*,
  t1.TransDateTime as tDT,
  t2.startDateNTime as sDT,
  t2.endDateNTime as eDT
 from ' + @untaggedtable + ' as t1
 left join
 ##sql_fraud_account as t2
 on t1.accountID = t2.accountID
 ) t'
exec sp_executesql @tagging

drop table ##sql_fraud_account
drop table ##sql_formatted_fraud 
end