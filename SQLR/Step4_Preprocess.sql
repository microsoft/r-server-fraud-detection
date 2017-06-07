/*
This script will create stored procedure to do preprocessing including:
1. fill missing values with 0
2. remove transactions with negative transaction amount
3. remove transactions with invalide transactionData and time

input parameters:
@table = table need to be preprocessed 
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS Preprocess
GO

create procedure Preprocess @table nvarchar(max)
as
begin

/* drop view if exists */
declare 
@sql_dropview nvarchar(max) = '';
set @sql_dropview = '
DROP VIEW IF EXISTS ' + @table + '_processed'
exec sp_executesql @sql_dropview;

/* create a veiw to do preprocessing */
declare @sql_process nvarchar(max) = '';
set @sql_process = '
create view ' + @table + '_processed as
select
Label,
accountID,
transactionID,
TransDateTime,
isProxyIP,
paymentInstrumentType,
cardType,
paymentBillingAddress,
paymentBillingPostalCode,
paymentBillingCountryCode,
paymentBillingName,
accountAddress,
accountPostalCode,
accountCountry,
accountOwnerName,
shippingAddress,
transactionCurrencyCode,
isnull(localHour,-99) as localHour,
ipState,
ipPostCode,
ipCountryCode,
browserLanguage,
paymentBillingState,
accountState,
case when transactionAmountUSD = ''""'' then ''0'' else transactionAmountUSD end as transactionAmountUSD,
case when digitalItemCount = ''""'' then ''0'' else digitalItemCount end as digitalItemCount,
case when physicalItemCount = ''""'' then ''0'' else physicalItemCount end as physicalItemCount,
case when accountAge = ''""'' then ''0'' else accountAge end as accountAge,
case when paymentInstrumentAgeInAccount = ''""'' then ''0'' else paymentInstrumentAgeInAccount end as paymentInstrumentAgeInAccount,
case when numPaymentRejects1dPerUser = ''""'' then ''0'' else numPaymentRejects1dPerUser end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like ''%[0-9]%'' then ''0'' else isUserRegistered end
from ' + @table + '
where cast(transactionAmountUSD as float) >= 0 and   
      (case when TransDateTime is null then 1 else 0 end) = 0' 

exec sp_executesql @sql_process
end


