/*
This script will create stored procedure to do preprocessing including:
1. fill missing values with 0
2. remove transactions with negative transaction amount
3. remove transactions with invalide transactionData and time
4. remove prefraud: label == 2

input parameters:
@table = table need to be preprocessed 
*/

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
DROP VIEW IF EXISTS ' + @table + '_Processed'
exec sp_executesql @sql_dropview;

/* create a veiw to do preprocessing */
declare @sql_process nvarchar(max) = '';
set @sql_process = '
create view ' + @table + '_Processed as
select
label,
accountID,
transactionID,
transactionDateTime,
isnull(isProxyIP, ''0'') as isProxyIP, 
isnull(paymentInstrumentType, ''0'') as paymentInstrumentType,
isnull(cardType, ''0'') as cardType,
isnull(paymentBillingAddress, ''0'') as paymentBillingAddress,
isnull(paymentBillingPostalCode, ''0'') as paymentBillingPostalCode,
isnull(paymentBillingCountryCode, ''0'') as paymentBillingCountryCode,
isnull(paymentBillingName, ''0'') as paymentBillingName,
isnull(accountAddress, ''0'') as accountAddress,
isnull(accountPostalCode, ''0'') as accountPostalCode,
isnull(accountCountry, ''0'') as accountCountry,
isnull(accountOwnerName, ''0'') as accountOwnerName,
isnull(shippingAddress, ''0'') as shippingAddress,
isnull(transactionCurrencyCode, ''0'') as transactionCurrencyCode,
isnull(localHour,''-99'') as localHour,
isnull(ipState, ''0'') as ipState,
isnull(ipPostCode, ''0'') as ipPostCode,
isnull(ipCountryCode, ''0'') as ipCountryCode,
isnull(browserLanguage, ''0'') as browserLanguage,
isnull(paymentBillingState, ''0'') as paymentBillingState,
isnull(accountState, ''0'') as accountState,
case when isnumeric(transactionAmountUSD)=1 then cast(transactionAmountUSD as float) else 0 end as transactionAmountUSD,
case when isnumeric(digitalItemCount)=1 then cast(digitalItemCount as float) else 0 end as digitalItemCount,
case when isnumeric(physicalItemCount)=1 then cast(physicalItemCount as float) else 0 end as physicalItemCount,
case when isnumeric(accountAge)=1 then cast(accountAge as float) else 0 end as accountAge,
case when isnumeric(paymentInstrumentAgeInAccount)=1 then cast(paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(numPaymentRejects1dPerUser)=1 then cast(numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
isUserRegistered = case when isUserRegistered like ''%[0-9]%'' then ''0'' else isUserRegistered end
from ' + @table + '
where cast(transactionAmountUSD as float) >= 0 and   
      (case when transactionDateTime is null then 1 else 0 end) = 0 and
	  label < 2' 

exec sp_executesql @sql_process
end


