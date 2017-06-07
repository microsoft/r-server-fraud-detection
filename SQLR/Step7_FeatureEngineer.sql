/*
This script will create stored procedure to do the following feature engineering:
1. create mismatch flags
2. convert categorical variables to numerical by assigning risk values based on risk tables
3. calculate aggregates

input parameters:
@table = the table need to be feature engineered
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS FeatureEngineer
GO

create procedure FeatureEngineer @table nvarchar(max)
as
begin

/* create mismatch flags and assign risk values */ 
declare 
@sql_dropview1 nvarchar(max) = '';
set @sql_dropview1 = '
DROP VIEW IF EXISTS ' + @table + '_features1'
exec sp_executesql @sql_dropview1;

declare @sql_fe1 nvarchar(max) = '';
set @sql_fe1 = 'create view ' + @table + '_features1 as
select t.Label,t.accountID,t.TransDateTime,
cast(t.transactionAmountUSD as float) as transactionAmountUSD,
cast(t.digitalItemCount as int) as digitalItemCount,
cast(t.physicalItemCount as int) as physicalItemCount,
t.isProxyIP,t.paymentInstrumentType,t.cardType,
case when isnumeric(t.accountAge)=1 then cast(t.accountAge as float) else 0 end as accountAge,
case when isnumeric(t.paymentInstrumentAgeInAccount)=1 then cast(t.paymentInstrumentAgeInAccount as float) else 0 end as paymentInstrumentAgeInAccount,
case when isnumeric(t.numPaymentRejects1dPerUser)=1 then cast(t.numPaymentRejects1dPerUser as float) else 0 end as numPaymentRejects1dPerUser,
case when cast(t.transactionAmountUSD as float) > 150 then ''1'' else ''0'' end as is_highAmount,
case when t.paymentBillingAddress = t.accountAddress then ''0'' else ''1'' end as acct_billing_address_mismatchFlag,
case when t.paymentBillingPostalCode = t.accountPostalCode then ''0'' else ''1'' end as acct_billing_postalCode_mismatchFlag,
case when t.paymentBillingCountryCode = t.accountCountry then ''0'' else ''1'' end as acct_billing_country_mismatchFlag,
case when t.paymentBillingName = t.accountOwnerName then ''0'' else ''1'' end as acct_billing_name_mismatchFlag,
case when t.shippingAddress = t.accountAddress then ''0'' else ''1'' end as acct_shipping_address_mismatchFlag,
case when t.shippingAddress = t.paymentBillingAddress then ''0'' else ''1'' end as shipping_billing_address_mismatchFlag,
isnull(ac.risk,0) as accountCountry_risk,
isnull(apc.risk,0) as accountPostalCode_risk,
isnull(actst.risk,0) as accountState_risk,
isnull(bl.risk,0) as browserLanguage_risk,
isnull(ic.risk,0) as ipCountryCode_risk,
isnull(ipc.risk,0) as ipPostCode_risk,
isnull(ips.risk,0) as ipState_risk,
isnull(lh.risk,0) as localHour_risk,
isnull(pbcc.risk,0) as paymentBillingCountryCode_risk,
isnull(pbpc.risk,0) as paymentBillingPostalCode_risk,
isnull(pbst.risk,0) as paymentBillingState_risk,
isnull(tcc.risk,0) as transactionCurrencyCode_risk
from ' +@table + ' as t
left join sql_risk_accountCountry as ac on ac.accountCountry = t.accountCountry
left join sql_risk_accountPostalCode as apc on apc.accountPostalCode = t.accountPostalCode
left join sql_risk_accountState as actst on actst.accountState = t.accountState
left join sql_risk_browserLanguage as bl on bl.browserLanguage = t.browserLanguage
left join sql_risk_ipCountryCode as ic on ic.ipCountryCode = t.ipCountryCode
left join sql_risk_ipPostCode as ipc on ipc.ipPostCode = t.ipPostCode
left join sql_risk_ipState as ips on ips.ipState = t.ipState
left join sql_risk_localHour as lh on lh.localHour = t.localHour
left join sql_risk_paymentBillingCountryCode as pbcc on pbcc.paymentBillingCountryCode = t.paymentBillingCountryCode
left join sql_risk_paymentBillingPostalCode as pbpc on pbpc.paymentBillingPostalCode = t.paymentBillingPostalCode
left join sql_risk_paymentBillingState as pbst on pbst.paymentBillingState = t.paymentBillingState
left join sql_risk_transactionCurrencyCode as tcc on tcc.transactionCurrencyCode = t.transactionCurrencyCode
'
exec sp_executesql @sql_fe1;

/* create aggregates on the fly */
declare 
@sql_dropview nvarchar(max) = '';
set @sql_dropview = '
DROP VIEW IF EXISTS ' + @table + '_features'
exec sp_executesql @sql_dropview;

declare @sql_fe nvarchar(max) = '';
set @sql_fe = 'create view ' + @table + '_features as
select * from ' + @table + '_features1 as t
outer apply
(select sum(case when t2.TransDateTime > last24Hours then cast(t2.transactionAmountUSD as float) end) as sumPurchaseAmount1dPerUser,count(case when t2.TransDateTime > last24Hours then t2.transactionAmountUSD end) as sumPurchaseCount1dPerUser
,sum(cast(t2.transactionAmountUSD as float)) as sumPurchaseAmount30dPerUser,count(t2.transactionAmountUSD) as sumPurchaseCount30dPerUser
from sql_transaction_history as t2
cross apply (values(t.TransDateTime, DATEADD(hour, -24, t.TransDateTime), DATEADD(day, -30, t.TransDateTime))) as c(TransDateTime, last24Hours, last30Days)
where t2.accountID = t.accountID and t2.TransDateTime < t.TransDateTime and t2.TransDateTime > last30Days
) as a1'

exec sp_executesql @sql_fe;
end




