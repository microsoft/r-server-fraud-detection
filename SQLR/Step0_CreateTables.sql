/*
The script will create the following tables:
1. table for untagged transactions
2. table for account information
3. table for fraud transactions
4. table storing historical transactions which will be used for calculating aggregates
*/

set ansi_nulls on
go

set quoted_identifier on
go

drop table if exists Untagged_Transactions
create table Untagged_Transactions (
transactionID varchar(255),
accountID varchar(255),
transactionAmountUSD varchar(255),
transactionAmount varchar(255),
transactionCurrencyCode varchar(255),
transactionCurrencyConversionRate varchar(255),
transactionDate varchar(255),
transactionTime varchar(255),
localHour varchar(255),
transactionScenario varchar(255),
transactionType varchar(255),
transactionMethod varchar(255),
transactionDeviceType varchar(255),
transactionDeviceId varchar(255),
transactionIPaddress varchar(255),
ipState varchar(255),
ipPostcode varchar(255),
ipCountryCode varchar(255),
isProxyIP varchar(255),
browserType varchar(255),
browserLanguage varchar(255),
paymentInstrumentType varchar(255),
cardType varchar(255),
cardNumberInputMethod varchar(255),
paymentInstrumentID varchar(255),
paymentBillingAddress varchar(255),
paymentBillingPostalCode varchar(255),
paymentBillingState varchar(255),
paymentBillingCountryCode varchar(255),
paymentBillingName varchar(255),
shippingAddress varchar(255),
shippingPostalCode varchar(255),
shippingCity varchar(255),
shippingState varchar(255),
shippingCountry varchar(255),
cvvVerifyResult varchar(255),
responseCode varchar(255),
digitalItemCount varchar(255),
physicalItemCount varchar(255),
purchaseProductType varchar(255)
);

drop table if exists Account_Info
create table Account_Info (
accountID varchar(255),
transactionDate varchar(255),
transactionTime varchar(255),  
accountOwnerName varchar(255),
accountAddress varchar(255),
accountPostalCode varchar(255),
accountCity varchar(255),
accountState varchar(255),
accountCountry varchar(255),
accountOpenDate varchar(255),
accountAge varchar(255),
isUserRegistered varchar(255),
paymentInstrumentAgeInAccount varchar(255),
numPaymentRejects1dPerUser varchar(255)
);

drop table if exists Fraud
create table Fraud (
transactionID varchar(255),
accountID varchar(255),
transactionAmount varchar(255),
transactionCurrencyCode varchar(255),
transactionDate varchar(255), 
transactionTime varchar(255),
localHour varchar(255),
transactionDeviceId varchar(255),
transactionIPaddress varchar(255)
);

drop table if exists Transaction_History
create table Transaction_History
(
accountID varchar(255),
transactionID varchar(255),
transactionDateTime datetime,
transactionAmountUSD varchar(255)
); 

