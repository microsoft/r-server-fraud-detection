/*
This script creates the stored procedure to:
1. ingest a string and store it into a temporary table
2. parse the string and output the parsed string to a sql table 
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS ParseStr
GO

create procedure ParseStr @inputstring VARCHAR(MAX)
as
begin

/* Reformat the long string into XML format whose elements can be retrieved by location index */
declare @parsequery nvarchar(max)
set @parsequery = '
DECLARE @tmp table ( ID int Identity(1,1)  ,[Name] nvarchar(max))
INSERT into @tmp SELECT ''' + @inputstring + '''
drop table if exists Parsed_String
;WITH tmp AS
( 
    SELECT
        CAST(''<M>'' + REPLACE([Name], '','' , ''</M><M>'') + ''</M>'' AS XML) 
        AS [NameParsed]
    FROM  @tmp 
)
SELECT
     [NameParsed].value(''/M[1]'', ''varchar (100)'') As [transactionID],
     [NameParsed].value(''/M[2]'', ''varchar (100)'') As [accountID],
     [NameParsed].value(''/M[3]'', ''varchar (100)'') As [transactionAmountUSD],
	 [NameParsed].value(''/M[4]'', ''varchar (100)'') As transactionAmount,
     [NameParsed].value(''/M[5]'', ''varchar (100)'') As [transactionCurrencyCode],
     [NameParsed].value(''/M[6]'', ''varchar (100)'') As [transactionCurrencyConversionRate],
	 [NameParsed].value(''/M[7]'', ''varchar (100)'') As [transactionDate],
     [NameParsed].value(''/M[8]'', ''varchar (100)'') As [transactionTime],
     [NameParsed].value(''/M[9]'', ''varchar (100)'') As [localHour],
	 [NameParsed].value(''/M[10]'', ''varchar (100)'') As [transactionScenario],
     [NameParsed].value(''/M[11]'', ''varchar (100)'') As [transactionType],
     [NameParsed].value(''/M[12]'', ''varchar (100)'') As [transactionMethod],
	 [NameParsed].value(''/M[13]'', ''varchar (100)'') As [transactionDeviceType],
     [NameParsed].value(''/M[14]'', ''varchar (100)'') As [transactionDeviceId],
     [NameParsed].value(''/M[15]'', ''varchar (100)'') As [transactionIPaddress],
	 [NameParsed].value(''/M[19]'', ''varchar (100)'') As [ipState],     
	 [NameParsed].value(''/M[20]'', ''varchar (100)'') As [ipPostcode],
     [NameParsed].value(''/M[21]'', ''varchar (100)'') As [ipCountryCode],
     [NameParsed].value(''/M[22]'', ''varchar (100)'') As [isProxyIP],
	 [NameParsed].value(''/M[23]'', ''varchar (100)'') As [browserType],
     [NameParsed].value(''/M[24]'', ''varchar (100)'') As [browserLanguage],
     [NameParsed].value(''/M[25]'', ''varchar (100)'') As [paymentInstrumentType],
	 [NameParsed].value(''/M[26]'', ''varchar (100)'') As [cardType],
	 [NameParsed].value(''/M[27]'', ''varchar (100)'') As [cardNumberInputMethod],
     [NameParsed].value(''/M[28]'', ''varchar (100)'') As [paymentInstrumentID],
     [NameParsed].value(''/M[29]'', ''varchar (100)'') As [paymentBillingAddress],
	 [NameParsed].value(''/M[30]'', ''varchar (100)'') As [paymentBillingPostalCode],
     [NameParsed].value(''/M[31]'', ''varchar (100)'') As [paymentBillingState],
     [NameParsed].value(''/M[32]'', ''varchar (100)'') As [paymentBillingCountryCode],
	 [NameParsed].value(''/M[33]'', ''varchar (100)'') As [paymentBillingName],
     [NameParsed].value(''/M[34]'', ''varchar (100)'') As [shippingAddress],
     [NameParsed].value(''/M[35]'', ''varchar (100)'') As [shippingPostalCode],
	 [NameParsed].value(''/M[36]'', ''varchar (100)'') As [shippingCity],
	 [NameParsed].value(''/M[37]'', ''varchar (100)'') As [shippingState],
     [NameParsed].value(''/M[38]'', ''varchar (100)'') As [shippingCountry],
     [NameParsed].value(''/M[39]'', ''varchar (100)'') As [cvvVerifyResult],
	 [NameParsed].value(''/M[40]'', ''varchar (100)'') As [responseCode],
     [NameParsed].value(''/M[41]'', ''varchar (100)'') As [digitalItemCount],
     [NameParsed].value(''/M[42]'', ''varchar (100)'') As [physicalItemCount],
	 [NameParsed].value(''/M[43]'', ''varchar (100)'') As [purchaseProductType]
into Parsed_String  
FROM tmp'
exec sp_executesql @parsequery
end