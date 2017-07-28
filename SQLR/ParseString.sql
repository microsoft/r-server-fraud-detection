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
     case when [NameParsed].value(''/M[1]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[1]'', ''varchar (100)'') end As [transactionID],
     case when [NameParsed].value(''/M[2]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[2]'', ''varchar (100)'') end As [accountID],
     case when [NameParsed].value(''/M[3]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[3]'', ''varchar (100)'') end As [transactionAmountUSD],
	 case when [NameParsed].value(''/M[4]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[4]'', ''varchar (100)'') end As transactionAmount,
     case when [NameParsed].value(''/M[5]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[5]'', ''varchar (100)'') end As [transactionCurrencyCode],
     case when [NameParsed].value(''/M[6]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[6]'', ''varchar (100)'') end As [transactionCurrencyConversionRate],
	 case when [NameParsed].value(''/M[7]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[7]'', ''varchar (100)'') end As [transactionDate],
     case when [NameParsed].value(''/M[8]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[8]'', ''varchar (100)'') end As [transactionTime],
     case when [NameParsed].value(''/M[9]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[9]'', ''varchar (100)'') end As [localHour],
	 case when [NameParsed].value(''/M[10]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[10]'', ''varchar (100)'') end As [transactionScenario],
     case when [NameParsed].value(''/M[11]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[11]'', ''varchar (100)'') end As [transactionType],
     case when [NameParsed].value(''/M[12]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[12]'', ''varchar (100)'') end As [transactionMethod],
	 case when [NameParsed].value(''/M[13]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[13]'', ''varchar (100)'') end As [transactionDeviceType],
     case when [NameParsed].value(''/M[14]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[14]'', ''varchar (100)'') end As [transactionDeviceId],
     case when [NameParsed].value(''/M[15]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[15]'', ''varchar (100)'') end As [transactionIPaddress],
	 case when [NameParsed].value(''/M[16]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[16]'', ''varchar (100)'') end As [ipState],     
	 case when [NameParsed].value(''/M[17]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[17]'', ''varchar (100)'') end As [ipPostcode],
     case when [NameParsed].value(''/M[18]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[18]'', ''varchar (100)'') end As [ipCountryCode],
     case when [NameParsed].value(''/M[19]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[19]'', ''varchar (100)'') end As [isProxyIP],
	 case when [NameParsed].value(''/M[20]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[20]'', ''varchar (100)'') end As [browserType],
     case when [NameParsed].value(''/M[21]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[21]'', ''varchar (100)'') end As [browserLanguage],
     case when [NameParsed].value(''/M[22]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[22]'', ''varchar (100)'') end As [paymentInstrumentType],
	 case when [NameParsed].value(''/M[23]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[23]'', ''varchar (100)'') end As [cardType],
	 case when [NameParsed].value(''/M[24]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[24]'', ''varchar (100)'') end As [cardNumberInputMethod],
     case when [NameParsed].value(''/M[25]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[25]'', ''varchar (100)'') end As [paymentInstrumentID],
     case when [NameParsed].value(''/M[26]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[26]'', ''varchar (100)'') end As [paymentBillingAddress],
	 case when [NameParsed].value(''/M[27]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[27]'', ''varchar (100)'') end As [paymentBillingPostalCode],
     case when [NameParsed].value(''/M[28]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[28]'', ''varchar (100)'') end As [paymentBillingState],
     case when [NameParsed].value(''/M[29]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[29]'', ''varchar (100)'') end As [paymentBillingCountryCode],
	 case when [NameParsed].value(''/M[30]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[30]'', ''varchar (100)'') end As [paymentBillingName],
     case when [NameParsed].value(''/M[31]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[31]'', ''varchar (100)'') end As [shippingAddress],
     case when [NameParsed].value(''/M[32]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[32]'', ''varchar (100)'') end As [shippingPostalCode],
	 case when [NameParsed].value(''/M[33]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[33]'', ''varchar (100)'') end As [shippingCity],
	 case when [NameParsed].value(''/M[34]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[34]'', ''varchar (100)'') end As [shippingState],
     case when [NameParsed].value(''/M[35]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[35]'', ''varchar (100)'') end As [shippingCountry],
     case when [NameParsed].value(''/M[36]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[36]'', ''varchar (100)'') end As [cvvVerifyResult],
	 case when [NameParsed].value(''/M[37]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[37]'', ''varchar (100)'') end As [responseCode],
     case when [NameParsed].value(''/M[38]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[38]'', ''varchar (100)'') end As [digitalItemCount],
     case when [NameParsed].value(''/M[39]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[39]'', ''varchar (100)'') end As [physicalItemCount],
	 case when [NameParsed].value(''/M[40]'', ''varchar (100)'')=''NULL'' then NULL else [NameParsed].value(''/M[40]'', ''varchar (100)'') end As [purchaseProductType]
into Parsed_String  
FROM tmp'
exec sp_executesql @parsequery
end