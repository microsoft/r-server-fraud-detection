##########################################################################################################################################
## This R script will do the following :
## 1. Hash the tagged data by accountID.
## 2. Split the tagged data set into a Training and a Testing set. 


## Input : Tagged data set.
## Output: Training and Testing sets.

##########################################################################################################################################

##############################################################################################################################
## The block below will hash accountID and split data into training and testing
##############################################################################################################################

## Hash accountID
print("Create HashID table by hash accountID...")

Drop_HashID_query <- "
hive -e \"drop table if exists HashID\"
"
Hashing_query <-"
hive -e \"create table HashID as
select accountID, abs(hash(accountID)%100) as hashCode from Tagged\"
"
system(Drop_HashID_query)
system(Hashing_query)

## Split into training and testing
print("Split Tagged data into training and testing based on hashCode...")

Drop_TaggedTraining_query <- "
hive -e \"drop table if exists TaggedTraining\" 
"
Get_TaggedTraining_query <- "
hive -e \"CREATE TABLE TaggedTraining AS
SELECT label, accountID, transactionID, transactionDateTime, isProxyIP, paymentInstrumentType, cardType, paymentBillingAddress,
paymentBillingPostalCode, paymentBillingCountryCode, paymentBillingName, accountAddress, accountPostalCode,  
accountCountry, accountOwnerName, shippingAddress, transactionCurrencyCode,localHour, ipState, ipPostCode,
ipCountryCode, browserLanguage, paymentBillingState, accountState, transactionAmountUSD, digitalItemCount, 
physicalItemCount, accountAge, paymentInstrumentAgeInAccount, numPaymentRejects1dPerUser, isUserRegistered,
transactionDate, transactionTime
FROM Tagged 
WHERE accountID IN (SELECT accountID from HashID WHERE hashCode <= 70)
AND label != 2
AND accountID IS NOT NULL
AND transactionID IS NOT NULL 
AND transactionDateTime IS NOT NULL 
AND transactionAmountUSD >= 0\"
"
system(Drop_TaggedTraining_query)
system(Get_TaggedTraining_query)

Drop_TaggedTesting_query <- "
hive -e \"drop table if exists TaggedTesting\" 
"
Get_TaggedTesting_query <- "
hive -e \"CREATE TABLE TaggedTesting AS
SELECT label, accountID, transactionID, transactionDateTime, isProxyIP, paymentInstrumentType, cardType, paymentBillingAddress,
paymentBillingPostalCode, paymentBillingCountryCode, paymentBillingName, accountAddress, accountPostalCode,  
accountCountry, accountOwnerName, shippingAddress, transactionCurrencyCode,localHour, ipState, ipPostCode,
ipCountryCode, browserLanguage, paymentBillingState, accountState, transactionAmountUSD, digitalItemCount, 
physicalItemCount, accountAge, paymentInstrumentAgeInAccount, numPaymentRejects1dPerUser, isUserRegistered,
transactionDate, transactionTime
FROM Tagged 
WHERE accountID IN (SELECT accountID from HashID WHERE hashCode > 70)
AND label != 2
AND accountID IS NOT NULL
AND transactionID IS NOT NULL 
AND transactionDateTime IS NOT NULL 
AND transactionAmountUSD >= 0\"
"
system(Drop_TaggedTesting_query)
system(Get_TaggedTesting_query)

print("Splitting finished!")

