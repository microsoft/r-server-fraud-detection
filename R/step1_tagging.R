##########################################################################################################################################
## This R script will do the following:
## 1. Upload the 3 data sets Untagged_Transactions, Account_Info and Fraud_Transactions from disk to SQL Server.
## 2. Create the transactionDateTime variable based on transactionDate and transactionTime.
## 3. Merge the two tables Untagged_Transaction ad Account_Info.
## 4. Remove duplicates from the 2 tables. 
## 5. Merge the 2 tables and create the label. 

## Input : 3 Data Tables: Untagged_Transactions, Account_Info and Fraud_Transactions.
## Output: Tagged data.

##########################################################################################################################################


# Set the compute context to local to upload data to SQL. 
rxSetComputeContext('local')

##############################################################################################################################
## The block below will do the following:
## 1. Specify the column types of the input data sets
## 2. Upload the data sets to SQL Server with rxDataStep.
## 3. Create transactionDateTime based on transactionDate and transactionTime. 
##############################################################################################################################

print("Uploading the 3 data sets to SQL Server...")

# Specify the desired column types. 
# Character and Factor are converted to varchar(255) in SQL Server. 
column_types_untagged <- c(transactionID = "character",
                           accountID = "character",
                           transactionAmountUSD = "character",
                           transactionAmount = "character",
                           transactionCurrencyCode = "character",
                           transactionCurrencyConversionRate = "character",
                           transactionDate = "character",
                           transactionTime = "character",
                           localHour = "character",
                           transactionScenario = "character",
                           transactionType = "character",
                           transactionMethod = "character",
                           transactionDeviceType = "character",
                           transactionDeviceId = "character",
                           transactionIPaddress = "character",
                           ipState = "character",
                           ipPostcode = "character",
                           ipCountryCode = "character",
                           isProxyIP = "character",
                           browserType = "character",
                           browserLanguage = "character",
                           paymentInstrumentType = "character",
                           cardType = "character",
                           cardNumberInputMethod = "character",
                           paymentInstrumentID = "character",
                           paymentBillingAddress = "character",
                           paymentBillingPostalCode = "character",
                           paymentBillingState = "character",
                           paymentBillingCountryCode = "character",
                           paymentBillingName = "character",
                           shippingAddress = "character",
                           shippingPostalCode = "character",
                           shippingCity = "character",
                           shippingState = "character",
                           shippingCountry = "character",
                           cvvVerifyResult = "character",
                           responseCode = "character",
                           digitalItemCount = "character",
                           physicalItemCount = "character",
                           purchaseProductType = "character")

column_types_account <- c(accountID = "character",
                          transactionDate = "character",
                          transactionTime = "character",  
                          accountOwnerName = "character",
                          accountAddress = "character",
                          accountPostalCode = "character",
                          accountCity = "character",
                          accountState = "character",
                          accountCountry = "character",
                          accountOpenDate = "character",
                          accountAge = "character",
                          isUserRegistered = "character",
                          paymentInstrumentAgeInAccount = "character",
                          numPaymentRejects1dPerUser = "character")

column_types_fraud <- c(transactionID = "character",
                        accountID = "character",
                        transactionAmount = "character",
                        transactionCurrencyCode = "character",
                        transactionDate = "character", 
                        transactionTime = "character",
                        localHour = "character",
                        transactionDeviceId = "character",
                        transactionIPaddress = "character")

# Point to the input data sets while specifying the classes.
Untagged_Transactions_text <- RxTextData(file = Untagged_Transactions, colClasses = column_types_untagged)
Account_Info_text <- RxTextData(file = Account_Info, colClasses = column_types_account)
Fraud_Transactions_text <- RxTextData(file = Fraud_Transactions, colClasses = column_types_fraud)

# Upload the data to SQL tables. 
## At the same time, we create transactionDateTime and recordDateTime. This is done by:
## converting transactionTime into a 6 digit time.
## concatenating transactionDate and transactionTime.
## converting it to a DateTime "%Y%m%d %H%M%S" format. 

Untagged_Transactions_sql <- RxSqlServerData(table = "Untagged_Transactions", connectionString = connection_string)
Account_Info_sql <- RxSqlServerData(table = "Account_Info", connectionString = connection_string)
Fraud_Transactions_sql <- RxSqlServerData(table = "Fraud_Transactions", connectionString = connection_string)

rxDataStep(inData = Untagged_Transactions_text, outFile = Untagged_Transactions_sql, overwrite = TRUE, 
           transforms = list(
             transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
           ))

rxDataStep(inData = Account_Info_text, outFile = Account_Info_sql, overwrite = TRUE, 
           transforms = list(
             recordDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
           ))

rxDataStep(inData = Fraud_Transactions_text, outFile = Fraud_Transactions_sql, overwrite = TRUE, 
           transforms = list(
             transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
           ))

# Set the compute context to SQL. 
rxSetComputeContext(sql)

#############################################################################################################################################
## The block below will 
## 1. Convert transactionDateTime to a dateTime format in SQL Server for faster execution of the next queries.
## 2. Sort the table Account_Info into Account_Info_Sort in descendent order of accountID, transactionDateTime. 
## 3. Merge the two tables Untagged_Transactions and Account_Info_Sort. 
############################################################################################################################################

print("Merging Untagged_Transactions and Account_Info into Untagged_Transactions_Account...")

# Convert transactionDateTime to a datetime format in SQL Server. 
print("Converting transactionDateTime to a datetime format in SQL Server ...")

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("ALTER TABLE Untagged_Transactions ALTER COLUMN transactionDateTime datetime;"
                                              , sep=""))

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("ALTER TABLE Account_Info ALTER COLUMN recordDateTime datetime;"
                                              , sep=""))

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("ALTER TABLE Fraud_Transactions ALTER COLUMN transactionDateTime datetime;"
                                              , sep=""))


# Sort Account_Info in ascending order of accountID, and descending order of transactionDateTime. 
# Note: SQL queries are used here because the rxSort function is not available for SQL data sources.
print("Sorting the Account_Info table ...")

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("DROP TABLE if exists Account_Info_Sort;"
                                              , sep=""))

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("SELECT * INTO Account_Info_Sort FROM Account_Info
                                              ORDER BY accountID, recordDateTime desc;"
                                              , sep=""))

# Inner join of the 2 tables Untagged_Transactions and Account_Info_Sort.
# Note: SQL queries are used here because the rxMerge function is not available for SQL data sources.
## the top 1 is the maximum recordDateTime up to current transactionDateTime.

print("Merging the 2 tables Untagged_Transacations and Account_Info_Sort ...")

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste("DROP TABLE if exists Untagged_Transactions_Account;"
                                              , sep=""))

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste(
  "SELECT t1.*, t2.accountOwnerName, t2.accountAddress, t2.accountPostalCode, t2.accountCity, t2.accountState,
  t2.accountCountry, t2.accountOpenDate, t2.accountAge, t2.isUserRegistered, 
  t2.paymentInstrumentAgeInAccount, t2.numPaymentRejects1dPerUser
  INTO Untagged_Transactions_Account
  FROM 
  (SELECT * FROM Untagged_Transactions) AS t1
  OUTER APPLY
  (SELECT top 1 * FROM Account_Info_Sort AS t WHERE t.accountID = t1.accountID and t.recordDateTime <= t1.transactionDateTime) AS t2
  WHERE t1.accountID = t2.accountID;"
  , sep=""))


############################################################################################################################################
## The block below will remove duplicates from Untagged_Transactions_Account and Fraud_Transactions. 
############################################################################################################################################

print("Removing duplicates from Untagged_Transactions_Account and Fraud_Transactions...")

# We remove duplicates based on keys: transactionID, accountID, transactionDateTime, transactionAmount.
## Sometimes an entire transaction might be divided into multiple sub-transactions, so we can have the same IDs and Time but different amounts. 
## Note that it will be done with SQL queries and not with rx functions because evaluating if a row is a duplicate would not be possible 
## if the data is loaded chunk by chunk.

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste(
  "WITH cte_1
  AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDateTime, transactionAmount ORDER BY transactionID ASC) RN 
  FROM Untagged_Transactions_Account)
  DELETE FROM cte_1
  WHERE  RN > 1;"
  , sep=""))

rxExecuteSQLDDL(outOdbcDS, sSQLString = paste(
  "WITH cte_2
  AS (SELECT ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDateTime, transactionAmount ORDER BY transactionID ASC) RN 
  FROM Fraud_Transactions)
  DELETE FROM cte_2
  WHERE  RN > 1;"
  , sep=""))

############################################################################################################################################
## The block below will generate the tagged data as follows:
## 1. Aggregate the Fraud table on the account level, creating a start and end datetime. 
## 2. Join this data with the Untagged_Transactions_Account data with a left join. Start and end time are the NULL for non fraud.
## 3. Tag the data: 0 for non fraud, 1 for fraud, 2 for pre-fraud. 
############################################################################################################################################

print("Tagging the transactions...")

# We aggregate the Fraud table on the account level and create start and end date time.
# We then perform a left join with the Untagged_Transactions_Account table. 
# This gives us a table with all the previous data, in addition to the date time of the 1st and last transactions for the accounts 
# that were in the fraud table. 

Untagged_Fraud_Account_sql <- RxSqlServerData(sqlQuery = 
                                                "SELECT t1.*, t2.startDateNTime, t2.endDateNTime
                                                FROM Untagged_Transactions_Account AS t1
                                                LEFT JOIN
                                                (SELECT accountID, min(transactionDateTime) as startDateNTime, max(transactionDateTime) as endDateNTime 
                                                FROM Fraud_Transactions
                                                GROUP BY accountID) AS t2
                                                ON t1.accountID = t2.accountID",
                                              connectionString = connection_string)

# Output table pointer.
Tagged_sql <- RxSqlServerData(table = "Tagged", connectionString = connection_string)

# We create the label variable as follows:
## if accountID can't be found in the fraud dataset, tag it as 0: not fraudulent.
## if accountID is found in the fraud dataset and transactionDateTime is within the fraud time range, tag it as 1: fraud.
## if accountID is found in the fraud dataset but transactionDateTime is out of the fraud time range, tag it as 2: pre-fraud.

rxDataStep(inData = Untagged_Fraud_Account_sql, 
           outFile = Tagged_sql,
           overwrite = TRUE,
           rowsPerRead = 200000,
           transforms = list(
             label = ifelse(is.na(startDateNTime), 0, 
                            ifelse(transactionDateTime >= startDateNTime & transactionDateTime <= endDateNTime, 1, 2))
              ))



