##########################################################################################################################################
## This R script will do the following :
## 1. Split the tagged data set into a Training and a Testing set. 
## 2. Clean the training set and perform some preprocessing.  

## Input : Tagged data set.
## Output: Training and Testing sets, and cleaned Training set Tagged_Training_Processed.   

##########################################################################################################################################

# Set the compute context to SQL. 
rxSetComputeContext(sql)

#############################################################################################################################################
## The block below will split the Tagged data set into a Training and a Testing set.
############################################################################################################################################

print("Randomly splitting into a training and a testing set...")

# Create the Hash_Id table containing accountID hashed to 100 bins. 
# The advantage of using a hashing function for splitting is to:
# - ensure that the same accountID ends up in the same split.
# - permit repeatability of the experiment.  
rxExecuteSQLDDL(outOdbcDS, sSQLString = "DROP TABLE if exists Hash_Id;")

rxExecuteSQLDDL(outOdbcDS, sSQLString = 
  "SELECT accountID, ABS(CAST(CAST(HashBytes('MD5', accountID) AS VARBINARY(64)) AS BIGINT) % 100) AS hashCode  
  INTO Hash_Id
  FROM Tagged ;")

# Point to the training set. 
# At the same time, we remove:
# - variables not used in the next steps (intermediate variables, variables not needed for the training, variables with only missing values). 
# - observations with labels equal to 2 (pre-fraud).
# - observations where accountID, transactionID and transactionDateTime are missing. 
# - observations where the transaction amount in USD is negative. 

query_training <- "SELECT label, accountID, transactionID, transactionDateTime, isProxyIP, paymentInstrumentType, cardType, paymentBillingAddress,
                          paymentBillingPostalCode, paymentBillingCountryCode, paymentBillingName, accountAddress, accountPostalCode,  
                          accountCountry, accountOwnerName, shippingAddress, transactionCurrencyCode,localHour, ipState, ipPostCode,
                          ipCountryCode, browserLanguage, paymentBillingState, accountState, transactionAmountUSD, digitalItemCount, 
                          physicalItemCount, accountAge, paymentInstrumentAgeInAccount, numPaymentRejects1dPerUser, isUserRegistered,
                          transactionDate, transactionTime
                   FROM Tagged 
                   WHERE accountID IN (SELECT accountID from Hash_Id WHERE hashCode <= 70)
                   AND label != 2
                   AND accountID IS NOT NULL
                   AND transactionID IS NOT NULL 
                   AND transactionDateTime IS NOT NULL 
                   AND cast(transactionAmountUSD as float) >= 0"

Tagged_Training_sql <- RxSqlServerData(sqlQuery = query_training, connectionString = connection_string)

############################################################################################################################################
## The block below will clean the Tagged data. 
############################################################################################################################################

print("Cleaning and preprocessing the training set...")

clean_preprocess <- function(input_data_query, output_sql_name){
  
  # Detect variables with missing values. 
  # No missing values in accountID, transactionID and transactionDateTime since we already filtered out missing values in the query above. 
  # For rxSummary to give correct info on characters, stringsAsFactors = TRUE should be used in the pointer to the SQL Tagged_Training table.
  Tagged_Data_sql_stringsfactors <- RxSqlServerData(sqlQuery = input_data_query, connectionString = connection_string, stringsAsFactors = TRUE)
  var <- rxGetVarNames(Tagged_Data_sql_stringsfactors)
  formula <- as.formula(paste("~", paste(var, collapse = "+")))
  summary <- rxSummary(formula, Tagged_Data_sql_stringsfactors, byTerm = TRUE)
  variables_NA <- summary$sDataFrame[summary$sDataFrame$MissingObs > 0, 1]
  variables_NA <- variables_NA[!variables_NA %in% c("accountID", "transactionID", "transactionDateTime", "transactionDate", "transactionTime")]
  
  # If no missing values, we will only preprocess the data. Otherwise, we clean and preprocess. 
  if(length(variables_NA) == 0){
    print("No missing values: only preprocessing will be performed.")
  } else{ 
    print("Variables containing missing values are:")
    print(variables_NA)
  }
  
  # Function to replace missing values with 0. It will be wrapped into rxDataStep. 
  preprocessing <- function(data) {
    data <- data.frame(data, stringsAsFactors = FALSE)
    
    # Replace missing values with 0 except for localHour with -99. 
    if(length(var_with_NA) > 0){
      for(i in 1:length(var_with_NA)){
        row_na <- which(is.na(data[, var_with_NA[i]])) 
        if(var_with_NA[i] == c("localHour")){
          data[row_na, var_with_NA[i]] <- "-99"
        } else{
          data[row_na, var_with_NA[i]] <- "0"
        }
      }
    }
    
    # Fix some data entries in isUserRegistered, which should be binary.  
    row_na <- which(data[, c("isUserRegistered")] %in% as.character(seq(1, 9)))
    data[row_na, c("isUserRegistered")] <- "0"
    
    # Convert a few variables to numeric, replacing non-numeric entries with 0. a few other variables to fix some data entries.  
    numeric_to_fix <- c("accountAge", "paymentInstrumentAgeInAccount", "numPaymentRejects1dPerUser", "transactionAmountUSD",
                        "digitalItemCount", "physicalItemCount")
    for(i in 1:length(numeric_to_fix)){
      data[, numeric_to_fix[i]] <- as.numeric(data[, numeric_to_fix[i]])
      row_na <- which(is.na(as.numeric(data[, numeric_to_fix[i]])))
      data[row_na, numeric_to_fix[i]] <- 0
    }
    return(data)  
  }
  
  # Input and Output pointers. 
  Input_sql <- RxSqlServerData(sqlQuery = input_data_query, connectionString = connection_string)
  Output_sql <- RxSqlServerData(table =  output_sql_name, connectionString = connection_string)
  
  # We drop the output if it already exists as a view in case the SQL SP was executed in the same database before. 
  rxExecuteSQLDDL(outOdbcDS, sSQLString = sprintf("IF OBJECT_ID ('%s', 'V') IS NOT NULL DROP VIEW %s ;", 
                                                  output_sql_name, output_sql_name))
  
  # Perform the data cleaning with rxDataStep. 
  ## To preserve the type of transactionDateTime, we recreate it.
  rxDataStep(inData = Input_sql, 
             outFile = Output_sql, 
             overwrite = TRUE, 
             rowsPerRead = 200000,
             transformFunc = preprocessing,
             transformObjects = list(var_with_NA = variables_NA),
             transforms = list(
               transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
             ))

}

# Apply the preprocessing and cleaning to the training set. 
clean_preprocess(input_data_query = query_training, 
                 output_sql_name = "Tagged_Training_Processed")

