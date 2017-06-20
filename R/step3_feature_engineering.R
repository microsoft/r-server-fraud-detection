##########################################################################################################################################
## This R script will do the following :
## 1. Create the Risk tables. 
## 2. Assign risk values to the variables in the training set.
## 3. Create flags for mismatches between addresses, and flags for high amount transactions. 
## 4. Create aggregates corresponding to the number and amount of transactions in the past day and 30 days for every transaction per accountID.
## 5. Fix variable types for training. 

## Input : Cleaned training set Tagged_Training_Processed.
## Output:  Training set with new features Tagged_Training_Preprocessed_Features.

##########################################################################################################################################

# Set the compute context to local. 
rxSetComputeContext('local')

# Load the data.table package (install it if on your own machine). 
if(!require(data.table)){
  #install.packages("data.table")
  library(data.table)
}

# Input table pointer. 
Tagged_Training_Processed_sql <- RxSqlServerData(table = "Tagged_Training_Processed", connectionString = connection_string)

############################################################################################################################################
## The block below will create the Risk tables for most of the variables on the training set. This is done for each variable by computing: 
## 1. The number of frauds and non frauds per level. 
## 2. A smoothed odd fraud rate per level. 
## 3. The log of the smoothed odd ratio.
############################################################################################################################################

print("Creating the Risk Tables using the Training set...")

# Function that creates and uploads to SQL a Risk table for 1 variable. 
create_risk_table <- function(variable_name, data){
  
  # Convert the data frame to a data.table for the following computations. 
  data <- data.table(data)
  
  # Set the Smoothing parameters. 
  smooth1 <- 10
  smooth2 <- 100
  
  # Compute the number of frauds and non-frauds for each level of the variable, then merge them. 
  Fraud_Count <- data[, sum(label), by = variable_name]
  Non_Fraud_Count <- data[, sum(1 - label), by = variable_name]
  Counts <- merge(x = Fraud_Count, y = Non_Fraud_Count, by = variable_name, all = TRUE, sort = TRUE)
  Counts <- as.data.frame(Counts)
  
  # Compute the smoothed fraud rate of each level of the variable. 
  Odds <- (Counts['V1.x'] + smooth1)/(Counts['V1.y'] + Counts['V1.x'] + smooth2)
  # Compute the log of the smoothed odds ratio.
  Risk <- log(Odds/(1-Odds))
  
  # Create the Risk table.
  Risk_df <- cbind(Counts[, variable_name], Risk)
  colnames(Risk_df) <- c(variable_name, "risk")
  
  # Export it to SQL: Output table pointer for the Risk Table of the specific variable. 
  table_name <- paste("Risk_", toupper(substring(variable_name, 1, 1)), substring(variable_name, 2), sep = "", collapse = " ")
  Risk_sql <- RxSqlServerData(table = table_name, connectionString = connection_string)
  
  rxDataStep(inData = Risk_df, outFile = Risk_sql, overwrite = TRUE)
  
}

# Import the training set to be able to apply the create_risk_table function. 
Train_df <- rxImport(Tagged_Training_Processed_sql)

# Convert label to numeric. 
Train_df$label <- as.numeric(Train_df$label)

# Variables for which we create Risk Tables. 
risk_vars <- c("transactionCurrencyCode", "localHour", "ipState", "ipPostCode","ipCountryCode", "browserLanguage",
               "accountPostalCode", "accountState", "accountCountry", "paymentBillingPostalCode", "paymentBillingState",
               "paymentBillingCountryCode")

# We apply create_risk_table sequentially over the variables in risk_vars. 
rxSetComputeContext('local')
for(variable_name in risk_vars){
  create_risk_table(variable_name, Train_df)
}

# Set the compute context back to SQL.
rxSetComputeContext(sql)


############################################################################################################################################
## The block below will perform feature engineering on the whole data set, by: 
## 1. Assigning the Risk values to the character variables. 
## 2. Creating isHighAmount: flag for transactions involving high amounts. 
## 3. Creating various flags showing if there is a mismatch in the addresses variables.
## 4. Creating aggregates corresponding to the number and amount of transactions in the past day and 30 days for every transaction per accountID.
############################################################################################################################################

# no_of_rows is used to take a sample of the final data set. 
# This is used to downsample the majority class (see below).

feature_engineer <- function(input_sql_name, output_sql_name, no_of_rows){
  
  print("Assigning risk values to the variables, and creating address mismatch and high amount flags...")

# Function to assign the risk values. It will be wrapped into rxDataStep. 
assign_risk <- function(data) {
  data <- data.frame(data, stringsAsFactors = F)
  
  for(name in  risk_variables){
    # Import the Risk table from SQL Server. 
    table_name <- paste("Risk_", toupper(substring(name, 1, 1)), substring(name, 2), sep = "", collapse = " ")
    Risk_sql <- RxSqlServerData(table = table_name, connectionString = connection_string)
    Risk_df <- rxImport(Risk_sql)
    
    # Perform a left outer join with the Risk table. This will assign the risk value to every level of the variable. 
    data <- base::merge(data, Risk_df, by = name, all.x = TRUE)
    new_name <- paste(name, "Risk", sep ="")
    colnames(data)[ncol(data)] <- new_name
    
    # If a new level was found in the data, the assigned risk is NULL. We convert it to 0. 
    row_na <- which(is.na(data[, new_name]) == TRUE) 
    data[row_na, new_name] <- 0
    
  }  
  return(data)  
}


# Input and Output pointers. 
Input_sql <- RxSqlServerData(table = input_sql_name, connectionString = connection_string)
Output_sql <- RxSqlServerData(table = output_sql_name, connectionString = connection_string)

# Create buckets for various numeric variables with the function Bucketize. 
# At the same time, we create other variables:
## isHighAmount: flag for transactions of a high amount. 
## various flags showing if there is a mismatch in the addresses variables.
rxDataStep(inData = Input_sql,
           outFile = Output_sql, 
           overwrite = TRUE, 
           rowsPerRead = 200000,
           transformFunc = assign_risk,
           transformObjects =  list(risk_variables = risk_vars, connection_string = connection_string),
           transforms = list(
             isHighAmount = ifelse(transactionAmountUSD > 150, "1", "0"),
             acctBillingAddressMismatchFlag = ifelse(paymentBillingAddress == accountAddress, "0", "1"),
             acctBillingPostalCodeMismatchFlag = ifelse(paymentBillingPostalCode == accountPostalCode, "0", "1"),
             acctBillingCountryMismatchFlag = ifelse(paymentBillingCountryCode == accountCountry, "0", "1"),
             acctBillingNameMismatchFlag= ifelse(paymentBillingName == accountOwnerName, "0", "1"),
             acctShippingAddressMismatchFlag = ifelse(shippingAddress == accountAddress, "0", "1"),
             shippingBillingAddressMismatchFlag = ifelse(shippingAddress == paymentBillingAddress, "0", "1")
           ))


print("Computing the number of transactions and their amounts in the past day and 30 days for every transaction per accountID...")

# Enforce the conversion of transactionDateTime to datetime format in SQL Server. 
query <- sprintf( "ALTER TABLE %s ALTER COLUMN transactionDateTime datetime;", output_sql_name)
rxExecuteSQLDDL(outOdbcDS, sSQLString = query)

# Convert the label to character format in SQL Server. 
query <- sprintf( "ALTER TABLE %s ALTER COLUMN label char(1);", output_sql_name)
rxExecuteSQLDDL(outOdbcDS, sSQLString = query)


# Since this aggregation is time-consuming, we will only create a pointer to the query, and it will be materialized on the fly during the training.
# In order to deal with the class imbalance between fraud and non-fraud, we will down sample the majority class. 
# This is done by: 
## 1- Sorting the data by label in descending order, and accountID. 
## 2- Selecting the top 10K rows from that table. 
# In this way, we make sure to get all the fraud observations as well as a sample of the non-fraud observations. 

query_features <- sprintf("SELECT TOP(%s) * 
                           FROM %s as t
                           OUTER APPLY
                           (SELECT isnull(SUM(CASE WHEN t2.transactionDateTime > last24Hours THEN t2.transactionAmountUSD end), 0) AS sumPurchaseAmount1dPerUser,
                            COUNT(CASE WHEN t2.transactionDateTime > last24Hours THEN t2.transactionAmountUSD end) AS sumPurchaseCount1dPerUser,
                            isnull(SUM(t2.transactionAmountUSD), 0) AS sumPurchaseAmount30dPerUser,
                            COUNT(t2.transactionAmountUSD) AS sumPurchaseCount30dPerUser
                            FROM %s as t2
                            CROSS APPLY 
                            (values(t.transactionDateTime, DATEADD(hour, -24, t.transactionDateTime), DATEADD(day, -30, t.transactionDateTime)))
                             AS c(transactionDateTime, last24Hours, last30Days)
                            WHERE t2.accountID = t.accountID and t2.transactionDateTime < t.transactionDateTime and t2.transactionDateTime > last30Days
                            ) as a1
                            ORDER BY label desc, accountID",no_of_rows, output_sql_name, output_sql_name)

return(query_features)

}

# Apply the feature engineering, 
query_training_features <- feature_engineer(input_sql_name = "Tagged_Training_Processed",
                                            output_sql_name = "Tagged_Training_Processed_Features1",
                                            no_of_rows = 10000)

# Point to the training set with new features with stringsAsFactors = TRUE to get the column information. 
Tagged_Training_Processed_Features_sql <- RxSqlServerData(sqlQuery = query_training_features, 
                                                          connectionString = connection_string,
                                                          stringsAsFactors = TRUE)

# Save the column information.  
column_info <- rxCreateColInfo(Tagged_Training_Processed_Features_sql, sortLevels = T)

column_info$accountID <- NULL
column_info$transactionDate <- NULL
column_info$transactionTime <- NULL

# Create a pointer to the training set, ready for training. 
Tagged_Training_Processed_Features_sql <- RxSqlServerData(sqlQuery = query_training_features, 
                                                          connectionString = connection_string,
                                                          colInfo = column_info)