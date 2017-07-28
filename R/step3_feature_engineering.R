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

# Set the compute context to sql. 
rxSetComputeContext(sql)

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
create_risk_table <- function(variable_name, data, smooth1, smooth2){
  
  # Compute the number of frauds and non-frauds for each level of the variable. 
  formula <- as.formula(paste(" ~ F(label) :", variable_name))
  Counts_table <- rxCrossTabs(formula = formula, data = data)$counts[[1]]
  Counts <- data.frame(x = colnames(Counts_table), fraudCount = Counts_table["1", ], nonFraudCount = Counts_table["0",], row.names = NULL)
  colnames(Counts)[1] <- variable_name
  
  # Compute the smoothed fraud rate of each level of the variable. 
  Odds <- (Counts$fraudCount + smooth1)/(Counts$nonFraudCount + Counts$fraudCount + smooth2)
  # Compute the log of the smoothed odds ratio.
  Risk <- log(Odds/(1-Odds))
  
  # Create the Risk table.
  Risk_df <- as.data.frame(cbind(as.character(Counts[, variable_name]), Risk))
  colnames(Risk_df) <- c(variable_name, "risk")
  
  # Export it to SQL: Output table pointer for the Risk Table of the specific variable. 
  rxSetComputeContext('local')
  table_name <- paste("Risk_", toupper(substring(variable_name, 1, 1)), substring(variable_name, 2), sep = "", collapse = " ")
  Risk_sql <- RxSqlServerData(table = table_name, connectionString = connection_string)
  rxDataStep(inData = Risk_df, outFile = Risk_sql, overwrite = TRUE)
  
  # Set back the compute context to sql.
  rxSetComputeContext(sql)
  
}

# Variables for which we create Risk Tables. 
risk_vars <- c("transactionCurrencyCode", "localHour", "ipState", "ipPostCode","ipCountryCode", "browserLanguage",
               "accountPostalCode", "accountState", "accountCountry", "paymentBillingPostalCode", "paymentBillingState",
               "paymentBillingCountryCode")

# Pointer to the preprocessed training set with stringsAsFactors = TRUE for correct summary computations. 
Train_sqlstringsfactors <- RxSqlServerData(table = "Tagged_Training_Processed", connectionString = connection_string, stringsAsFactors = TRUE)

# We apply create_risk_table sequentially over the variables in risk_vars. 
for(variable_name in risk_vars){
  create_risk_table(variable_name = variable_name, data = Train_sqlstringsfactors, smooth1 = 10, smooth2 = 100)
}


############################################################################################################################################
## The block below will perform feature engineering on the whole data set, by: 
## 1. Assigning the Risk values to the character variables. 
## 2. Creating isHighAmount: flag for transactions involving high amounts. 
## 3. Creating various flags showing if there is a mismatch in the addresses variables.
############################################################################################################################################

print("Assigning risk values to the variables, and creating address mismatch and high amount flags...")

assign_risk_and_flags <- function(input_sql_name, output_sql_name){
  
  # Function to assign the risk values. It will be wrapped into rxDataStep. 
  assign_risk <- function(data) {
    data <- data.frame(data, stringsAsFactors = FALSE)
    
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
  
  # We drop the output if it already exists as a view in case the SQL SP was executed in the same database before. 
  rxExecuteSQLDDL(outOdbcDS, sSQLString = sprintf("IF OBJECT_ID ('%s', 'V') IS NOT NULL DROP VIEW %s ;", 
                                                  output_sql_name, output_sql_name))
  
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
}

# Apply the assign_risk_and_flags function. 
assign_risk_and_flags(input_sql_name = "Tagged_Training_Processed",
                      output_sql_name = "Tagged_Training_Processed_Features1")


############################################################################################################################################
## The block below will create aggregates.
## They correspond to the number and amount of transactions in the past day and 30 days for every transaction per accountID.
## It is done by: 
## 1. Load Tagged_Training_Processed_Features1 in memory. 
## 2. Split the data set into a list of data frames for each accountID. 
## 3. Compute aggregates for each accountID with the function aggregates_account_level.
## 4. Combine the results, use current values when no aggregates, and write the result back to SQL Server. 
############################################################################################################################################

print("Computing the number of transactions and their amounts in the past day and 30 days for every transaction per accountID...")

compute_aggregates <- function(input_sql_name, output_sql_name){
  
  # We drop the output if it already exists as a view in case the SQL SP was executed in the same database before. 
  rxExecuteSQLDDL(outOdbcDS, sSQLString = sprintf("IF OBJECT_ID ('%s', 'V') IS NOT NULL DROP VIEW %s ;", 
                                                  output_sql_name, output_sql_name))
  
  # Import the data set and fix the datetime type. 
  rxSetComputeContext('local')
  
  Input_sql <- RxSqlServerData(table = input_sql_name, connectionString = connection_string)
  data = rxImport(Input_sql)
  data$transactionDateTime <- as.POSIXct(paste(data$transactionDate, sprintf("%06d", as.numeric(data$transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")
  
  # Function that computes the aggregates for a given accountID. 
  
  aggregates_account_level <- function(dt){
    if(nrow(dt) == 1){ #if there is only 1 transaction in that account, no aggregation. 
      return(NULL)
      
    } else{ 
      # Perform a cross-apply and filter: for each transactionID, z has data about the other transactionID that occured in the past 30 days.
      z = merge(x = dt, y = dt[, c("transactionID", "transactionDateTime", "transactionAmountUSD")], by = NULL)
      z = z[z$transactionID.x != z$transactionID.y & difftime(z$transactionDateTime.x , z$transactionDateTime.y, units = "days")  > 0 & difftime(z$transactionDateTime.x , z$transactionDateTime.y, units = "days") < 30,]
      
      # Keep the transactionIDs that occurred in the past 1 day and 30 days respectively. 
      z1day = z[difftime(z$transactionDateTime.x , z$transactionDateTime.y, units = "days") <= 1, ]
      z30day = z[difftime(z$transactionDateTime.x , z$transactionDateTime.y, units = "days") <= 30, ]
      
      # Compute the number of rows (sumPurchaseCount1dPerUser) and the total amount spent in the past day (sumPurchaseAmount1dPerUser). 
      if(nrow(z30day) == 0){
        return(NULL)
      } else{
        aggsum30day <- aggregate(z30day$transactionAmountUSD.y, by = list(z30day$transactionID.x), FUN = sum)
        colnames(aggsum30day) <- c("transactionID", "sumPurchaseAmount30dPerUser")
        aggcount30day <- aggregate(z30day$transactionAmountUSD.y, by = list(z30day$transactionID.x), FUN = NROW)
        colnames(aggcount30day) <- c("transactionID", "sumPurchaseCount30dPerUser")
        agg30day <- merge(x = aggsum30day, y = aggcount30day  , by = "transactionID")
      }
      
      # Compute the number of rows (sumPurchaseCount30dPerUser) and the total amount spent in the past 30 days (sumPurchaseAmount30dPerUser). 
      if(nrow(z1day) == 0){
        agg30day$sumPurchaseAmount1dPerUser <- 0
        agg30day$sumPurchaseCount1dPerUser <- 0
        return(agg30day)
      } else{
        aggsum1day <- aggregate(z1day$transactionAmountUSD.y, by = list(z1day$transactionID.x), FUN = sum)
        colnames(aggsum1day) <- c("transactionID", "sumPurchaseAmount1dPerUser")
        aggcount1day <- aggregate(z1day$transactionAmountUSD.y, by = list(z1day$transactionID.x), FUN = NROW)
        colnames(aggcount1day) <- c("transactionID", "sumPurchaseCount1dPerUser")
        agg1day <- merge(x = aggsum1day, y = aggcount1day  , by = "transactionID")
      }
      
      # Return the 4 new variables for each transactionID that had other transactions in the past 30 days. 
      agg <- merge(x = agg1day, y = agg30day  , by = "transactionID", all = TRUE)
      return(agg)
    }
  }
  
  # Split the data set by accountID. 
  Splits <- split(data, f = data$accountID)
  
  # Compute the aggregations for each accountID with the user defined function aggregates_account_level. 
  Aggregations_list <- lapply(X = Splits, FUN = aggregates_account_level)
  
  # Bind the results into 1 data frame. 
  Aggregations_df <- do.call("rbind", Aggregations_list)
  
  # Add the new variables to the initial data with a left outer join.  
  Output_df <- merge(x = data, y = Aggregations_df, by = "transactionID", all.x = TRUE)
  
  # The transactions that had no other transactions in the 30 day time frame have missing values. We convert them to 0.
  for(new_name in c("sumPurchaseCount1dPerUser", "sumPurchaseCount30dPerUser", "sumPurchaseAmount1dPerUser", "sumPurchaseAmount30dPerUser")){
    row_na <- which(is.na(Output_df[, new_name])) 
    Output_df[row_na, new_name] <- 0
  }

  # Write the result back to SQL. 
  Output_sql <- RxSqlServerData(table = output_sql_name, connectionString = connection_string)
  
  rxDataStep(inData = Output_df,
             outFile = Output_sql,
             overwrite = TRUE)
  
  # Set the compute context back to sql. 
  rxSetComputeContext(sql)
  
  # Convert the label to character format in SQL Server. 
  query <- sprintf( "ALTER TABLE %s ALTER COLUMN label char(1);", output_sql_name)
  rxExecuteSQLDDL(outOdbcDS, sSQLString = query)
  
}

# Apply the function. 
input_sql_name <- "Tagged_Training_Processed_Features1"
output_sql_name <- "Tagged_Training_Processed_Features"

compute_aggregates(input_sql_name, 
                   output_sql_name)


############################################################################################################################################
## The block below will get the column information for training and testing. 
############################################################################################################################################

print("Getting and saving the variables information...")

# Point to the training set with new features with stringsAsFactors = TRUE to get the column information. 
Tagged_Training_Processed_Features_sql <- RxSqlServerData(table = "Tagged_Training_Processed_Features", 
                                                          connectionString = connection_string,
                                                          stringsAsFactors = TRUE)

# Save the column information.  
column_info <- rxCreateColInfo(Tagged_Training_Processed_Features_sql, sortLevels = TRUE)

column_info$accountID <- NULL
column_info$transactionDate <- NULL
column_info$transactionTime <- NULL
column_info$transactionDateTime <- NULL

# Create a pointer to the training set, ready for training. 
Tagged_Training_Processed_Features_sql <- RxSqlServerData(table = "Tagged_Training_Processed_Features", 
                                                          connectionString = connection_string,
                                                          colInfo = column_info)