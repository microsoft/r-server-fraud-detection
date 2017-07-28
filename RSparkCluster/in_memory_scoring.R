##########################################################################################################################################
## This R script will perform in-memory scoring for batch scoring or for scoring remotely with a web service. 
##########################################################################################################################################
# Inputs of the function: 
## Untagged_Transactions_df: data frame with the untagged transactions to be scored.
## Account_Info_df: data frame with the account information for the untagged transactions. 
## Stage: "Prod" for batch scoring, or "Web" for scoring remotely with web service.

in_memory_scoring <- function(Untagged_Transactions_df, 
                              Account_Info_df,
                              Stage)
{
  # Load library. 
  library(RevoScaleR)
  library(MicrosoftML)
  
  # Set the compute context to local. 
  rxSetComputeContext('local')
  
  # Create transactionDateTime or recordDateTime. This is done by:
  ## converting transactionTime into a 6 digit time.
  ## concatenating transactionDate and transactionTime.
  ## converting it to a DateTime "%Y%m%d %H%M%S" format. 
  
  Untagged_Transactions_df$transactionDateTime = as.POSIXct(paste(Untagged_Transactions_df$transactionDate, sprintf("%06d", as.numeric(Untagged_Transactions_df$transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")
  Account_Info_df$recordDateTime = as.POSIXct(paste(Account_Info_df$transactionDate, sprintf("%06d", as.numeric(Account_Info_df$transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")
  
  # Load variables from Development Stage. 
  if(Stage == "Web"){
    Risk_list <- model_objects$Risk_list
    boosted_fit <- model_objects$boosted_fit
  }
  
  if(Stage == "Prod"){
    # Directory that holds the tables and model from the Development stage.
    LocalModelsDir <- file.path(LocalWorkDir, "model")
    
    Risk_list <- readRDS(file.path(LocalModelsDir, "Risk_list.rds"))
    boosted_fit <- readRDS(file.path(LocalModelsDir, "gbt_model.rds"))
  }
  
  ############################################################################################################################################
  ## The block below will do the following:
  ## 1. Merge the two tables Untagged_Transactions and Account_Info.
  ## 2. Remove duplicates from the table. 
  ############################################################################################################################################
  
  # Merge Untagged_Transactions and Account_Info.
  
  ## Merge the input tables on accountID.
  Untagged_Account_All_df = merge(x = Untagged_Transactions_df, y = Account_Info_df, by = "accountID", all.x = TRUE)
  
  ## Keep rows where recordDateTime <= transactionDateTime for every accountID, transactionID.  
  Untagged_Account_All_df = Untagged_Account_All_df[difftime(Untagged_Account_All_df$transactionDateTime, Untagged_Account_All_df$recordDateTime, units = "days")  >= 0 , ]
  
  ## Get the highest recordDateTime for every accountID, transactionID.
  Latest_Record_df <- aggregate(Untagged_Account_All_df$recordDateTime, by = list(Untagged_Account_All_df$accountID, Untagged_Account_All_df$transactionID), FUN = max)
  colnames(Latest_Record_df) <- c("accountID", "transactionID", "recordDateTime")
  
  ## Merge with the Untagged_Account. 
  Untagged_Latest_Record_df = merge(x = Untagged_Transactions_df, y = Latest_Record_df, by = c("accountID", "transactionID"), all.x = TRUE)
  
  ## Add the data from Account_Info. 
  Untagged_Account_df = merge(x = Untagged_Latest_Record_df, y = Account_Info_df, by = c("accountID", "recordDateTime"), all.x = TRUE)
  
  ## Remove some columns. 
  Untagged_Account_df$recordDateTime <- NULL
  Untagged_Account_df$transactionTime.y <- NULL
  Untagged_Account_df$transactionDate.y <- NULL
  
  ## Select specific columns and remove observations when an ID variable is missing or when the transaction amount is negative. 
  to_keep <- c("accountID", "transactionID", "transactionDateTime", "isProxyIP", "paymentInstrumentType", "cardType", "paymentBillingAddress",
               "paymentBillingPostalCode", "paymentBillingCountryCode", "paymentBillingName", "accountAddress", "accountPostalCode",  
               "accountCountry", "accountOwnerName", "shippingAddress", "transactionCurrencyCode","localHour", "ipState", "ipPostcode",
               "ipCountryCode", "browserLanguage", "paymentBillingState", "accountState", "transactionAmountUSD", "digitalItemCount", 
               "physicalItemCount", "accountAge", "paymentInstrumentAgeInAccount", "numPaymentRejects1dPerUser", "isUserRegistered",
               "transactionDate.x", "transactionTime.x")
  
  Untagged_Account_df <- Untagged_Account_df[!(is.na(Untagged_Account_df$accountID) | is.na(Untagged_Account_df$transactionID) | is.na(Untagged_Account_df$transactionDateTime) | Untagged_Account_df$transactionAmountUSD < 0) , to_keep] 
  
  # Remove duplicates. 
  Untagged_Account_df <- Untagged_Account_df[!duplicated(Untagged_Account_df[, c("transactionID", "accountID", "transactionDateTime", "transactionAmountUSD")]), ]
  
  ############################################################################################################################################
  ## The block below will clean the merged data set. 
  ############################################################################################################################################
  
  # For coherence with the development code that used Hive tables, we change all the variable names to lower case. 
  colnames(Untagged_Account_df) <- unlist(sapply(colnames(Untagged_Account_df), tolower))
  
  # Get the variable names with NA.
  no_of_NA <- sapply(Untagged_Account_df, function(x) sum(is.na(x)))
  var_with_NA <- names(no_of_NA[no_of_NA > 0])
  
  # Cleaning and preprocessing function. 
  preprocessing <- function(data) {
    # Replace missing values with 0 except for localHour with -99. 
    if(length(var_with_NA) > 0){
      for(i in 1:length(var_with_NA)){
        row_na <- which(is.na(data[, var_with_NA[i]]) == TRUE) 
        if(var_with_NA[i] == c("localhour")){
          data[row_na, var_with_NA[i]] <- "-99"
        } else{
          data[row_na, var_with_NA[i]] <- "0"
        }
      }
    }
    
    # Fix some data entries in isUserRegistered, which should be binary.  
    row_na <- which(data[, c("isuserregistered")] %in% as.character(seq(1, 9)))
    data[row_na, c("isuserregistered")] <- "0"
    
    # Convert a few variables to numeric, replacing non-numeric entries with 0. a few other variables to fix some data entries.  
    numeric_to_fix <- c("accountage", "paymentinstrumentageinaccount", "numpaymentrejects1dperuser", "transactionamountusd",
                        "digitalitemcount", "physicalitemcount")
    
    for(i in 1:length(numeric_to_fix)){
      data[, numeric_to_fix[i]] <- as.numeric(data[, numeric_to_fix[i]])
      row_na <- which(is.na(as.numeric(data[, numeric_to_fix[i]])) == TRUE)
      data[row_na, numeric_to_fix[i]] <- 0
    }
    return(data)  
  }
  
  # Apply the function. 
  Untagged_Account_Preprocessed_df <- preprocessing(Untagged_Account_df)
  
  
  ############################################################################################################################################
  ## The block below will add risk values and create mismatch address flags and a high amount flag. 
  ############################################################################################################################################
  
  # Variables to which we will add risk values. 
  risk_vars <- c("transactioncurrencycode", "localhour", "ipstate", "ippostcode","ipcountrycode", "browserlanguage",
                 "accountpostalcode", "accountstate", "accountcountry", "paymentbillingpostalcode", "paymentbillingstate",
                 "paymentbillingcountrycode")
  
  # Function to assign risks. 
  assign_risk <- function(data){
    
    for(name in  risk_vars){
      # Get the appropriate Risk Table from the Risk_list.
      Risk_df <- Risk_list[[name]]
      
      # Perform a left outer join with the Risk table. This will assign the risk value to every level of the variable. 
      data <- merge(data, Risk_df, by = name, all.x = TRUE)
      new_name <- paste(name, "risk", sep ="")
      colnames(data)[ncol(data)] <- new_name
      
      # If a new level was found in the data, the assigned risk is NULL. We convert it to 0. 
      row_na <- which(is.na(data[, new_name]) == TRUE) 
      data[row_na, new_name] <- 0
      
    }  
    return(data)  
  }
  
  # Apply the function. 
  Untagged_Account_Features1_df <- assign_risk(Untagged_Account_Preprocessed_df)
  
  # Create other variables:
  ## isHighAmount: flag for transactions of a high amount. 
  ## various flags showing if there is a mismatch in the addresses variables.
  Untagged_Account_Features1_df$ishighamount = ifelse(Untagged_Account_Features1_df$transactionamountusd > 150, "1", "0")
  Untagged_Account_Features1_df$acctbillingaddressmismatchflag = ifelse(Untagged_Account_Features1_df$paymentbillingaddress == Untagged_Account_Features1_df$accountaddress, "0", "1")
  Untagged_Account_Features1_df$acctbillingpostalcodemismatchflag = ifelse(Untagged_Account_Features1_df$paymentbillingpostalcode == Untagged_Account_Features1_df$accountpostalcode, "0", "1")
  Untagged_Account_Features1_df$acctbillingcountrymismatchflag = ifelse(Untagged_Account_Features1_df$paymentbillingcountrycode == Untagged_Account_Features1_df$accountcountry, "0", "1")
  Untagged_Account_Features1_df$acctbillingnamemismatchflag= ifelse(Untagged_Account_Features1_df$paymentbillingname == Untagged_Account_Features1_df$accountownername, "0", "1")
  Untagged_Account_Features1_df$acctshippingaddressmismatchflag = ifelse(Untagged_Account_Features1_df$shippingaddress == Untagged_Account_Features1_df$accountaddress, "0", "1")
  Untagged_Account_Features1_df$shippingBillingAddressmismatchflag = ifelse(Untagged_Account_Features1_df$shippingaddress == Untagged_Account_Features1_df$paymentbillingaddress, "0", "1")
  
  # Create an artificial target variable label. This is for rxPredict to work. 
  Untagged_Account_Features1_df$label <- sample(c("0", "1"), size = nrow(Untagged_Account_Features1_df), replace = T)
  
  ############################################################################################################################################
  ## The block below will compute the aggregates.
  ############################################################################################################################################
  
  # Function that computes the aggregates for a given accountID. 
  aggregates_account_level <- function(dt){
    if(nrow(dt) == 1){ #if there is only 1 transaction in that account, no aggregation. 
      return(NULL)
      
    } else{ 
      # Perform a cross-apply and filter: for each transactionID, z has data about the other transactionID that occured in the past 30 days.
      z = merge(x = dt, y = dt[, c("transactionid", "transactiondatetime", "transactionamountusd")], by = NULL)
      z = z[z$transactionid.x != z$transactionid.y & difftime(z$transactiondatetime.x , z$transactiondatetime.y, units = "days")  > 0 & difftime(z$transactiondatetime.x , z$transactiondatetime.y, units = "days") < 30,]
      
      # Keep the transactionIDs that occurred in the past 1 day and 30 days respectively. 
      z1day = z[difftime(z$transactiondatetime.x , z$transactiondatetime.y, units = "days") <= 1, ]
      z30day = z[difftime(z$transactiondatetime.x , z$transactiondatetime.y, units = "days") <= 30, ]
      
      # Compute the number of rows (sumPurchaseCount1dPerUser) and the total amount spent in the past day (sumPurchaseAmount1dPerUser). 
      if(nrow(z30day) == 0){
        return(NULL)
      } else{
        aggsum30day <- aggregate(z30day$transactionamountusd.y, by = list(z30day$transactionid.x), FUN = sum)
        colnames(aggsum30day) <- c("transactionid", "sumpurchaseamount30dperuser")
        aggcount30day <- aggregate(z30day$transactionamountusd.y, by = list(z30day$transactionid.x), FUN = NROW)
        colnames(aggcount30day) <- c("transactionid", "sumpurchasecount30dperuser")
        agg30day <- merge(x = aggsum30day, y = aggcount30day  , by = "transactionid")
      }
      
      # Compute the number of rows (sumPurchaseCount30dPerUser) and the total amount spent in the past 30 days (sumPurchaseAmount30dPerUser). 
      if(nrow(z1day) == 0){
        agg30day$sumpurchaseamount1dperuser <- 0
        agg30day$sumpurchasecount1dperuser <- 0
        return(agg30day)
      } else{
        aggsum1day <- aggregate(z1day$transactionamountusd.y, by = list(z1day$transactionid.x), FUN = sum)
        colnames(aggsum1day) <- c("transactionid", "sumpurchaseamount1dperuser")
        aggcount1day <- aggregate(z1day$transactionamountusd.y, by = list(z1day$transactionid.x), FUN = NROW)
        colnames(aggcount1day) <- c("transactionid", "sumpurchasecount1dperuser")
        agg1day <- merge(x = aggsum1day, y = aggcount1day  , by = "transactionid")
      }
      
      # Return the 4 new variables for each transactionID that had other transactions in the past 30 days. 
      agg <- merge(x = agg1day, y = agg30day, by = "transactionid", all = TRUE)
      return(agg)
    }
  }
  
  
  # Split the data set by accountID. 
  Splits <- split(Untagged_Account_Features1_df, f = Untagged_Account_Features1_df$accountid)
  
  # Compute the aggregations for each accountID with the user defined function aggregates_account_level. 
  Aggregations_list <- lapply(X = Splits, FUN = aggregates_account_level)
  
  # Bind the results into 1 data frame. 
  Aggregations_df <- do.call("rbind", Aggregations_list)
  
  # Add the new variables to the initial data:  
  
  ## If there was 1 transaction per account ID for all accounts, we simply add the 4 aggregate variables with values of 0.
  if(is.null(Aggregations_df)){
    Untagged_Account_Features_df <- Untagged_Account_Features1_df
    for(new_name in c("sumpurchasecount1dperuser", "sumpurchasecount30dperuser", "sumpurchaseamount1dperuser", "sumpurchaseamount30dperuser")){
      Untagged_Account_Features_df[, new_name] <- 0
    } 
    
  }else{
    ## Otherwise, add the new variables to the initial data with a left outer join.  
    Untagged_Account_Features_df <- merge(x = Untagged_Account_Features1_df, y = Aggregations_df, by = "transactionid", all.x = TRUE)
    # The transactions that had no other transactions in the 30 day time frame have missing values. We convert them to 0.
    for(new_name in c("sumpurchasecount1dperuser", "sumpurchasecount30dperuser", "sumpurchaseamount1dperuser", "sumpurchaseamount30dperuser")){
      row_na <- which(is.na(Untagged_Account_Features_df[, new_name]) == TRUE) 
      Untagged_Account_Features_df[row_na, new_name] <- 0
    }
  }
  
  ############################################################################################################################################
  ## The block below will convert character to factors for the prediction step.
  ############################################################################################################################################
  
  for (name in colnames(Untagged_Account_Features_df)){
    if(class(Untagged_Account_Features_df[[name]])[1] == "character" & ! name %in% c("accountid", "transactionid", "transactiondatetime", "transactiondate.x", "transactiontime.x", "paymentbillingaddress",
                                                                                     "paymentbillingname", "accountaddress", "accountownername", "shippingaddress", risk_vars)){
      Untagged_Account_Features_df[[name]] <- factor(Untagged_Account_Features_df[[name]])
    }
  }
  
  Untagged_Account_Features_df$isproxyip <- as.factor(as.character( Untagged_Account_Features_df$isproxyip))
  Untagged_Account_Features_df$isuserregistered <- as.factor(as.character( Untagged_Account_Features_df$isuserregistered))
  
  
  ############################################################################################################################################
  ## The block below will score the featurized data set.
  ############################################################################################################################################
  
  # Make predictions. 
  Predictions <- rxPredict(boosted_fit, 
                           data = Untagged_Account_Features_df, 
                           extraVarsToWrite = c("accountid", "transactionid", "transactiondate.x", "transactiontime.x", "transactionamountusd"))
  
  # Change the names of the variables in the predictions table for clarity.
  Predictions$transactiondatetime = as.character(as.POSIXct(paste(Predictions$transactiondate.x, sprintf("%06d", as.numeric(Predictions$transactiontime.x)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
  Predictions$transactiondate.x <- NULL
  Predictions$transactiontime.x <- NULL
  
  Predictions <- Predictions[, c(1, 2, 3, 7, 6)]
  colnames(Predictions)[5] <- c("score")
  
  return(Predictions)
  
}







