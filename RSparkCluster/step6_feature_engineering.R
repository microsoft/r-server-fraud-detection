##########################################################################################################################################
## This R script will create a function that does the following :
## 1. Assign risk values to the variables based on the Risk tables.
## 2. Create flags for mismatches between addresses, and flags for high amount transactions. 
## 3. Create aggregates corresponding to the number and amount of transactions in the past day and 30 days for every transaction per accountID.

## Input: 1. LocalWorkDir and HDFSWorkDir: working directories on HDFS and local edge node.
##        2. HiveTable: name of the Hive table containing the preprocessed data set to which new features will be added.
##        3. Stage: "Dev" for development, "Prod" for batch scoring, "Web" for web scoring. 

## Output: Preprocessed xdf file with new features and correct variable types. 

##########################################################################################################################################


feature_engineering <- function(LocalWorkDir,
                                HDFSWorkDir,
                                HiveTable,
                                Stage)
{
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  # Get the Risk tables. 
  if(Stage == "Dev" | Stage == "Prod"){
    # Define the directory where Risk tables will be loaded from. 
    LocalModelsDir <- file.path(LocalWorkDir, "model")
    
    # Import the Risk Tables list from LocalModelsDir
    Risk_list <- readRDS(file.path(LocalModelsDir, "Risk_list.rds"))
    
  }else{
    Risk_list <- model_objects$Risk_list
  }
  
  # Variables for which we create Risk Tables. 
  risk_vars <- c("transactioncurrencycode", "localhour", "ipstate", "ippostcode","ipcountrycode", "browserlanguage",
                 "accountpostalcode", "accountstate", "accountcountry", "paymentbillingpostalcode", "paymentbillingstate",
                 "paymentbillingcountrycode")
  
  ############################################################################################################################################
  ## The block below will perform feature engineering on the whole data set, by: 
  ## 1. Assigning the Risk values to the character variables. 
  ## 2. Creating ishighamount: flag for transactions involving high amounts. 
  ## 3. Creating various flags showing if there is a mismatch in the addresses variables.
  
  ## We also create the variable splitnumber whish hashes the accountID to a number (numSplits) of groups in order to compute aggregates
  ## efficiently in the next block of code. 
  ############################################################################################################################################
  
  print("Assigning risk values to the variables, and creating address mismatch and high amount flags...")
  
  assign_risk_and_flags <- function(input_hive_name, output_hive_name){
    
    # Function to assign the risk values. It will be wrapped into rxDataStep. 
    assign_risk <- function(data) {
      data <- data.frame(data, stringsAsFactors = F)
      
      for(name in  risk_variables){
        
        # Get the appropriate Risk Table from the Risk_list.
        Risk_df <- Risk_list[[name]]
        
        # Perform a left outer join with the Risk table. This will assign the risk value to every level of the variable. 
        data <- base::merge(data, Risk_df, by = name, all.x = TRUE)
        new_name <- paste(name, "risk", sep ="")
        colnames(data)[ncol(data)] <- new_name
        
        # If a new level was found in the data, the assigned risk is NULL. We convert it to 0. 
        row_na <- which(is.na(data[, new_name]) == TRUE) 
        data[row_na, new_name] <- 0
        
      }  
      return(data)  
    }
    
    
    # Input and Output pointers. 
    Input_hive <-  RxHiveData(table = input_hive_name)
    Output_hive <-  RxHiveData(table = output_hive_name)
    
    # Create buckets for various numeric variables with the function Bucketize. 
    # At the same time, we create other variables:
    ## isHighAmount: flag for transactions of a high amount. 
    ## various flags showing if there is a mismatch in the addresses variables.
    
    # At the same time, we map the accountID to integers with the murmur3.32 hash function.
    # and split the data into groups with the same accountIDs belonging to the same group.
    # The number of groups is determined by the numSplits variable.
    ## You can manually increase numSplits for bigger data sets. 
    numSplits <- 10
    
    rxDataStep(inData = Input_hive,
               outFile = Output_hive, 
               overwrite = TRUE, 
               rowsPerRead = 200000,
               transformFunc = assign_risk,
               transformObjects =  list(risk_variables = risk_vars, Risk_list = Risk_list, numSplits = numSplits),
               transforms = list(
                 ishighamount = ifelse(transactionamountusd > 150, "1", "0"),
                 acctbillingaddressmismatchflag = ifelse(paymentbillingaddress == accountaddress, "0", "1"),
                 acctbillingpostalcodemismatchflag = ifelse(paymentbillingpostalcode == accountpostalcode, "0", "1"),
                 acctbillingcountrymismatchflag = ifelse(paymentbillingcountrycode == accountcountry, "0", "1"),
                 acctbillingnamemismatchflag= ifelse(paymentbillingname == accountownername, "0", "1"),
                 acctshippingaddressmismatchflag = ifelse(shippingaddress == accountaddress, "0", "1"),
                 shippingBillingAddressmismatchflag = ifelse(shippingaddress == paymentbillingaddress, "0", "1"),
                 splitnumber = ceiling((sapply(as.character(accountid), murmur3.32) %% 100)*(numSplits-1)/100)
               ),
               transformPackages = "hashFunction")
  }
  
  # Apply the assign_risk_and_flags function. 
  assign_risk_and_flags(input_hive_name = HiveTable,
                        output_hive_name = paste(HiveTable, "Features1", sep = ""))
  
  
  ############################################################################################################################################
  ## The block below will create aggregates.
  ## They correspond to the number and amount of transactions in the past day and 30 days for every transaction per accountID.
  ## It is done by using rxExecBy. It splits the data according to the splitNumber. For each split, the function compute_aggregates will:
  ## 1. Load the split in memory. 
  ## 2. Split the data set into a list of data frames for each accountID. 
  ## 3. Compute aggregates for each accountID with the function aggregates_account_level.
  ## 4. Combine the results, use current values when no aggregates, and output the split with the aggregate variables. 
  ############################################################################################################################################
  
  print("Computing the number of transactions and their amounts in the past day and 30 days for every transaction per accountID...")
  
  # Function that computes aggregates for 1 split of the data based on splitNumber. 
  compute_aggregates <- function(keys, data, HDFSIntermediateDir, OutputName){
    
    data <- rxDataStep(data)
    data$transactiondatetime <- as.POSIXct(paste(data$transactiondate, sprintf("%06d", as.numeric(data$transactiontime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")
    
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
    Splits <- split(data, f = data$accountid)
    
    # Compute the aggregations for each accountID with the user defined function aggregates_account_level. 
    Aggregations_list <- lapply(X = Splits, FUN = aggregates_account_level)
    
    # Bind the results into 1 data frame. 
    Aggregations_df <- do.call("rbind", Aggregations_list)
    
    # Add the new variables to the initial data with a left outer join.  
    Output_df <- merge(x = data, y = Aggregations_df, by = "transactionid", all.x = TRUE)
    
    # The transactions that had no other transactions in the 30 day time frame have missing values. We convert them to 0.
    for(new_name in c("sumpurchasecount1dperuser", "sumpurchasecount30dperuser", "sumpurchaseamount1dperuser", "sumpurchaseamount30dperuser")){
      row_na <- which(is.na(Output_df[, new_name]) == TRUE) 
      Output_df[row_na, new_name] <- 0
    }
    
    # Save the result to text files. This will simplify combining the different results. 
    split_number <- Output_df$splitnumber[1]
    Output_df$splitnumber <- NULL
    Output_txt <- RxTextData(file = file.path(HDFSIntermediateDir, sprintf("%sSplits/%spart%s.csv", OutputName, OutputName, split_number)), fileSystem = RxHdfsFileSystem(), firstRowIsColNames = F)
    rxDataStep(inData = Output_df, outFile = Output_txt, overwrite = TRUE)
    
    ## Return the column names of the output, to be used when combining the txt files into xdf. 
    ## We are removing the header (column names) when writing to txt files. Otherwise, when combining them into 1 xdf file, the column names would be counted as values.
    return(colnames(Output_df))
  }
  
  # Apply the function in parallel based on the splitNumber.
  Input_hive <- RxHiveData(table = paste(HiveTable, "Features1", sep = ""))
  OutputName <- paste(HiveTable, "Features", sep = "")
  
  res <- rxExecBy(inData = Input_hive,
                  keys = c("splitnumber"),
                  func = compute_aggregates,
                  funcParams = list(HDFSIntermediateDir = HDFSIntermediateDir,
                                    OutputName = OutputName))
  
  
  ############################################################################################################################################
  ## The block below will combine all the text files created above into 1 xdf file with the correct variable types for the next step. 
  ############################################################################################################################################
  
  # We are going to combine the txt files into 1 xdf file. 
  # We need to specify the variable types at the same time. 
  
  # Get the column names and types from one of the output text files. 
  split_number <- 0
  Output_txt <- RxTextData(file = file.path(HDFSIntermediateDir, sprintf("%sSplits/%spart%s.csv", OutputName, OutputName, split_number)), fileSystem = RxHdfsFileSystem())
  outputColInfo <- rxCreateColInfo(Output_txt)
  names(outputColInfo) <- res[[1]]$result
  colNames <- names(outputColInfo)
  
  # Specify which variables should be converted to factors. 
  ## We do not convert the ID, time and addresses variables as well as the variables later converted to numeric risk values, since they are not used for training.
  factorCols <- c()
  for(name in colNames){
    if(outputColInfo[[name]]$type == "character" & ! name %in% c("accountid", "transactionid", "transactiondatetime", "transactiondate", "transactiontime", "paymentbillingaddress",
                                                                 "paymentbillingname", "accountaddress", "accountownername", "shippingaddress", risk_vars)){
      factorCols[length(factorCols) + 1] <- name
    }
  }
  
  ## Encode the label as a factor too for the model training step. 
  factorCols <- c(factorCols, "label")
  
  ## Create the newColInfo list  that specifies the factor variables. 
  newColInfo <- mapply(function(i, colname){
    if (colname %in% factorCols) {
      list(index=i, newName = colname, type = "factor")
    }else {
      list(index=i, newName = colname)
    }
  }, 1:length(colNames), colNames, SIMPLIFY = F)
  
  
  # Pointer to the intput directory with all the txt files.
  Tagged_Processed_Features_txt <- RxTextData(file = file.path(HDFSIntermediateDir, paste(HiveTable, "FeaturesSplits", sep = "")), fileSystem = RxHdfsFileSystem(), colInfo = newColInfo, firstRowIsColNames = F)
  
  # Pointer to the output xdf file. 
  Tagged_Processed_Features_xdf <- RxXdfData(file = file.path(HDFSIntermediateDir, paste(HiveTable, "Features", sep = "")), fileSystem = RxHdfsFileSystem())
  
  # Merging the text files into 1 xdf file while converting characters to factors.
  rxDataStep(inData = Tagged_Processed_Features_txt, 
             outFile = Tagged_Processed_Features_xdf, 
             overwrite = T)
  
  # Check for debugging that the output xdf file has the correct types and levels of variables.
  # colInfo_new <- rxCreateColInfo(Tagged_Processed_Features_xdf, sortLevels = TRUE)
  
  print("Feature Engineering finished!")
}