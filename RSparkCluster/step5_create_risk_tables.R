##########################################################################################################################################
## This R script will create the risk tables for various character variables.  

## Input: 1. LocalWorkDir and HDFSWorkDir: working directories on HDFS and local edge node.
##        2. HiveTable: name of the Hive table containing the preprocessed training set to be used to create risk tables.
##        3. smooth1 and smooth2: smoothing parameters used to compute the risk values. 

## Output: Risk tables embedded in a list Risk_list, saved on the edge node. 
##########################################################################################################################################

create_risk_tables <- function(LocalWorkDir,
                               HDFSWorkDir,
                               HiveTable,
                               smooth1,
                               smooth2){
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  # Define the directory where Risk tables will be saved in the Development stage.
  LocalModelsDir <- file.path(LocalWorkDir, "model")
  
  # Variables for which we create Risk Tables. 
  risk_vars <- c("transactioncurrencycode", "localhour", "ipstate", "ippostcode","ipcountrycode", "browserlanguage",
                 "accountpostalcode", "accountstate", "accountcountry", "paymentbillingpostalcode", "paymentbillingstate",
                 "paymentbillingcountrycode")
  
  # Point to the input hive table, while converting the strings to factors for correct computations with rxSummary. 
  factorRiskInfo <- mapply(function(names){list(type = "factor")}, risk_vars, SIMPLIFY = FALSE)
  Tagged_Processed_hivefactors <- RxHiveData(table = HiveTable, colInfo = factorRiskInfo) 
  
  # Count the number of fraud and non-fraud observations for each level of the variables in risk_vars. 
  ## This is done in the following way:
  ## rxExecBy will split the Hive table according to the key argument (here label).
  ## The.counts function is then executed on each of the 2 splits and it returns the counts for each level of the variables.
  
  .counts <- function(keys, data, risk_vars){
    formula <- as.formula(paste("~", paste(risk_vars, collapse = "+")))
    summary <- rxSummary(formula = formula, data = data, byTerm = TRUE)
    Summary_Counts <- summary$categorical
    return(Summary_Counts)  
  }
  
  counts_by_label_list <- rxExecBy(inData = Tagged_Processed_hivefactors,
                                   keys = c("label"),
                                   func = .counts,
                                   funcParams = list(risk_vars = risk_vars))
  
  # Get the 2 lists of count tables, one for each label. 
  ## We use the $keys value to know which split corresponded to label = 0 and which one to label = 1.
  fraud_key <- ifelse(unlist(counts_by_label_list[[1]]$keys) == 1, 1, 2)
  non_fraud_key <- ifelse(fraud_key == 1, 2, 1) 
  Fraud_Counts_list <- counts_by_label_list[[fraud_key]]$result
  Non_Fraud_Counts_list <- counts_by_label_list[[non_fraud_key]]$result
  
  # Renaming column names accordingly to the label. 
  names(Fraud_Counts_list) <- lapply(Fraud_Counts_list, FUN = function(x){colnames(x)[1]})
  names(Non_Fraud_Counts_list) <- lapply(Non_Fraud_Counts_list, FUN = function(x){colnames(x)[1]})
  Fraud_Counts_list <- lapply(Fraud_Counts_list, FUN = function(df){setNames(df, c(colnames(df)[1],"fraudCount"))})
  Non_Fraud_Counts_list <- lapply(Non_Fraud_Counts_list, FUN = function(df){setNames(df, c(colnames(df)[1],"nonFraudCount"))})
  
  # Merging the results into 1 list of data frames. 
  Counts_list <- mapply(FUN = function(df1, df2){merge(df1, df2, all = TRUE)}, Fraud_Counts_list, Non_Fraud_Counts_list, SIMPLIFY = FALSE)
  
  # Replace NA with 0 (case when a level was not present for one of the labels).
  Counts_list <- lapply(Counts_list, FUN = function(df){df[is.na(df)] <- 0; return(df)})
  
  # Create the risk tables.
  ## Function for 1 data frame in the Counts_list. 
  compute_risk_values <- function(df){
    # Compute the smoothed odds for every level of the variable. 
    df$Odds <- (df$fraudCount + smooth1)/(df$nonFraudCount + df$fraudCount + smooth2)
    # Compute the log of the smoothed odds ratio. This is the risk value.
    df$Risk <- log(df$Odds/(1-df$Odds))
    return(df[, c(1,5)])
  }
  
  ## Apply compute_risk_values to every table of the Counts_list.
  Risk_list <- lapply(Counts_list, FUN = compute_risk_values)
  
  # Save it to the LocalModelsDir for future use. 
  saveRDS(Risk_list, file.path(LocalModelsDir, "Risk_list.rds"))
  
  print("Creating the Risk Tables finished!")
  print(sprintf("Risk tables created and saved on the edge node at %s", file.path(LocalModelsDir, "Risk_list.rds")))
  
}

