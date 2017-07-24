##########################################################################################################################################
## This R script will perform preprocessing on an input data.

## Input : 1. HDFSWorkDir:Working directory on HDFS.
##         2. HiveTable: Input data name of Hive table to be preprocessed.
## Output: Hive table with preprocessed data.

##########################################################################################################################################

preprocess <- function(HDFSWorkDir,
                       HiveTable){
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp") 
  
  
  # get variables with missing values 
  print("getting variable names with missing values...")
  
  # Point to the input hive table, while converting the strings to factors for correct computations with rxSummary. 
  factorRiskInfo <- mapply(function(names){list(type = "factor")}, 
                           c("paymentinstrumenttype",
                             "cardtype",
                             "paymentbillingpostalcode",
                             "paymentbillingcountrycode",
                             "accountpostalcode",
                             "accountcountry",
                             "transactioncurrencycode",
                             "ipstate",
                             "ippostcode",
                             "browserlanguage",
                             "paymentbillingstate",
                             "accountstate",
                             "isuserregistered"
                           ) , 
                           SIMPLIFY = F)
  
  Input_Table_hive <- RxHiveData(table = HiveTable)
  Input_Table_hivefactors <- RxHiveData(table = HiveTable, colInfo = factorRiskInfo) 
  
  var <- rxGetVarNames(Input_Table_hive)
  formula <- as.formula(paste("~", paste(var, collapse = "+")))
  summary <- rxSummary(formula, Input_Table_hivefactors, byTerm = TRUE)
  variables_NA <- summary$sDataFrame[summary$sDataFrame$MissingObs > 0, 1]
  variables_NA <- variables_NA[!variables_NA %in% c("accountid", "transactionid", "transactiondatetime", "transactiondate", "transactiontime")]
  
  # If no missing values, we will only preprocess the data. Otherwise, we clean and preprocess. 
  if(length(variables_NA) == 0){
    print("No missing values: only preprocessing will be performed.")
  } else{ 
    print("Variables containing missing values are:")
    print(variables_NA)
  }
  
  preprocessing <- function(data) {
    data <- data.frame(data, stringsAsFactors = F)
    
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
  
  # Output pointer. 
  Output_Table_hive <- RxHiveData(table = paste(HiveTable,"Processed",sep=""))
  
  # set compute context to local
  print("preprocessing...")
  rxDataStep(inData = Input_Table_hive, 
             outFile = Output_Table_hive, 
             overwrite = T, 
             transformFunc = preprocessing,
             transformObjects = list(var_with_NA = variables_NA)
  )
  
  print("Preprocessing finished!")
}