##########################################################################################################################################
## This R script will do the following:
## 1. Convert the UntaggedTransaction and AccountInfo data sets to hive tables.
## 2. Create the transactionDateTime variable based on transactionDate and transactionTime.
## 3. Merge the two tables Untagged_Transaction ad Account_Info.
## 4. Remove duplicates.

## Input : 1. 2 Data Tables: Untagged_Transactions, Account_Info.
##         2. HDFSWorkDir: the working directory on HDFS.
##         3. Stage: "Dev" for development, "Prod" for batch scoring, "Web" for web scoring. 
## Output: Hive table: UntaggedTransactionsAccountUnique (Stage = "Dev") or TaggedProd (Stage = "Prod" or "Web").

##########################################################################################################################################

merge_account_info <- function(Untagged_Transactions,
                               Account_Info,
                               HDFSWorkDir,
                               Stage)
{
 
  # For the Production or Web-Scoring stages, in order to avoid overwriting hive tables from the Development stage, 
  # we will add the suffix Prod to the table names. This is encoded in the variable hive_name that will be
  ## an empty string for Dev
  ## "Prod" for Prod or Web. 
  if(Stage == "Dev"){
    hive_name <- ""
  }else{
    hive_name <- "Prod"
  }
  
  # Define the intermediate directory that will hold the intermediate data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  
  ##############################################################################################################################
  ## The block below will convert the data format to Hive in order to increase the efficiency of rx functions. 
  ##############################################################################################################################
  
  print("Converting the input data to Hive on HDFS...")
  
  # Create Hive pointers for the 3 data sets on HDFS. 
  Untagged_Transactions_hive <- RxHiveData(table = sprintf("UntaggedTransactions%s", hive_name)) 
  Account_Info_hive <- RxHiveData(table = sprintf("AccountInfo%s", hive_name)) 
  
  # Check the input format. Return an error if it is not a path. 
  if((class(Untagged_Transactions) == "character") & (class(Account_Info) == "character")){
    
    # Text pointers to the inputs. 
    Untagged_Transactions_txt <- RxTextData(Untagged_Transactions, firstRowIsColNames = T, fileSystem = RxHdfsFileSystem())
    Account_Info_txt <- RxTextData(Account_Info, firstRowIsColNames = T, fileSystem = RxHdfsFileSystem()) 
    
    # Conversion to Hive tables. 
    ## At the same time, we create transactionDateTime and recordDateTime. This is done by:
    ## converting transactionTime into a 6 digit time.
    ## concatenating transactionDate and transactionTime.
    ## converting it to a DateTime "%Y%m%d %H%M%S" format. 
    rxDataStep(inData = Untagged_Transactions_txt, outFile = Untagged_Transactions_hive, overwrite = T,
               transforms = list(
                 transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
               ))
    
    rxDataStep(inData = Account_Info_txt, outFile = Account_Info_hive, overwrite = T,
               transforms = list(
                 recordDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
               ))
    
  } else {
    stop("invalid input format")
  }
  
  #############################################################################################################################################
  ## The block below will merge the two tables Untagged_Transactions and Account_Info. 
  ############################################################################################################################################
  
  print("Merging the 2 tables Untagged_Transactions and Account_Info ...")
  
  # Inner join of the 2 tables Untagged_Transactions and Account_Info using HIVE command 
  Drop_Untagged_Transactions_Account_query <- sprintf("hive -e \"DROP TABLE IF EXISTS UntaggedTransactionsAccount%s\"", hive_name)
  Create_Untagged_Transactions_Account_query <-  sprintf("hive -e \"CREATE TABLE UntaggedTransactionsAccount%s AS 
  SELECT ut.*, latestRecord, ai.accountOwnerName, ai.accountAddress, ai.accountPostalCode, ai.accountCity, ai.accountState,
  ai.accountCountry, ai.accountOpenDate, ai.accountAge, ai.isUserRegistered, 
  ai.paymentInstrumentAgeInAccount, ai.numPaymentRejects1dPerUser
  FROM UntaggedTransactions%s ut
  full outer join (
  SELECT t1.accountID, max(t2.recordDateTime) as latestRecord, t1.transactionDateTime 
  FROM UntaggedTransactions%s t1 join AccountInfo%s t2 
  ON t2.accountID = t1.accountID 
  WHERE t2.recordDateTime <= t1.transactionDateTime
  GROUP BY t1.accountID, t1.transactionDateTime
  ) as lastTrans
  ON (ut.accountID = lastTrans.accountID and ut.transactionDateTime = lastTrans.transactionDateTime)
  JOIN AccountInfo%s ai
  ON ut.accountID = ai.accountID and latestRecord = ai.recordDateTime\"", hive_name, hive_name, hive_name, hive_name, hive_name)
  
  # drop UntaggedTransactionsAccount table if exists
  #cat(Drop_Untagged_Transactions_Account_query)
  system(Drop_Untagged_Transactions_Account_query)
  
  # create table UntaggedTransactionsAccount by merging Untagged_Transactions and Account_Info tables
  #cat(Create_Untagged_Transactions_Account_query)
  system(Create_Untagged_Transactions_Account_query)
  
  ############################################################################################################################################
  ## The block below will remove duplicates from UntaggedTransactionsAccount
  ############################################################################################################################################
  
  print("Removing duplicates ...")
  
  Drop_UntaggedTransactionsAccountUnique_query <-sprintf("
  hive -e \"
  DROP TABLE IF EXISTS UntaggedTransactionsAccountUnique%s\"
  ", hive_name)
  
  Remove_UntaggedTransactionsAccount_Duplicates_query <- sprintf("
  hive -e \"
  CREATE TABLE UntaggedTransactionsAccountUnique%s AS
  SELECT t.* FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDateTime, transactionAmount
  ORDER BY transactionID ASC) RN 
  FROM UntaggedTransactionsAccount%s) as t
  WHERE t.RN = 1\"
  ", hive_name, hive_name)
  
  system(Drop_UntaggedTransactionsAccountUnique_query)
  system(Remove_UntaggedTransactionsAccount_Duplicates_query)
  
  #############################################################################################################################################
  ## The block below will tag the UntaggedTransactionsAccount by creating a fake label for rxPredict to work correctly.
  ## We also exclude transactions with a negative dollar amount or missing ID variables. This preprocessing step is done in the splitting
  ## step for the Development stage. 
  ############################################################################################################################################
  
  if(Stage == "Prod" | Stage == "Web"){
    print("Adding a fake label and removing rows with missing ID variables or negative transaction amount...")
    
    Drop_Tagged_query <- "hive -e \"DROP TABLE IF EXISTS TaggedProd\""
    Tagging_query <- "
     hive -e \"create table TaggedProd as
     select t.*, 1 as label
     from  UntaggedTransactionsAccountUniqueProd as t
     where accountID IS NOT NULL
     and transactionID IS NOT NULL 
     and transactionDateTime IS NOT NULL 
     and transactionAmountUSD >= 0\"
    "
    #cat(Drop_Tagged_query)
    system(Drop_Tagged_query)
    
    #cat(Tagging_query)
    system(Tagging_query)
  }
  
  print("Merging account info finished!")
}