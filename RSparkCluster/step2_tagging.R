##########################################################################################################################################
## This R script will do the following:
## 1. Convert the fraud data set to a hive table.
## 2. Create the transactionDateTime variable based on transactionDate and transactionTime for fraud table.
## 3. Remove duplicates for fraud table.
## 4. Merge the input table with fraud table and create the label at the same time.

## Input : 1. Input_Hive_Table: name of the hive table from the merging step with the untagged transactions and account info. 
##         2. Path to csv Fraud files with the raw data Fraud_Transactions.
##         3. HDFSWorkDir:Working directory on HDFS.
## Output: Tagged data.

##########################################################################################################################################


tagging <- function(Input_Hive_Table,
                    Fraud_Transactions,
                    HDFSWorkDir)
{
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  
  ##############################################################################################################################
  ## The block below will convert the data format to Hive in order to increase the efficiency of rx functions. 
  ##############################################################################################################################
  
  print("Converting the fraud data to Hive on HDFS...")
  
  # Create Hive pointers for the 3 data sets on HDFS. 
  Fraud_Transactions_hive <- RxHiveData(table = "FraudTransactions") 
  
  # Check the input format. Return an error if it is not a path. 
  if(class(Fraud_Transactions) == "character"){
    
    # Text pointers to the inputs. 
    Fraud_Transactions_txt <- RxTextData(Fraud_Transactions, firstRowIsColNames = TRUE, fileSystem = RxHdfsFileSystem()) 
    
    # Conversion to Hive tables. 
    ## At the same time, we create transactionDateTime. This is done by:
    ## converting transactionTime into a 6 digit time.
    ## concatenating transactionDate and transactionTime.
    ## converting it to a DateTime "%Y%m%d %H%M%S" format. 
    rxDataStep(inData = Fraud_Transactions_txt, 
               outFile = Fraud_Transactions_hive,
               overwrite = TRUE, 
               transforms = list(
                 transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT"))
               ))
    
  } else {
    stop("invalid input format")
  }
  
  
  ############################################################################################################################################
  ## The block below will remove duplicates from the FraudTransactions table.
  ############################################################################################################################################
  print("Removing duplicates in the Fraud table...")
  
  Drop_FraudTransactionsUnique_query <-"
  hive -e \"
  DROP TABLE IF EXISTS FraudTransactionsUnique\"
  "
  Remove_FraudTransactions_Duplicates_query <- "
  hive -e \"
  CREATE TABLE FraudTransactionsUnique AS
  SELECT t.* FROM
  (SELECT *, ROW_NUMBER() OVER (PARTITION BY transactionID, accountID, transactionDateTime, transactionAmount
  ORDER BY transactionID ASC) RN 
  FROM FraudTransactions) as t
  WHERE t.RN = 1\"
  "
  
  system(Drop_FraudTransactionsUnique_query)
  system(Remove_FraudTransactions_Duplicates_query)
  
  #############################################################################################################################################
  ## The block below will tag the Input_Hive_Table on account level.
  ## The tagging is completed by merging UntaggedTransactionsAccount table with FraudTransactions table.
  ## The tagging logic is: 
  #    if accountID can't be found in fraud dataset => tag as 0, non fraud
  #    if accountID found in fraud dataset but transactionDateTime is out of the fraud time range => tag as 2, pre-fraud
  #    if accountID found in fraud dataset and transactionDateTime is within the fraud time range => tag as 1, fraud
  ############################################################################################################################################
  print("Tagging on account level ...")
  
  Drop_Tagged_query <- "hive -e \"DROP TABLE IF EXISTS Tagged\""
  Tagging_query <- paste("
                         hive -e \"create table Tagged as
                         select t.*, 
                         case when sDT is not null and tDT >= sDT and tDT <= eDT then 1
                         when sDT is not null and tDT < sDT then 2 
                         when sDT is not null and tDT > eDT then 2
                         when sDT is null then 0 end as label
                         from 
                         (select t1.*, t1.transactionDateTime as tDT, t2.startDateNTime as sDT, t2.endDateNTime as eDT
                         from ", Input_Hive_Table," as t1
                         left join
                         (select accountID, min(transactionDateTime) as startDateNTime,  max(transactionDateTime) as endDateNTime
                         from FraudTransactionsUnique 
                         group by accountID) as t2
                         on t1.accountID = t2.accountID) as t\"
                         ")
  #cat(Drop_Tagged_query)
  system(Drop_Tagged_query)
  
  #cat(Tagging_query)
  system(Tagging_query)
  
  print("Tagging finished!")
}