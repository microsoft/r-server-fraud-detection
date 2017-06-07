# load revolution R library
library(RevoScaleR)

####################################################################################################
## Compute context
####################################################################################################
connection_string <- "Driver=SQL Server;
Server=[Server name];
Database=[Database name];
UID=[User name];
PWD=[Password]"
sql_share_directory <- paste("c:\\AllShare\\", Sys.getenv("USERNAME"), sep = "")
dir.create(sql_share_directory, recursive = TRUE)
sql <- RxInSqlServer(connectionString = connection_string, 
                     shareDir = sql_share_directory)
local <- RxLocalSeq()
####################################################################################################
## Clean untagged and fraud file
####################################################################################################
# specify numeric and logical columns for untagged data
untagged_columns <- c(transactionAmountUSD = "numeric",
                      transactionAmount = "numeric",
                      localHour = "numeric",
                      isProxyIP = "logical",
                      digitalItemCount = "numeric",
                      physicalItemCount = "numeric",
                      accountAge = "numeric",
                      isUserRegistered = "logical",
                      paymentInstrumentAgeInAccount = "numeric",
                      sumPurchaseAmount1dPerUser = "numeric",
                      sumPurchaseAmount30dPerUser = "numeric",
                      sumPurchaseCount1dPerUser = "numeric",
                      numPaymentRejects1dPerUser = "numeric")
# set compute context to sql server
rxSetComputeContext(sql)
# specify untagged data source
untagged_data_table <- RxSqlServerData(table = "untaggedData", 
                                       connectionString = connection_string,
                                       colClasses = untagged_columns)
# specify the data source for output table
untagged_clean_table <- RxSqlServerData(table = "untagged_clean",
                                        connectionString = connection_string)
# uniform transactionTime of untagged data into 6 digits and write back to sql server
rxDataStep(inData = untagged_data_table,
           outFile = untagged_clean_table,
           transforms = list(transactionTime = sprintf("%06d", as.numeric(transactionTime))),
           overwrite = TRUE,
           rowsPerRead = 10000)

# uniform transactionTime of fraud data into 6 digits and write back to sql server
fraud_columns <- c(transactionAmount = "numeric",
                   localHour = "numeric")
fraud_data_table <- RxSqlServerData(table = "fraud",
                                    connectionString = connection_string,
                                    colClasses = fraud_columns)
fraud_clean_table <- RxSqlServerData(table = "fraud_clean",
                                     connectionString = connection_string)
rxDataStep(inData = fraud_data_table,
           outFile = fraud_clean_table,
           transforms = list(transactionTime = sprintf("%06d", as.numeric(transactionTime))),
           overwrite = TRUE)

# create the sorted untagged data source. note you may use a sql query referring to a table instead of a stored table
sql_query <- "select * from untagged_clean order by accountID, transactionDate, transactionTime"
untagged_sorted_table <- RxSqlServerData(sqlQuery = sql_query,
                                         connectionString = connection_string)
####################################################################################################
## Tag data with fraud type
####################################################################################################
tag_fraud <- function(in_table1, in_table2, out_table, tag_mode) {
  # in_table1 and in_table2 are input table source; out_table is output table source;
  df <- rxImport(in_table1)
  frd_df <- rxImport(in_table2)
  
  #Convert transaction level fraud file to account-level
  #Identify fraud start datetime and end datetime 
  #Input data frame should be in account-date-time order
  convTranFrd2Acct<- function(df)
  {
    #assume df is in account-date-time order
    nrows = dim(df)[1]
    prev_acct = 0
    
    
    acct_hash = list()
    
    
    for(r in 1:nrows)
    {
      acct = as.character(df$accountID[r])
      dt = df$transactionDate[r]
      tm = df$transactionTime[r]
      
      if(acct != prev_acct)
      {
        acct_hash[[acct]] = c(dt,tm, dt, tm) 		
      }
      else
      {
        acct_hash[[acct]][3] = dt
        acct_hash[[acct]][4] = tm
      }
      prev_acct = acct	
      
    }
    
    nrows = length(acct_hash)
    frd_acct = matrix(rep(0,nrows * 5), nrows, 5)
    frd_acct[,1] = names(acct_hash)
    
    for (i in 1:nrows)
    {
      frd_acct[i,2:5] = acct_hash[[i]]
    }   
    
    frd_acct_df = as.data.frame(frd_acct)
    colnames(frd_acct_df) = c("accountID","startDate", "startTime", "endDate", "endTime")
    return (frd_acct_df)
    
  }
  
  #Tag transactions with account-level fraud file.
  #Transactions of fraud account prior to the first fraud transaction are prefraud.
  #Prefraud -- 2
  #Fraud    -- 1
  #NonFraud -- 0
  tagTranWithAcctFrd <- function (df, frd_df)
  {
    
    tst.df = merge(df, frd_df, by="accountID", sort = TRUE, all.x =TRUE)
    
    tDT = paste(tst.df$transactionDate,tst.df$transactionTime,sep="")
    sDT = paste(tst.df$startDate,tst.df$startTime,sep="")
    eDT = paste(tst.df$endDate,tst.df$endTime,sep="")
    
    tst.df$Label = 0
    tst.df$Label[!is.na(tst.df$startDate) & tDT >=sDT & tDT <=eDT] = 1
    tst.df$Label[!is.na(tst.df$startDate) &tDT < sDT] = 2
    tst.df$Label[!is.na(tst.df$startDate) &tDT > eDT] = 2
    return (subset(tst.df, select=-c(startDate,startTime,endDate, endTime)))
  }
  
  #Tag transactions with transcation-level fraud file
  tagTranWithTranFrd <- function (df, frd_df)
  {
    cnames = names(df)  
    tmp_frd_df = subset(frd_df, select=transactionID);
    tmp_frd_df$Label2 = 1;
    
    #tag by transactionID
    tst.df = merge(df, tmp_frd_df, by="transactionID", sort = TRUE, all.x=TRUE)
    tst.df$Label = 0
    tst.df$Label[!is.na(tst.df$Label2)] = 1;
    
    if(! ('Label' %in% cnames) )
      cnames = c(cnames, 'Label')
    return (subset(tst.df, select=c(cnames)))
  }
  
  
  #Tag the transactions with fraud file
  #mode = "TRAN", do transaction-level tagging
  #otherwise, do account-level tagging.
  tagTranDataWithFraud<-function(df, frd_df, mode)
  {
    df$rowId = 1:dim(df)[1]
    
    if(mode == "TRAN")
    {
      print ("transaction-level tagging")
      tagged.df = tagTranWithTranFrd(df, frd_df)
    }
    else
    { 
      tagged.df = tagTranWithAcctFrd(df, convTranFrd2Acct(frd_df))  
    }
    
    #merge() somehow break the order of the rows, sort it back by rowId, and drop rowId
    tagged.df = tagged.df[with(tagged.df, order(rowId)),]
    tagged.df$rowId = NULL;
    return (tagged.df)
    
  }
  
  data <- tagTranDataWithFraud(df, frd_df, tag_mode)
  rxDataStep(inData = data,
             outFile = out_table,
             overwrite = TRUE,
             rowsPerRead = 10000)
}
rxSetComputeContext(sql)
sql_query <- "select * from untagged_clean order by accountID, transactionDate, transactionTime"
untagged_table <- RxSqlServerData(sqlQuery = sql_query,
                                  connectionString = connection_string)
sql_query <- "select * from fraud_clean order by accountID, transactionDate, transactionTime"
fraud_table <- RxSqlServerData(sqlQuery = sql_query,
                               connectionString = connection_string)
tagged_fraud_table <- RxSqlServerData(table = "tagged_fraud",
                                     connectionString = connection_string)

#rxExec function allows distributed execution of a function in parallel across nodes (computers) or cores of a "compute context" such as a cluster
rxExec(tag_fraud, 
       in_table1 = untagged_table,
       in_table2 = fraud_table,
       out_table = tagged_fraud_table,
       tag_mode = "Acct")
####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
