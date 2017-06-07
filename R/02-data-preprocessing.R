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
## Account level partition: create trainFlag and append it as the last column of tagged_fraud table
####################################################################################################
account_level_partition <- function(in_table, out_table, key, nfRate, frdRate) {
  data <- rxImport(in_table)
  
  # Split and sample the data by key: all trans with the same key will stay in the same sample population.
  # A trainFlag column is added: trainFlag =1 for train set, trainFlag = 0 for test set
  
  splitDataByKey<-function(dataset, key, NFrate, Frate= NFrate)
  {
    Facct = unique(dataset[dataset$Label > 0,][key])  
    set.seed(23)
    Frand = runif(dim(Facct)[1])
    Facct$trainFlag = (Frand <= Frate)
    
    NFacct = unique(dataset[dataset$Label == 0,][key])
    NFrand = runif(dim(NFacct)[1])
    NFacct$trainFlag = (NFrand <= NFrate)
    
    data.set = merge(dataset, rbind(Facct,NFacct), by= key, all.x =TRUE, sort=FALSE)    
    return (data.set)
    
  }
  
  data <- splitDataByKey(data, key, nfRate, frdRate)
  #data_file_path <- file.path(tempdir(), "data.csv")
  #write.csv(x = data, 
  #         file = data_file_path,
  #          row.names = FALSE)
  #data_text <- RxTextData(file = data_file_path)
  rxDataStep(inData = data,
             outFile = out_table,
             overwrite = TRUE)
}
rxSetComputeContext(sql)
tagged_fraud_table <- RxSqlServerData(table = "tagged_fraud",
                                      connectionString = connection_string)
partition_fraud_table <- RxSqlServerData(table = "partition_fraud",
                                         connectionString = connection_string)
rxExec(account_level_partition, 
       in_table = tagged_fraud_table,
       out_table = partition_fraud_table,
       key = 'accountID',
       nfRate = 0.7,
       frdRate = 0.7)
####################################################################################################
## Create train and test tables and clean them respectively
####################################################################################################
clean_table <- function(in_table, out_table) {
  data <- rxImport(in_table)
  data$trainFlag <- NULL
  data[is.na(data)] <- 0
  data[data == "\"\"\"\"\"\"\"\""] <- 0
  data <- subset(data, transactionAmount > 0)
  date_time <- paste0(data$transactionDate, data$transactionTime)
  error_flag <- is.na(strptime(date_time, "%Y%m%d%H%M%S"))
  data <- data[!error_flag, ]
  #data_file_path <- file.path(tempdir(), "data.csv")
  #write.csv(x = data, 
  #          file = data_file_path,
  #          row.names = FALSE)
  #data_text <- RxTextData(file = data_file_path)
  rxDataStep(inData = data,
             outFile = out_table,
             overwrite = TRUE)
}
rxSetComputeContext(sql)
partition_fraud_table <- RxSqlServerData(table = "partition_fraud",
                                         connectionString = connection_string)
filter_table <- RxSqlServerData(sqlQuery = "select * from partition_fraud where trainFlag = 'TRUE'",
                                connectionString = connection_string)
clean_training_table <- RxSqlServerData(table = "clean_training",
                                        connectionString = connection_string)
rxExec(clean_table,
       in_table = filter_table,
       out_table = clean_training_table)
filter_table <- RxSqlServerData(sqlQuery = "select * from partition_fraud where trainFlag = 'FALSE'",
                                connectionString = connection_string)
clean_testing_table <- RxSqlServerData(table = "clean_testing",
                                       connectionString = connection_string)
rxExec(clean_table,
       in_table = filter_table,
       out_table = clean_testing_table)
####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
