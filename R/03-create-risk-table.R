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

###################################################################################################
## create risk table 
###################################################################################################
create_risk_table <- function(in_table, out_table){
  library(data.table)
  data <- rxImport(in_table)
  Calc_single_Risks <- function(dataframe, xcolumn, label, smooth1, smooth2){
    
    newname = make.names(xcolumn)
    
    DT <- data.table(dataframe) # make the data frame as a data.table object
    
    setnames(DT, xcolumn, newname) 
    
    #frd_count <- DT[, sum(label), by=columns]
    #caution: please exclude pre-fraud with label =2
    #equivalent to the sql language: select sum(label) as frd_count from DT group by xcolumn
    #calculate fraud count for each level of a particular column
    cmdStr = sprintf("frd_count <- DT[, sum(%s), by=%s]", label, newname)
    eval(parse(text=cmdStr))
    
    #nf_count <- DT[, sum(label), by=columns]
    #calculate non-fraud count for each level of a particular column
    cmdStr = sprintf("nf_count <- DT[, sum(1-%s), by=%s]", label, newname)
    eval(parse(text=cmdStr))
    
    #merge fraud count and non-fraud count by each level name
    Merge_Table <- merge(x=frd_count, y=nf_count, by=newname, all=TRUE, sort=TRUE)
    Merge_Table <- as.data.frame(Merge_Table)
    
    #smoothed fraud rate of each level
    Odds <- (Merge_Table['V1.x']+smooth1)/(Merge_Table['V1.y'] + Merge_Table['V1.x'] +smooth2)
    #log of smoothed odds ratio
    risk <- log(Odds/(1-Odds))
    
    #generate the key: xcolumn_levelname
    key = paste(xcolumn,Merge_Table[,newname], sep="_")  
    Risk_Frame <- cbind(key,risk)
    colnames(Risk_Frame) <- c("key", "risk")
    
    return(Risk_Frame)
  }
  
  Calc_Risks<-function(ycolname, xcolnames, dataset1)
  {  
    data.set =data.frame()
    for(name in xcolnames)
    {  
      risk.tab.data <- Calc_single_Risks(dataset1,name,ycolname, 10, 100)
      data.set= rbind(data.set, risk.tab.data)
    }
    return(data.set)
  }
  
  ycolname <- "Label"
  #assume all columns except the  target column will create risk table
  risk_vars <- c("transactionCurrencyCode", "localHour", "ipState", "ipPostcode",
                 "ipCountryCode", "browserLanguage", "shippingPostalCode", "shippingState",
                 "shippingCountry", "accountPostalCode", "accountState", "accountCountry",
                 "paymentBillingPostalCode", "paymentBillingState", 
                 "paymentBillingCountryCode", "Label")
  xcolnames = risk_vars[risk_vars != ycolname]
  subset_data <- subset(data, select = risk_vars)
  calculated_risks <- Calc_Risks(ycolname,xcolnames,subset_data)
  
  data <- calculated_risks
  #data_file_path <- file.path(tempdir(), "data.csv")
  #write.csv(x = data, 
  #         file = data_file_path,
  #         row.names = FALSE)
  #data_text <- RxTextData(file = data_file_path)
  rxDataStep(inData = data,
             outFile = out_table,
             overwrite = TRUE)
}

rxSetComputeContext(sql)
clean_training_table <- RxSqlServerData(table = "clean_training",
                                        connectionString = connection_string)
risk_table <- RxSqlServerData(table = "risk_table",
                              connectionString = connection_string)
rxExec(create_risk_table,
       in_table = clean_training_table,
       out_table = risk_table)
####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())


