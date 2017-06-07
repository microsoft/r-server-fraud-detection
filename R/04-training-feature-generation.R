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
## Update train tables with new features
####################################################################################################
create_new_features <- function(in_table_1, in_table_2, out_table, vars_to_keep) {
  # import data
  data <- rxImport(in_table_1)
  calculated_risks <- rxImport(in_table_2)
  # functions to assign risks
  Assign_single_Risks <- function(dataframe, riskframe, xcolumn){
    df_after_join <- merge(x=dataframe,y=riskframe, by="key", all.x=TRUE, sort=FALSE)
    df.join.ordered = df_after_join[order(df_after_join$id),]
    #print(df.join.ordered[1:20,])
    
    Risks_Col <- df.join.ordered['risk']
    na_index <- is.na(Risks_Col)
    
    Risks_Col[na_index] <- mean(as.numeric(unlist(riskframe['risk'])))
    riskVarName = paste(xcolumn, "_risk", sep="")
    names(Risks_Col) = riskVarName
    
    return(Risks_Col)
  }
  
  AssignRiskstoData<-function(dataset1,dataset2, xcolnames)
  {  
    ds1 = dataset1
    ds1$id = seq_len(nrow(ds1))
    ds1$key = paste(xcolnames[1], dataset1[[xcolnames[1]]], sep="_")
    data.set = Assign_single_Risks(ds1,dataset2,xcolnames[1])
    
    if (length(xcolnames) > 1) 
    {
      for(i in 2:length(xcolnames))
      {
        #ds1$key1 = ds1$key;
        ds1$key = paste(xcolnames[i], ds1[[xcolnames[i]]], sep="_")  	
        data.set = cbind(data.set,Assign_single_Risks(ds1,dataset2,xcolnames[i]))
        
      }
    }
    ds1$key = NULL
    
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
  assigned_risks <- AssignRiskstoData(data,calculated_risks,xcolnames)
  assigned_risks <- cbind(Label = data$Label, assigned_risks)
  
  # binary variables
  is_highAmount = data$transactionAmountUSD > 150
  bVars.df = as.data.frame(is_highAmount)
  
  #addresss mismatch flags
  bVars.df$acct_billing_address_mismatchFlag = as.character(data$paymentBillingAddress) == as.character(data$accountAddress)
  bVars.df$acct_billing_postalCode_mismatchFlag = as.character(data$paymentBillingPostalCode) == as.character(data$accountPostalCode)
  bVars.df$acct_billing_country_mismatchFlag = as.character(data$paymentBillingCountryCode) == as.character(data$accountCountry)
  bVars.df$acct_billing_name_mismatchFlag = as.character(data$paymentBillingName) == as.character(data$accountOwnerName)
  
  bVars.df$acct_shipping_address_mismatchFlag = as.character(data$shippingAddress) == as.character(data$accountAddress)
  bVars.df$acct_shipping_postalCode_mismatchFlag = as.character(data$shippingPostalCode) == as.character(data$accountPostalCode)
  bVars.df$acct_shipping_country_mismatchFlag = as.character(data$shippingCountry) == as.character(data$accountCountry)
  
  bVars.df$shipping_billing_address_mismatchFlag = as.character(data$shippingAddress) == as.character(data$paymentBillingAddress)
  bVars.df$shipping_billing_postalCode_mismatchFlag = as.character(data$shippingPostalCode) == as.character(data$paymentBillingPostalCode)
  bVars.df$shipping_billing_country_mismatchFlag = as.character(data$shippingCountry) == as.character(data$paymentBillingCountryCode)
  
  assigned_risks <- cbind(assigned_risks, bVars.df)
  data <- subset(data, select = vars_to_keep)
  data <- cbind(assigned_risks, data)
  data <- subset(data, Label != 2)
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
clean_training_table <- RxSqlServerData(table = "clean_training",
                                        connectionString = connection_string)
risk_table <- RxSqlServerData(table = "risk_table",
                              connectionString = connection_string)
training_table <- RxSqlServerData(table = "training",
                                  connectionString = connection_string)
vars_to_keep <- c("sumPurchaseAmount1dPerUser", "sumPurchaseAmount30dPerUser", "sumPurchaseCount1dPerUser",
                  "sumPurchaseCount30dPerUser", "numPaymentRejects1dPerUser", "transactionAmountUSD",
                  "transactionAmount", "transactionType", "transactionMethod", "transactionDeviceType",
                  "isProxyIP", "browserType", "paymentInstrumentType", "cardType", 
                  "cardNumberInputMethod", "cvvVerifyResult", "responseCode", "digitalItemCount",
                  "physicalItemCount", "purchaseProductType", "accountAge", "isUserRegistered", 
                  "paymentInstrumentAgeInAccount")
rxExec(create_new_features,
       in_table_1 = clean_training_table,
       in_table_2 = risk_table,
       out_table = training_table,
       vars_to_keep)
####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
