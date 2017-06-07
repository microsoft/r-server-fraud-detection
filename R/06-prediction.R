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

#####################################################################
## prediction function including feature engineering for testing data
#####################################################################
# in_table: testing data source
# out_table: scored data source
# model_table: the fitted model data source
# vars_to_keep: variables will be kept
pred <- function(in_table_1,in_table_2,out_table,model_table,vars_to_keep){
  # feature engineering for testing data  
  library(data.table)
  data <- rxImport(in_table_1)
  calculated_risks <- rxImport(in_table_2)
  
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
  prediction_df <- data
  
  # import the fitted model
  boostedfit_char <- rxImport(model_table)
  boostedfit_raw <- as.raw(strtoi(boostedfit_char$x, 16))
  writeBin(boostedfit_raw,con="boostedfit.rds")
  boosted_fit <- readRDS(file="boostedfit.rds")
  predictions <- rxPredict(modelObject = boosted_fit,
                           data = prediction_df,
                           type = "prob",
                           overwrite = TRUE)
  threshold <- 0.5
  names(predictions) <- c("Boosted_Probability")
  predictions$Boosted_Probability <- 1 - predictions$Boosted_Probability
  predictions$Boosted_Prediction <- ifelse(predictions$Boosted_Probability > threshold, 1, 0)
  predictions$Boosted_Prediction <- factor(predictions$Boosted_Prediction, levels = c(1, 0))
  prediction_df <- cbind(prediction_df, predictions)
  
  #data_file_path <- file.path(tempdir(), "data.csv")
  #write.csv(x = prediction_df, 
  #          file = data_file_path,
  #          row.names = FALSE)
  #data_text <- RxTextData(file = data_file_path)
  rxDataStep(inData = prediction_df,
             outFile = out_table,
             overwrite = TRUE)
}

model_table <- RxSqlServerData(table = "trained_model",
                               connectionString = connection_string)
clean_testing_table <- RxSqlServerData(table = "clean_testing",
                                       connectionString = connection_string)
risk_table <- RxSqlServerData(table = "risk_table",
                              connectionString = connection_string)
scored_table <- RxSqlServerData(table = "Scores",
                                connectionString = connection_string)
vars_to_keep <- c("sumPurchaseAmount1dPerUser", "sumPurchaseAmount30dPerUser", "sumPurchaseCount1dPerUser",
                  "sumPurchaseCount30dPerUser", "numPaymentRejects1dPerUser", "transactionAmountUSD",
                  "transactionAmount", "transactionType", "transactionMethod", "transactionDeviceType",
                  "isProxyIP", "browserType", "paymentInstrumentType", "cardType", 
                  "cardNumberInputMethod", "cvvVerifyResult", "responseCode", "digitalItemCount",
                  "physicalItemCount", "purchaseProductType", "accountAge", "isUserRegistered", 
                  "paymentInstrumentAgeInAccount")
vars_to_keep <- c(vars_to_keep, "accountID", "transactionDate", "transactionTime") # testing set need three more columns than training set

rxSetComputeContext(sql)
rxExec(pred,
       in_table_1 = clean_testing_table,
       in_table_2 = risk_table,
       out_table = scored_table,
       model_table = model_table,
       vars_to_keep = vars_to_keep)

####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
