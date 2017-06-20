##########################################################################################################################################
## This R script will do the following:
## 1. Train a boosted tree classification model on the training set and save it to SQL. 
## 2. Preprocess and perform feature engineering for the testing set. 
## 3. Score the GBT on the test set.
## 4. Evaluate the tested model: ROC, AUC, and fraud level account metrics 

## Input : Featurized training set Tagged_Training_Processed_Features.
## Output: GBT Model, Predictions and Evaluation Metrics. 
##########################################################################################################################################


# Set the compute context to sql. 
rxSetComputeContext(sql)

# Input table pointer. 
Tagged_Training_Processed_Features_sql <- RxSqlServerData(sqlQuery = query_training_features, 
                                                          connectionString = connection_string,
                                                          colInfo = column_info)

##########################################################################################################################################
##	The block below will make the formula used for the training.
##########################################################################################################################################
# Write the formula after removing variables not used in the modeling.
## We remove the id variables, the dates, and the variables for which we computed the risk values. 
variables_all <- rxGetVarNames(Tagged_Training_Processed_Features_sql)

risk_vars <- c("transactionCurrencyCode", "localHour", "ipState", "ipPostCode","ipCountryCode", "browserLanguage",
               "accountPostalCode", "accountState", "accountCountry", "paymentBillingPostalCode", "paymentBillingState",
               "paymentBillingCountryCode")

variables_to_remove <- c("label", "accountID", "transactionID", "transactionDateTime", "transactionDate", "transactionTime", risk_vars)
training_variables <- variables_all[!(variables_all %in% c("label", variables_to_remove))]
formula <- as.formula(paste("label ~", paste(training_variables, collapse = "+")))

##########################################################################################################################################
## The block below will do the following:
## 1. Train a gradient boosted trees (GBT) model. 
## 2. Save the trained model to SQL Server.
##########################################################################################################################################
print("Training the gradient boosted trees (GBT) model...")

# Train the GBT Boosted Trees model.
library(MicrosoftML)
boosted_fit <- rxFastTrees(formula = formula,
                           data = Tagged_Training_Processed_Features_sql,
                           type = c("binary"),
                           numTrees = 100,
                           learningRate = 0.02,
                           splitFraction = 5/24,
                           featureFraction = 1,
                           minSplit = 10)	

# The standard RevoScaleR rxBTrees function can also be used.
#boosted_fit <- rxBTrees(formula = formula,
#                        data = Tagged_Training_Processed_Features_sql,
#                        learningRate = 0.2,
#                        minSplit = 10,
#                        minBucket = 10,
#                        nTree = 100,
#                        seed = 5,
#                        lossFunction = "bernoulli")

# Save the fitted model to SQL. Compute Context is set to local. 
rxSetComputeContext('local')

## Open an Odbc connection with SQL Server.
OdbcModel <- RxOdbcData(table = "Trained_Model", connectionString = connection_string)
rxOpen(OdbcModel, "w")

## Drop the Model table if it exists. 
if(rxSqlServerTableExists(OdbcModel@table, OdbcModel@connectionString)) {
  rxSqlServerDropTable(OdbcModel@table, OdbcModel@connectionString)
}

## Create an empty Model table. 
rxExecuteSQLDDL(OdbcModel, 
                sSQLString = paste(" CREATE TABLE [", OdbcModel@table, "] (",
                                   "     [id] varchar(200) not null, ",
                                   "     [value] varbinary(max), ",
                                   "     constraint unique_id3 unique (id))",
                                   sep = "")
)

## Write the model to SQL. 
rxWriteObject(OdbcModel, "Gradient Boosted Tree", boosted_fit)

# Close the Obdc connection used. 
rxClose(OdbcModel)

# Set the compute context back to SQL. 
rxSetComputeContext(sql)

############################################################################################################################################
## The block below will preprocess and perform feature engineering on the testing set, by calling previously defined functions. 
############################################################################################################################################

query_testing <- "SELECT label, accountID, transactionID, transactionDateTime, isProxyIP, paymentInstrumentType, cardType, paymentBillingAddress,
                         paymentBillingPostalCode, paymentBillingCountryCode, paymentBillingName, accountAddress, accountPostalCode,  
                         accountCountry, accountOwnerName, shippingAddress, transactionCurrencyCode,localHour, ipState, ipPostCode,
                         ipCountryCode, browserLanguage, paymentBillingState, accountState, transactionAmountUSD, digitalItemCount, 
                         physicalItemCount, accountAge, paymentInstrumentAgeInAccount, numPaymentRejects1dPerUser, isUserRegistered,
                         transactionDate, transactionTime
                  FROM Tagged 
                  WHERE accountID IN (SELECT accountID from Hash_Id WHERE hashCode > 70)
                  AND label != 2
                  AND accountID IS NOT NULL
                  AND transactionID IS NOT NULL 
                  AND transactionDateTime IS NOT NULL 
                  AND cast(transactionAmountUSD as float) >= 0"

# Apply the preprocessing and cleaning ot the training set. 
print("Cleaning and preprocessing the testing set...")

clean_preprocess(input_data_query = query_testing, 
                 output_sql_name = "Tagged_Testing_Processed")

# Apply the feature engineering on the testing set. 
print("Perform feature engineering the testing set...")

query_testing_features <- feature_engineer(input_sql_name = "Tagged_Testing_Processed",
                                           output_sql_name = "Tagged_Testing_Processed_Features1",
                                           no_of_rows = 60000)

# Create a pointer to the testing set. 
Tagged_Testing_Processed_Features_sql <- RxSqlServerData(sqlQuery = query_testing_features, 
                                                          connectionString = connection_string,
                                                          colInfo = column_info)

##########################################################################################################################################
## The block below will score the trained GBT on the test set and output the prediction table.
##########################################################################################################################################
print("Scoring the GBT...")

# Pointer to the SQL table where predictions will be written. 
Predict_Score1_sql <- RxSqlServerData(table = "Predict_Score1", connectionString = connection_string)

rxPredict(modelObject = boosted_fit,
          data = Tagged_Testing_Processed_Features_sql,
          outData = Predict_Score1_sql,
          overwrite = T,
          extraVarsToWrite = c("accountID", "transactionDate", "transactionTime", "transactionAmountUSD", "label"))


# To preserve the type of transactionDateTime, we recreate it.
# We also drop some variables. 
Predict_Score_sql <- RxSqlServerData(table = "Predict_Score", connectionString = connection_string)
rxDataStep(inData = Predict_Score1_sql, 
           outFile = Predict_Score_sql, 
           overwrite = T, 
           rowsPerRead = 200000,
           transforms = list(
             transactionDateTime = as.character(as.POSIXct(paste(transactionDate, sprintf("%06d", as.numeric(transactionTime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")),
             transactionDate = NULL, 
             transactionTime = NULL,
             PredictedLAbel = NULL, 
             Score.1 = NULL,
             labelProb = Probability.1, 
             Probability.1 = NULL
           ))

##########################################################################################################################################
## The block below will evaluate the model on the testing set by computing the AUC and plotting the ROC.  
##########################################################################################################################################

print("Evaluating the GBT model: Computing the AUC...")

# Set the compute context to local. 
rxSetComputeContext('local')

# Import the prediction table and convert label to numeric for correct evaluation. 
predictions_table <- "Predict_Score"
Predictions_sql <- RxSqlServerData(table = predictions_table, connectionString = connection_string)
Predictions <- rxImport(Predictions_sql)
Predictions$label <- as.numeric(as.character(Predictions$label))

# Plot the ROC and compute the AUC. 
ROC <- rxRoc(actualVarName = "label", predVarNames = "labelProb", data = Predictions, numBreaks = 1000)
AUC <- rxAuc(ROC)
plot(ROC, title = "ROC Curve for Logistic Regression")

print(sprintf("AUC = %s", AUC))
  
##########################################################################################################################################
## The block below will evaluate the model on the testing set by computing fraud level account metrics. 
##########################################################################################################################################

print("Evaluating the GBT model: Computing fraud level account metrics...")

## FRAUD ACCOUNT LEVEL METRICS: Implement account-level performance metrics and transaction-level metrics.
# ADR -- Fraud account detection rate
# VDR -- Value detection rate. The percentage of values saved.
# AFPR -- Account-level false positive ratio.
# ROC  -- Transaction-level ROC 
# $ROC -- Dollar weighted ROC
# TFPR -- Transaction level false positive ratio.

# sampling rate are taken into consideration to derive performance on original unsampled dataset.
# Variable contactPeriod is in the unit of days, indicating the lag before a customer is contacted again. 
# to verify high-score transactions are legitimate. 

scr2stat <- function(dataset, contactPeriod, sampleRateNF, sampleRateFrd)
{
  #scr quantization/binning into 1000 equal bins
  
  #accout level score is the maximum of trans scores of that account
  #all transactions after the first fraud transaction detected are value savings
  #input score file needs to be acct-date-time sorted   
  dataset$"Scored Probabilities" <- dataset$Score
  
  fields = names(dataset)
  if(! ("accountID" %in% fields)) {print ("Error: Need accountID column!")}
  if(! ("transactionDateTime" %in% fields)) {print ("Error: Need transactionDateTime column!")}
  if(! ("transactionAmountUSD" %in% fields)){print ("Error: Need transactionAmountUSD column!")}
  if(! ("labelProb" %in% fields)) {print ("Error: Need labelProb column!")}
  
  nRows = dim(dataset)[1]
  nBins = 1000
  
  #1. Calculate the perf stats by score band.  
  
  prev_acct <- dataset$accountID[1]
  prev_score <- 0
  is_frd_acct <- 0
  max_scr <- 0	
  
  scr_hash <- matrix(0, nBins, 10)	
  
  f_scr_rec <- vector("numeric", nBins)
  # nf_scr_rec <- matrix(0, nBins, 2)  #count, datetime
  nf_scr_rec_count <- vector("numeric", nBins)
  nf_scr_rec_time <- vector("numeric", nBins)
  
  for (r in 1:nRows){
    acct <- as.character(dataset$accountID[r])
    dolamt <- as.double(dataset$transactionAmountUSD[r])
    label <- dataset$label[r]
    score <- dataset$labelProb[r]
    datetime <- dataset$transactionDateTime[r]
    
    if(score == 0){ 
      score <- score + 0.00001
      print ("The following account has zero score!")
      print (paste(acct, dolamt, datetime,sep = " "))
    }
    
    if(label == 2) next
    
    if (acct != prev_acct){
      scr_bin <- ceiling(max_scr*nBins)
      
      if (is_frd_acct){
        scr_hash[, 5] <- scr_hash[, 5] + f_scr_rec   #vdr
        scr_hash[scr_bin, 1] <- scr_hash[scr_bin, 1] + 1   #adr
      } else{
        scr_hash[,6] <- scr_hash[, 6] + as.numeric(nf_scr_rec_count)  #FP with contact period, a FP could be considered as multiple
        scr_hash[scr_bin, 2] <- scr_hash[scr_bin, 2] + 1   #a FP account considered one acct  		
      }
      
      f_scr_rec <- vector("numeric", nBins)
      
      nf_scr_rec_count <- vector("numeric", nBins)
      nf_scr_rec_time <- vector("numeric", nBins)
      
      is_frd_acct <- 0
      total_nf_dol <- 0
      total_frd_dol <- 0
      max_scr <- 0
    }
    
    if (score > max_scr) {
      max_scr <- score
    }
    
    # Find out the bin the current account falls in. 
    tran_scr_bin <- ceiling(score*nBins)
    
    
    # Dollar weighted ROC and regular ROC.
    if(label == 1){
      scr_hash[tran_scr_bin, 3] <- scr_hash[tran_scr_bin, 3] + dolamt
      scr_hash[tran_scr_bin, 7] <- scr_hash[tran_scr_bin, 7] + 1
      is_frd_acct = 1;
    }
    else{
      scr_hash[tran_scr_bin, 4] <- scr_hash[tran_scr_bin, 4] + dolamt	
      scr_hash[tran_scr_bin, 8] <- scr_hash[tran_scr_bin, 8] + 1 	
    }
    
    # ADR/VDR
    if(label == 1)
    {
      # ADR
      f_scr_rec[tran_scr_bin] <- 1
      
      # VDR
      # If a higher score appeared before the current score, then this is also savings for the higher score.
      # Once a fraud transaction is discovered, all subsequent approved transactons are savings.
      for(i in  1: ceiling(max_scr*nBins))
      {
        f_scr_rec[i] <- f_scr_rec[i] + dolamt
      }
    }
    else
    { 
      # False Positive Accounts (FP) with recontact period.
      # Check if there is any earlier dates for the same or lower score.
      # Update the count and dates when within recontact period.
      
      #for(i in  1: floor(max_scr*nBins))
      for(i in  1: tran_scr_bin)
      {
        prev_time <- nf_scr_rec_time[i]
        #print(paste(i, tran_scr_bin, sep=" "))
        #print(paste(acct, datetime, sep=" "))
        #print(prev_time)
        if(prev_time > 0){
          timeDiff <- difftime(strptime(datetime,"%Y-%m-%d %H:%M:%S"), strptime(prev_time, "%Y-%m-%d %H:%M:%S"), units = "days") 
          if(timeDiff >= contactPeriod){
            nf_scr_rec_count[i] <- nf_scr_rec_count[i] + 1
            nf_scr_rec_time[i] <- datetime
          }
        } else{
          nf_scr_rec_count[i] <- nf_scr_rec_count[i] + 1
          nf_scr_rec_time[i] <- datetime
        }
      }
    } 
    prev_acct <- acct;
    
  }
  #1 -- #Frd Acct
  #2 -- #NF  Acct with infinite recontact period
  #3 -- $Frd Tran
  #4 -- $NF  Tran
  #5 -- $Frd Saving
  #6 -- #NF Acct with finite recontact period
  #7 -- #Frd Tran
  #8 -- #NF Tran
  #9 -- AFPR
  #10 --TFPR
  
  #2. Calculate the cumulative perf counts.
  
  # 5, 6 already in cumulative during previous calculation.
  for (i in (nBins-1):1){
    for(j in c(1:4,7:8)){
      scr_hash[i, j] <- scr_hash[i, j]+scr_hash[i + 1, j]
    }
  }
  
  #3 Calculate AFPR, TFPR:
  scr_hash[, 9] <- scr_hash[, 6]/(scr_hash[, 1] + 0.0001)
  scr_hash[, 10] <- scr_hash[, 8]/(scr_hash[, 7] + 0.0001)
  
  #print(scr_hash)
  
  #4. Calculate the ADR/VDR, ROC percentage.	 	
  for(j in c(1:5,7:8)){
    scr_hash[, j] <- scr_hash[, j]/scr_hash[1, j]
  }
  
  #5. Adjust for the sampling rate.
  for (j in c(1, 3, 5 ,7)){
    scr_hash[, j] <- scr_hash[, j]/sampleRateFrd
  }
  
  for (j in c(2, 4, 6 ,8)){
    scr_hash[, j] <- scr_hash[, j]/sampleRateNF
  }
  
  for (j in c(9, 10)){
    scr_hash[, j] <- scr_hash[, j]/sampleRateNF*sampleRateFrd
  }
  
  perf.df <- as.data.frame(scr_hash)
  colnames(perf.df) <- c("ADR", "PCT NF Acct", "Dol Frd", "Dol NF", "VDR", "Acct FP(recontact period)", "PCT Frd", "PCT NF","AFPR","TFPR")
  return(perf.df)	
}

# Apply the evaluation function to the imported predictions table.
  perf <- scr2stat(dataset = Predictions,
                   contactPeriod = 30, 
                   sampleRateNF = 1,
                   sampleRateFrd = 1)
  
# Performance plots. 
## ADR
plot(perf[, 9], perf[, 1], type = 'b', xlab = 'AFPR', ylab = 'ADR', xlim=c(0, 100))
grid()
  
## VDR
plot(perf[, 9], perf[, 5], type = 'b', xlab = 'AFPR', ylab = 'VDR', xlim = c(0, 100))
grid()
  
## Dollar weighted ROC
plot(perf[, 4], perf[, 3], type = 'b', xlab = 'PCT NF Dol', ylab = 'PCT Frd Dol', xlim = c(0, 0.1))
grid()
  
## ROC
plot(perf[, 8], perf[, 7], type = 'b', xlab ='PCT NF', ylab = 'PCT Frd', xlim = c(0, 0.1))
grid()
  
## TFPR vs TDR
plot(perf[, 10], perf[, 7], type = 'b', xlab ='TFPR', ylab = 'PCT Frd', xlim = c(0, 100))
grid()