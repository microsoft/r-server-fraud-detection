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
## Classification model evaluation metrics
####################################################################################################
evaluate_model <- function(data, observed, predicted) {
  confusion <- table(data[[observed]], data[[predicted]])
  print(confusion)
  tp <- confusion[1, 1]
  fn <- confusion[1, 2]
  fp <- confusion[2, 1]
  tn <- confusion[2, 2]
  accuracy <- (tp + tn) / (tp + fn + fp + tn)
  precision <- tp / (tp + fp)
  recall <- tp / (tp + fn)
  fscore <- 2 * (precision * recall) / (precision + recall)
  metrics <- c("Accuracy" = accuracy,
               "Precision" = precision,
               "Recall" = recall,
               "F-Score" = fscore)
  return(metrics)
}

####################################################################################################
## Fraud account level metrics
####################################################################################################
# Implement account-level performance metrics and transaction-level metrics.
# ADR -- Fraud account detection rate
# VDR -- Value detection rate. The percentage of values saved.
# AFPR -- Account-level false positive ratio.
# ROC  -- Transaction-level ROC 
# $ROC -- Dollar weighted ROC
# TFPR -- Transaction level false positive ratio.
# sampling rate are taken into consideration to derive performance on original unsampled dataset.
# contactPeriod is in the unit of days, indicating the lag before a customer is contacted again 
# to verify high-score transactions are legitimate. 
scr2stat <-function(dataset, contactPeriod, sampleRateNF,sampleRateFrd)
{
  #scr quantization/binning into 1000 equal bins
  
  #accout level score is the maximum of trans scores of that account
  #all transactions after the first fraud transaction detected are value savings
  #input score file needs to be acct-date-time sorted   
  dataset$"Scored Probabilities" <- dataset$Boosted_Probability
  
  fields = names(dataset)
  if(! ("accountID" %in% fields)) 
  {print ("Error: Need accountID column!")}
  if(! ("transactionDate" %in% fields) |  !("transactionTime" %in% fields) )
  {print ("Error: Need transactionDate and transactionTime column!")}
  if(! ("transactionAmountUSD" %in% fields))
  {print ("Error: Need transactionAmountUSD column!")}
  if(! ("Scored Probabilities" %in% fields))
  {print ("Error: Need Scored Probabilities column!")}
  
  nRows = dim(dataset)[1];
  
  nBins = 1000; 
  
  #1. first calculate the perf stats by score band  
  
  prev_acct =dataset$accountID[1]
  prev_score = 0
  is_frd_acct = 0
  max_scr = 0	
  
  
  scr_hash=matrix(0, nBins,10)	
  
  f_scr_rec = vector("numeric",nBins)
  #nf_scr_rec = matrix(0, nBins,2)  #count, datetime
  nf_scr_rec_count = vector("numeric",nBins)
  nf_scr_rec_time = vector("numeric",nBins)
  
  for (r in 1:nRows)
  {
    acct = as.character(dataset$accountID[r])
    dolamt = as.double(dataset$transactionAmountUSD[r])
    label = dataset$Label[r]
    score = dataset$"Scored Probabilities"[r]
    datetime = paste(dataset$transactionDate[r],dataset$transactionTime[r], sep="")
    
    if(score == 0)
    { 
      score = score + 0.00001
      print ("The following account has zero score!")
      print (paste(acct,dolamt,datetime,sep=" "));
    }
    
    if(label == 2) next
    
    
    if (acct != prev_acct){
      scr_bin = ceiling(max_scr*nBins)
      
      
      if (is_frd_acct) {
        scr_hash[,5] = 	scr_hash[,5] + f_scr_rec   #vdr
        scr_hash[scr_bin,1] = scr_hash[scr_bin,1] + 1   #adr
      }
      else {
        scr_hash[,6] =  scr_hash[,6] + as.numeric(nf_scr_rec_count)  #FP with contact period, a FP could be considered as multiple
        scr_hash[scr_bin,2] = scr_hash[scr_bin,2]+1;   #a FP account considered one acct  		
      }
      
      f_scr_rec = vector("numeric",nBins)
      
      nf_scr_rec_count = vector("numeric",nBins)
      nf_scr_rec_time = vector("numeric",nBins)
      
      is_frd_acct = 0;
      total_nf_dol = 0;
      total_frd_dol = 0;
      max_scr = 0;
    }
    
    if (score > max_scr) {
      max_scr = score;
    }
    
    #find out the bin the current acct falls in. 
    tran_scr_bin = ceiling(score*nBins)
    
    
    #dollar weighted ROC and regular ROC
    if(label == 1){
      scr_hash[tran_scr_bin,3] = scr_hash[tran_scr_bin,3]+dolamt;
      scr_hash[tran_scr_bin,7] = scr_hash[tran_scr_bin,7]+1;
      is_frd_acct = 1;
    }
    else{
      scr_hash[tran_scr_bin,4] = scr_hash[tran_scr_bin,4]+dolamt;		
      scr_hash[tran_scr_bin,8] = scr_hash[tran_scr_bin,8]+1;  	
    }
    
    #ADR/VDR
    if(label == 1)
    {
      #ADR
      f_scr_rec[tran_scr_bin] = 1
      
      #VDR
      #If a higher score appeared before the current score, then this is also savings for the higher score.
      #Once a fraud transaction is discovered, all subsequent approved transactons are savings.
      for(i in  1: ceiling(max_scr*nBins))
      {
        f_scr_rec[i] = f_scr_rec[i] + dolamt
      }
    }
    else
    { 
      #False Positive Accounts (FP) with recontact period
      #check if there is any earlier dates for the same or lower score
      #update the count and dates when within recontact period
      
      #for(i in  1: floor(max_scr*nBins))
      for(i in  1: tran_scr_bin)
      {
        
        prev_time = nf_scr_rec_time[i]
        #print(paste(i, tran_scr_bin, sep=" "))
        #print(paste(acct, datetime, sep=" "))
        #print(prev_time)
        if( prev_time > 0)
        {
          timeDiff = difftime(strptime(datetime,"%Y%m%d%H%M%S"),strptime(prev_time,"%Y%m%d%H%M%S"), units="days" ) 
          if(timeDiff >= contactPeriod)
          {
            nf_scr_rec_count[i] = nf_scr_rec_count[i] +1
            nf_scr_rec_time[i] = datetime
          }
        }
        else
        {
          nf_scr_rec_count[i] = nf_scr_rec_count[i] +1
          nf_scr_rec_time[i] = datetime
        }
        
      }
      
    }  
    
    prev_acct = acct;
    
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
  
  #2. now calculate the cumulative perf counts
  
  # 5, 6 already in cumulative during previous calculation
  
  for (i in (nBins-1):1){
    
    for(j in c(1:4,7:8)){
      scr_hash[i,j] = scr_hash[i,j]+scr_hash[i+1,j];
    }
  }
  
  #3 calculate AFPR, TFPR:
  scr_hash[,9] = scr_hash[,6]/(scr_hash[,1]+0.0001)
  scr_hash[,10] = scr_hash[,8]/(scr_hash[,7]+0.0001)
  
  #print(scr_hash)
  
  #4. now calculate the ADR/VDR, ROC percentage	 	
  for(j in c(1:5,7:8)){
    scr_hash[,j] = scr_hash[,j]/scr_hash[1,j];
  }
  
  #5. Adjust for sampling rate
  for (j in c(1, 3, 5 ,7))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateFrd
  }
  
  for (j in c(2, 4, 6 ,8))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateNF
  }
  
  for (j in c(9, 10))
  {
    scr_hash[,j]= scr_hash[,j]/sampleRateNF*sampleRateFrd
  }
  
  
  perf.df = as.data.frame(scr_hash)
  colnames(perf.df) = c('ADR','PCT NF Acct','Dol Frd', 'Dol NF', 'VDR', 'Acct FP(recontact period)', 'PCT Frd', 'PCT NF','AFPR','TFPR' )
  return (perf.df)	
}

####################################################################################################
## ROC curve
####################################################################################################
roc_curve <- function(data, observed, predicted) {
  data <- data[, c(observed, predicted)]
  data[[observed]] <- as.numeric(as.character(data[[observed]]))
  rxRocCurve(actualVarName = observed,
             predVarNames = predicted,
             data = data)
}

#############################################
## compute the metrics
#############################################
rxSetComputeContext(local)
scored_table <- RxSqlServerData(table = "Scores",
                                connectionString = connection_string)
prediction_df <- rxImport(scored_table)
boosted_metrics <- evaluate_model(data = prediction_df,
                                  observed = "Label",
                                  predicted = "Boosted_Prediction")
roc_curve(data = prediction_df,
          observed = "Label",
          predicted = "Boosted_Probability")

contactPeriod = 30
nfSamplingRate = 1.0
frdSamplingRate = 1.0

perf = scr2stat(prediction_df, contactPeriod, nfSamplingRate, frdSamplingRate)
print(dim(perf))
print("finished perf calculation")

####################################################################################################
## Combine metrics and write to SQL
####################################################################################################
metrics_df <- rbind(boosted_metrics)
metrics_df <- as.data.frame(metrics_df)
rownames(metrics_df) <- NULL
Algorithms <- c("Boosted Decision Tree")
metrics_df <- cbind(Algorithms, metrics_df)
#metrics_file_path <- file.path(tempdir(), "binary_metrics.csv")
#write.csv(x = metrics_df, 
#          file = metrics_file_path,
#          row.names = FALSE)
#metrics_text <- RxTextData(file = metrics_file_path)
metrics_table <- RxSqlServerData(table = "binary_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = metrics_df,
           outFile = metrics_table,
           overwrite = TRUE)
####################################################################################################
## Write account metrics to SQL
####################################################################################################
#metrics_file_path <- file.path(tempdir(), "account_metrics.csv")
#write.csv(x = perf, 
#          file = metrics_file_path,
#          row.names = FALSE)
#metrics_text <- RxTextData(file = metrics_file_path)
metrics_table <- RxSqlServerData(table = "account_metrics",
                                 connectionString = connection_string)
rxDataStep(inData = perf,
           outFile = metrics_table,
           overwrite = TRUE)
####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
