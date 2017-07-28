##########################################################################################################################################
## This R script will perform evaluation on a scored data set.

## Input: 1. HDFSWorkDir: working directories on HDFS and local edge node
##        2. Scored_Data_Xdf: scored data set 
## Output: 1. AUC
##         2. Plotted ROC curve
##         3. Fraud level account metrics and plots. 

##########################################################################################################################################

evaluation <- function(HDFSWorkDir,
                       Scored_Data_Xdf)
{
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  # Pointer to the scored data
  Predict_Score_Xdf <- RxXdfData(file.path(HDFSIntermediateDir,Scored_Data_Xdf), fileSystem = RxHdfsFileSystem())
  
  # recreate transactiondatetime, change label type
  Predict_Score_New_Xdf <- RxXdfData(file.path(HDFSIntermediateDir,"PredictScoreNew"), fileSystem = RxHdfsFileSystem())
  
  rxDataStep(inData = Predict_Score_Xdf, 
             outFile = Predict_Score_New_Xdf, 
             overwrite = T, 
             #rowsPerRead = 200000,
             transforms = list(
               transactiondatetime = as.character(as.POSIXct(paste(transactiondate, sprintf("%06d", as.numeric(transactiontime)), sep=""), format = "%Y%m%d %H%M%S", tz = "GMT")),
               transactiondate = NULL, 
               transactiontime = NULL,
               PredictedLabel = NULL, 
               Score.1 = NULL,
               labelProb = Probability.1, 
               Probability.1 = NULL,
               label = as.numeric(as.character(label))
             ))
  
  
  # evaluation on transaction level
  print("Calculating transaction level metrics...")
  rxSetComputeContext('local')
  ROC <- rxRoc(actualVarName = "label", predVarNames = "labelProb", data = Predict_Score_New_Xdf, numBreaks = 1000)
  AUC <- rxAuc(ROC)
  plot(ROC, title = "ROC Curve for GBT")
  print(sprintf("AUC = %s", AUC))
  rxSparkConnect(consoleOutput = TRUE, reset = FALSE)
  
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
  
  scr2stat <- function(data, contactPeriod, sampleRateNF, sampleRateFrd)
  {
    #scr quantization/binning into 1000 equal bins
    
    #account level score is the maximum of trans scores of that account
    #all transactions after the first fraud transaction detected are value savings
    #input score file needs to be acct-date-time sorted   
    
    nRows <- nrow(data)
    nBins <- 1000
    
    #1. Calculate the perf stats by score band.  
    prev_acct <- data$accountid[1]
    is_frd_acct <- 0
    max_scr <- 0	
    
    scr_hash <- matrix(0, nBins, 10)	
    
    f_scr_rec <- vector("numeric", nBins)
    # nf_scr_rec <- matrix(0, nBins, 2)  #count, datetime
    nf_scr_rec_count <- vector("numeric", nBins)
    nf_scr_rec_time <- vector("numeric", nBins)
    
    for (r in 1:nRows){
      acct <- as.character(data$accountid[r])
      dolamt <- data$transactionamountusd[r]
      label <- data$label[r]
      score <- data$labelProb[r]
      datetime <- data$transactiondatetime[r]
      
      if(score == 0){ 
        score <- score + 0.00001
        print ("The following account has zero score!")
        print (paste(acct, dolamt, datetime,sep = " "))
      }
      
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
        is_frd_acct = 1
      } else{
        scr_hash[tran_scr_bin, 4] <- scr_hash[tran_scr_bin, 4] + dolamt	
        scr_hash[tran_scr_bin, 8] <- scr_hash[tran_scr_bin, 8] + 1 	
      }
      
      # ADR/VDR
      if(label == 1){
        # ADR
        f_scr_rec[tran_scr_bin] <- 1
        
        # VDR
        # If a higher score appeared before the current score, then this is also savings for the higher score.
        # Once a fraud transaction is discovered, all subsequent approved transactons are savings.
        for(i in  1: ceiling(max_scr*nBins)){
          f_scr_rec[i] <- f_scr_rec[i] + dolamt
        }
      } else { 
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
        scr_hash[i, j] <- scr_hash[i, j]+ scr_hash[i + 1, j]
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
  
  # Import the scored data
  Predictions <- rxImport(Predict_Score_New_Xdf, reportProgress = 0)
  
  # Sort data in acct_date_time order
  Predictions <- Predictions[with(Predictions, order(accountid,transactiondatetime)),]
  
  # Apply the evaluation function to the imported predictions table.
  perf <- scr2stat(data = Predictions,
                   contactPeriod = 30, 
                   sampleRateNF = 1,
                   sampleRateFrd = 1)
  
  # Performance plots. 
  ## ADR -- Fraud account detection rate
  plot(perf[, 9], perf[, 1], type = 'b', xlab = 'AFPR', ylab = 'ADR', xlim = c(0, 100))
  grid()
  
  ## VDR -- Value detection rate. The percentage of values saved.
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
  
  print("Evaluation finished!")
  
}