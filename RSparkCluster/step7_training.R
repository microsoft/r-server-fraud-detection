##########################################################################################################################################
## This R script will train a gradient boosted trees (GBT) model on input data.

## Input : 1. LocalWorkDir and HDFSWorkDir: working directories on HDFS and local edge node.  
##         2. Input_Data_Xdf: training data.

## Output: Trained random forest model object.

##########################################################################################################################################

training <- function(HDFSWorkDir,
                     LocalWorkDir,
                     Input_Data_Xdf)
{
  
  # Load MicrosoftML library for rxFastTrees. 
  library("MicrosoftML")
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  # Define the directory where Risk tables will be loaded from. 
  LocalModelsDir <- file.path(LocalWorkDir, "model")
  
  Tagged_Training_Processed_Features_Xdf <- RxXdfData(file.path(HDFSIntermediateDir, Input_Data_Xdf), fileSystem = RxHdfsFileSystem())
  
  # Make equations
  print("Making equations for training ...")
  variables_all <- rxGetVarNames(Tagged_Training_Processed_Features_Xdf)
  variables_to_remove <- c("label", "accountid", "transactionid", "transactiondatetime", "transactiondate","transactiontime",
                           "transactioncurrencycode", "localhour", "ipstate", "ippostcode","ipcountrycode", "browserlanguage",
                           "accountpostalcode", "accountstate", "accountcountry", "paymentbillingpostalcode", "paymentbillingstate",
                           "paymentbillingcountrycode","paymentbillingaddress", "paymentbillingname", "accountaddress", "accountownername", "shippingaddress")
 
  training_variables <- variables_all[!(variables_all %in% variables_to_remove)]
  equation <- paste("label ~ ", paste(training_variables, collapse = "+", sep=""), sep="")
  
  # Train the GBT model.
  print("Training random forest model...")
  #rxSetComputeContext('local')
  #boosted_fit <- rxFastTrees(formula = as.formula(equation),
  #                            data = Tagged_Training_Processed_Features_Xdf,
  #                           type = c("binary"),
  #                           numTrees = 100,
  #                           learningRate = 0.2,
  #                           splitFraction = 5/24,
  #                            featureFraction = 1,
  #                           minSplit = 10,
  #                           unbalancedSets = TRUE,
  #                           randomSeed = 5)
  
  boosted_fit <- rxDForest(formula = as.formula(equation),
                            data = Tagged_Training_Processed_Features_Xdf,
                            nTree = 2, 
                            timesToRun = 20,
                            seed = 5,
                            method = "class",
                            scheduleOnce = TRUE, 
                            computeOobError=-1 )
  
  # Save the fitted model to the local edge node 
  saveRDS(boosted_fit, file = paste(LocalModelsDir, "/gbt_model.rds", sep = ""))
  print("Training finished!")
  print(paste("Model is saved on the edge node under ", LocalModelsDir, sep=""))
}