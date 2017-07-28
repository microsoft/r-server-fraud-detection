##########################################################################################################################################
## This R script will do batch scoring and evaluation

## Input: 1. LocalWorkDir and HDFSWorkDir: working directories on HDFS and local edge node.
##        2. Input_Data_Xdf: input data name of xdf file to be scored.
##        3. Stage: "Dev" for development, "Prod" for batch scoring, "Web" for web scoring. 
## Output: Scored data set.

##########################################################################################################################################

prediction <- function(HDFSWorkDir,
                       LocalWorkDir,
                       Input_Data_Xdf,
                       Stage)
{
  
  # Load the Microsoft ML library for rxPredict on the GBT model. 
  library("MicrosoftML")
  
  # Define the intermediate directory holding the input data.  
  HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")
  
  # Get the GBT model. 
  if(Stage == "Dev" | Stage == "Prod"){
    # Define the directory where the model will be loaded from. 
    LocalModelsDir <- file.path(LocalWorkDir, "model")
    
    # Import the model from LocalModelsDir
    boosted_fit <- readRDS(file.path(LocalModelsDir,"gbt_model.rds"))
    
  }else{
    boosted_fit <- model_objects$boosted_fit
  }
  
  print("Scoring the GBT...")
  
  # Pointer to the Xdf data to be scored
  Score_Data_Xdf <- RxXdfData(file.path(HDFSIntermediateDir,Input_Data_Xdf), fileSystem = RxHdfsFileSystem())
  
  # Pointer to the Xdf data of output 
  Predict_Score_Xdf <- RxXdfData(file.path(HDFSIntermediateDir,"PredictScore"), fileSystem = RxHdfsFileSystem())
  
  # Make predictions. 
  rxPredict(modelObject = boosted_fit,
            data = Score_Data_Xdf,
            outData = Predict_Score_Xdf,
            overwrite = T,
            extraVarsToWrite = c("accountid", "transactionid", "transactiondate","transactiontime", "transactionamountusd", "label"))
  
  if(Stage == "Dev"){
    # Save the Predictions data as a Hive table to be used in PowerBI for visualizations (only used in the Dev Stage). 
    Predict_Score_hive <- RxHiveData(table = "PredictScore") 
    rxDataStep(inData = Predict_Score_Xdf, outFile = Predict_Score_hive, overwrite = TRUE)
  }
  
  print("Scoring Finished!")
}