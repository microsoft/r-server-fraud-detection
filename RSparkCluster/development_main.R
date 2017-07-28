##########################################################################################################################################
## This R script will do the following:
## 1. Specify parameters for main function.
## 2. Define the main function for development. 
## 3. Invoke the main function.

## Input : 1. Full path of the three input tables on HDFS.
##         2. Working directories on local edge node and HDFS
##         3. Stage: "Dev" for development.
## Output: The evaluation metrics of the model. 
##         Tables and model to be used for Production or Web Scoring are copied to the Production directory. 

##########################################################################################################################################

# Current working directory should be set with setwd() to the location of the .R files.

##########################################################################################################################################
## Open Spark Connection and load RevoScaleR library. 
##########################################################################################################################################

rxSparkConnect(consoleOutput = TRUE, reset = TRUE)
library(RevoScaleR)

##########################################################################################################################################
## Data sets full path
##########################################################################################################################################

# Write the full path to the 3 data sets.
Untagged_Transactions <- "/Fraud/Data/untaggedTransactions.csv"
Account_Info <- "/Fraud/Data/accountInfo.csv"
Fraud_Transactions <- "/Fraud/Data/fraudTransactions.csv"

##########################################################################################################################################
## Directories
##########################################################################################################################################

# Local (edge node) working directory. We assume it already exists. 
LocalWorkDir <- paste("/var/RevoShare/", Sys.info()[["user"]], "/Fraud/dev", sep="") 
#dir.create(LocalWorkDir, recursive = TRUE)

# HDFS directory for user calculation. We assume it already exists. 
HDFSWorkDir <- paste("/",Sys.info()[["user"]],"/Fraud/dev", sep="")
#rxHadoopMakeDir(HDFSWorkDir)

##############################################################################################################################
## Define main function
##############################################################################################################################

## The user should replace the directory in "source" function with the directory of his own.
## The directory should be the full path containing the source scripts.

fraud_dev <- function(Untagged_Transactions,
                      Account_Info,
                      Fraud_Transactions,
                      LocalWorkDir,
                      HDFSWorkDir, 
                      Stage = "Dev",
                      update_prod_flag = 1){
  
  # step0: intermediate directories creation.
  print("Creating Intermediate Directories on Local and HDFS...")
  source(paste(getwd(),"/step0_directories_creation.R", sep =""))
  
  ## Define and create the directory where Risk tables, models etc. will be saved in the Development stage.
  LocalModelsDir <- file.path(LocalWorkDir, "model")
  if(dir.exists(LocalModelsDir)){
    system(paste("rm -rf ",LocalModelsDir,"/*", sep="")) # clean up the directory if exists
  } else {
    dir.create(LocalModelsDir, recursive = TRUE) # make new directory if doesn't exist
  }
  
  # step1: merging with account info
  source(paste(getwd(),"/step1_merge_account_info.R", sep =""))
  print("Step 1: Merging with account info...")
  merge_account_info(Untagged_Transactions = Untagged_Transactions,
                     Account_Info = Account_Info,
                     HDFSWorkDir = HDFSWorkDir, 
                     Stage = Stage)
  
  # step2: tagging
  source(paste(getwd(),"/step2_tagging.R", sep =""))
  print("Step 2: Tagging...")
  tagging(Input_Hive_Table = "UntaggedTransactionsAccountUnique",
          Fraud_Transactions = Fraud_Transactions,
          HDFSWorkDir = HDFSWorkDir)
  
  # step3: splitting
  print("Step3: Splitting...")
  source(paste(getwd(),"/step3_splitting.R", sep =""))
  
  # step4: preprocessing
  print("Step4: Preprocessing...")
  source(paste(getwd(),"/step4_preprocessing.R", sep =""))
  preprocess(HDFSWorkDir = HDFSWorkDir,
             HiveTable = "TaggedTraining")
  preprocess(HDFSWorkDir = HDFSWorkDir,
             HiveTable = "TaggedTesting")
  
  # step5: creating risk tables
  print("Step5: Creating risk tables...")
  source(paste(getwd(),"/step5_create_risk_tables.R", sep =""))
  create_risk_tables(LocalWorkDir = LocalWorkDir,
                     HDFSWorkDir = HDFSWorkDir,
                     HiveTable = "TaggedTrainingProcessed",
                     smooth1 = 10,
                     smooth2 = 100)
  
  # step6: feature engineering
  print("Step6: Feature Engineering...")
  source(paste(getwd(),"/step6_feature_engineering.R", sep =""))
  feature_engineering(LocalWorkDir = LocalWorkDir,
                      HDFSWorkDir = HDFSWorkDir,
                      HiveTable = "TaggedTrainingProcessed",
                      Stage = Stage)
  feature_engineering(LocalWorkDir = LocalWorkDir,
                      HDFSWorkDir = HDFSWorkDir,
                      HiveTable = "TaggedTestingProcessed",
                      Stage = Stage)
  
  
  # step7: training 
  print("Step7: Training...")
  source(paste(getwd(),"/step7_training.R", sep =""))
  training(HDFSWorkDir = HDFSWorkDir,
           LocalWorkDir = LocalWorkDir,
           Input_Data_Xdf = "TaggedTrainingProcessedFeatures")
  
  # copy risk tables, model object to production folder if update_prod_flag = 1
  if (update_prod_flag == 1){
    # Production directory that will hold the development data. 
    ProdModelDir <- paste("/var/RevoShare/", Sys.info()[["user"]], "/Fraud/prod/model/", sep="") 
    # Development directory that holds data to be used in Production. 
    DevModelDir <- LocalModelsDir
    
    source(paste(getwd(),"/copy_dev_to_prod.R", sep =""))
    copy_dev_to_prod(DevModelDir, ProdModelDir)
  } 
  
  # step8: prediction
  print("Step8: Prediction...")
  source(paste(getwd(),"/step8_prediction.R", sep =""))
  prediction(HDFSWorkDir = HDFSWorkDir,
             LocalWorkDir = LocalWorkDir,
             Input_Data_Xdf = "TaggedTestingProcessedFeatures",
             Stage = Stage)
  
  # step9: evaluation
  print("Step9: Evaluation...")
  source(paste(getwd(),"/step9_evaluation.R", sep =""))
  evaluation(HDFSWorkDir = HDFSWorkDir,
             Scored_Data_Xdf = "PredictScore")
}

##############################################################################################################################
## Apply the main function
##############################################################################################################################

fraud_dev (Untagged_Transactions = Untagged_Transactions, 
           Account_Info = Account_Info,
           Fraud_Transactions = Fraud_Transactions, 
           LocalWorkDir = LocalWorkDir, 
           HDFSWorkDir = HDFSWorkDir, 
           Stage = "Dev",
           update_prod_flag = 1)
