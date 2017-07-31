##########################################################################################################################################
## This R script will do the following:
## 1. Specify parameters for main function.
## 2. Define the main function for production batch scoring. 
## 3. Invoke the main function.

## Input : 1. Full path of the two input tables on HDFS (for scoring with Spark) 
##            OR the two tables as data frames (for in-memory scoring).
##         2. Working directories on local edge node and HDFS.
##         3. Stage: "Prod" for batch scoring.
## Output: The directory on HDFS which contains the Scores (Spark version) or The Scores table (in-memory version).

##########################################################################################################################################

##########################################################################################################################################
## Load the RevoScaleR library and Open Spark Connection
##########################################################################################################################################

library(RevoScaleR)
rxSparkConnect(consoleOutput = TRUE, reset = TRUE)

##########################################################################################################################################
## Directories
##########################################################################################################################################

# Local (edge node) working directory. We assume it already exists. 
LocalWorkDir <- paste("/var/RevoShare/", Sys.info()[["user"]], "/Fraud/prod", sep="") 
#dir.create(LocalWorkDir, recursive = TRUE)

# HDFS directory for user calculation. We assume it already exists. 
HDFSWorkDir <- paste("/",Sys.info()[["user"]],"/Fraud/prod", sep="")
#rxHadoopMakeDir(HDFSWorkDir)

# Current working directory should be set with setwd() to the location of the .R files.

##########################################################################################################################################
## Data sets full path
##########################################################################################################################################

# Paths to the input data sets on HDFS. 
Untagged_Transactions_str <- "/Fraud/Data/untaggedTransactions_Prod.csv"
Account_Info_str <- "/Fraud/Data/accountInfo.csv"

# Import the .csv files as data frames. stringsAsFactors = F to avoid converting the ID variables to factors, which takes a very long time.
Untagged_Transactions_df <- rxImport(RxTextData(file = Untagged_Transactions_str, fileSystem = RxHdfsFileSystem()), stringsAsFactors = F)
Account_Info_df <- rxImport(RxTextData(file = Account_Info_str, fileSystem = RxHdfsFileSystem()), stringsAsFactors = F)


##############################################################################################################################
## Define main function
##############################################################################################################################

## If Untagged_Transactions and Account_Info are data frames, the web scoring is done in_memory. 
## Use paths to csv files on HDFS for large data sets that do not fit in-memory. 

fraud_batch_scoring <- function(Untagged_Transactions, 
                                Account_Info, 
                                LocalWorkDir,
                                HDFSWorkDir,
                                Stage = "Prod")
{
  
  # Directory that holds the tables and model from the Development stage.
  LocalModelsDir <- file.path(LocalWorkDir, "model")
  
  if((class(Untagged_Transactions) == "data.frame") & (class(Account_Info) == "data.frame")){ # In-memory scoring. 
    source("./in_memory_scoring.R")
    print("Scoring in-memory...")
    return(in_memory_scoring(Untagged_Transactions, Account_Info, Stage = Stage))
    
  } else{ # Using Spark for scoring. 
    
    rxSparkConnect(consoleOutput = TRUE, reset = TRUE)
    
    # step0: intermediate directories creation.
    print("Creating Intermediate Directories on Local and HDFS...")
    source("./step0_directories_creation.R")
    
    # step1: merging the raw data. 
    source("./step1_merge_account_info.R")
    print("Step 1: Production data merging.")
    
    merge_account_info(Untagged_Transactions = Untagged_Transactions,
                       Account_Info = Account_Info,
                       HDFSWorkDir = HDFSWorkDir,
                       Stage = Stage)
    
    # step2: additional preprocessing. 
    source("./step4_preprocessing.R")
    print("Step 2: Additional preprocessing of the production data.")
    
    preprocess(HDFSWorkDir = HDFSWorkDir,
               HiveTable = "TaggedProd")
    
    
    # step3: feature engineering
    source("./step6_feature_engineering.R")
    print("Step 3: Feature Engineering.")
    
    feature_engineering(LocalWorkDir = LocalWorkDir,
                        HDFSWorkDir = HDFSWorkDir,
                        HiveTable = "TaggedProdProcessed",
                        Stage = Stage)
    
    # step4: making predictions. 
    source("./step8_prediction.R")
    print("Step 4: Making Predictions.")
    
    prediction(HDFSWorkDir = HDFSWorkDir,
               LocalWorkDir = LocalWorkDir,
               Input_Data_Xdf = "TaggedProdProcessedFeatures",
               Stage = Stage)
    
    # Return the directory storing the final scores. 
    return(file.path(HDFSWorkDir,"temp", "PredictScore"))
    
  }
}

##############################################################################################################################
## Apply the main function
##############################################################################################################################

# Case 1: Input are data frames. Scoring is performed in-memory. 
Scores <-  fraud_batch_scoring(Untagged_Transactions = Untagged_Transactions_df, 
                               Account_Info = Account_Info_df, 
                               LocalWorkDir = LocalWorkDir,
                               HDFSWorkDir = HDFSWorkDir, 
                               Stage = "Prod")

# Case 2: Input are paths to csv files. Scoring using Spark. 
## This alternative is slow and should only be used if the data set to score is too large to fit in memory.
#scores_directory <- fraud_batch_scoring(Untagged_Transactions = Untagged_Transactions_str, 
#                                        Account_Info = Account_Info_str, 
#                                        LocalWorkDir = LocalWorkDir,
#                                        HDFSWorkDir = HDFSWorkDir, 
#                                       Stage = "Prod")

# Warning: in case you get the following error: "Error: file.exists(inData1) is not TRUE", 
# you should reset your R session with Ctrl + Shift + F10 (or Session -> Restart R) and try running it again.
