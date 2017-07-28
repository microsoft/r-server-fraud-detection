##########################################################################################################################################
## This R script will do the following:
## 1. Remote login to the edge node for authentication purpose.
## 2. Load model related files as a list which will be used when publishing web service.
## 3. Define the main web scoring function.
## 4. Publish the web service.
## 3. Verify the webservice locally.

## Input : 1. Full path of the two input tables on HDFS (for processing with Spark) 
##            OR the two tables as data frames (for in-memory processing).
##         2. Working directories on local edge node and HDFS.
##         3. Stage: "Web" for scoring remotely with web service.
## Output: The directory on HDFS which contains the Scores (Spark version) or The Scores table (in-memory version).

##########################################################################################################################################

##############################################################################################################################
## Setup
##############################################################################################################################

# Load mrsdeploy package.
library(mrsdeploy)

# Remote login for authentication purpose.
## This would only work if the edge node was configured to host web services. 
remoteLogin(
  "http://localhost:12800",
  username = "admin",
  password = "D@tascience2017",
  session = FALSE
)

# Grant additional permissions on HDFS and the edge node. 
system("hadoop fs -mkdir /user/RevoShare/rserve2")
system("hadoop fs -chmod 777 /user/RevoShare/rserve2")
dir.create("/var/RevoShare/rserve2", recursive = TRUE)
system("sudo chmod 777 /var/RevoShare/rserve2")

##########################################################################################################################################
## Directories
##########################################################################################################################################

# Local (edge node) working directory. We assume it already exists. 
LocalWorkDir <- paste("/var/RevoShare/", Sys.info()[["user"]], "/Fraud/prod", sep="") 
#dir.create(LocalWorkDir, recursive = TRUE)

# HDFS directory for user calculation. We assume it already exists. 
HDFSWorkDir <- paste("/",Sys.info()[["user"]],"/Fraud/prod", sep="")
#rxHadoopMakeDir(HDFSWorkDir)

# Local directory holding data and model from the Development Stage. 
LocalModelsDir <- paste(LocalWorkDir, "/model", sep ="")

##########################################################################################################################################
## Load data from the Development stage. 
##########################################################################################################################################

# Load .rds files saved from the Development stage and that will be used for web-scoring.

## Risk_list: list containing the risk tables created on the training set of the dev stage. 
## gbt_model: GBT model trained in the dev stage. 

Risk_list <- readRDS(file.path(LocalModelsDir, "Risk_list.rds"))
boosted_fit <- readRDS(file.path(LocalModelsDir, "gbt_model.rds"))


# They are packed in a list to be published along with the scoring function.
model_objects <- list(Risk_list = Risk_list, 
                      boosted_fit  = boosted_fit)

##############################################################################################################################
## Define main function
##############################################################################################################################

## If Untagged_Transactions and Account_Info are data frames, the web scoring is done in_memory. 
## Use paths to csv files on HDFS for large data sets that do not fit in-memory. 

fraud_web_scoring <- function(Untagged_Transactions, 
                              Account_Info, 
                              LocalWorkDir,
                              HDFSWorkDir,
                              Stage = "Web",
                              Username = Sys.info()[["user"]])
{
  
  if((class(Untagged_Transactions) == "data.frame") & (class(Account_Info) == "data.frame")){ # In-memory scoring. 
    source(paste("/home/", Username,"/in_memory_scoring.R", sep=""))
    print("Scoring in-memory...")
    return(in_memory_scoring(Untagged_Transactions, Account_Info, Stage = Stage))
    
  } else{ # Using Spark for scoring. 
    
    library(RevoScaleR)
    rxSparkConnect(consoleOutput = TRUE, reset = TRUE)
    
    # step0: intermediate directories creation.
    print("Creating Intermediate Directories on Local and HDFS...")
    source(paste("/home/", Username,"/step0_directories_creation.R", sep=""))
    
    # step1: merging the raw data. 
    source(paste("/home/", Username,"/step1_merge_account_info.R", sep=""))
    print("Step 1: Production data merging.")
    
    merge_account_info(Untagged_Transactions = Untagged_Transactions,
                       Account_Info = Account_Info,
                       HDFSWorkDir = HDFSWorkDir,
                       Stage = Stage)
    
    # step2: additional preprocessing. 
    source(paste("/home/", Username,"/step4_preprocessing.R", sep=""))
    print("Step 2: Additional preprocessing of the production data.")
    
    preprocess(HDFSWorkDir = HDFSWorkDir,
               HiveTable = "TaggedProd")
    
    
    # step3: feature engineering
    source(paste("/home/", Username,"/step6_feature_engineering.R", sep=""))
    print("Step 3: Feature Engineering.")
    
    feature_engineering(LocalWorkDir = LocalWorkDir,
                        HDFSWorkDir = HDFSWorkDir,
                        HiveTable = "TaggedProdProcessed",
                        Stage = Stage)
    
    # step4: making predictions. 
    source(paste("/home/", Username,"/step8_prediction.R", sep=""))
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
## Publish as a Web Service  
##############################################################################################################################

# Specify the version of the web service
version <- "v1.2.289"

# Publish the api for the character input case (ie. Untagged_Transactions and Account_Info are data paths.)
api_string <- publishService(
  "fraud_scoring_string_input",
  code = fraud_web_scoring,
  model = model_objects,
  inputs = list(Untagged_Transactions = "character",
                Account_Info = "character",
                LocalWorkDir = "character",
                HDFSWorkDir = "character",
                Stage = "character",
                Username = "character"),
  outputs = list(answer = "character"),
  v = version
)


# Publish the api for the data frame input case (ie. Web scoring is done in-memory.)
api_frame <- publishService(
  "fraud_scoring_dframe_input",
  code = fraud_web_scoring,
  model = model_objects,
  inputs = list(Untagged_Transactions = "data.frame",
                Account_Info = "data.frame",
                LocalWorkDir = "character",
                HDFSWorkDir = "character",
                Stage = "character",
                Username = "character"),
  outputs = list(answer = "data.frame"),
  v = version
)

##############################################################################################################################
## Verify The Published API  
##############################################################################################################################

# Paths to the input data sets on HDFS. 
Untagged_Transactions_str <- "/Fraud/Data/untaggedTransactions_Prod.csv"
Account_Info_str <- "/Fraud/Data/accountInfo.csv"

# Import the .csv files as data frames. stringsAsFactors = F to avoid converting the ID variables to factors, which takes a very long time.
Untagged_Transactions_df <- rxImport(RxTextData(file = Untagged_Transactions_str, fileSystem = RxHdfsFileSystem()), stringsAsFactors = F)
Account_Info_df <- rxImport(RxTextData(file = Account_Info_str, fileSystem = RxHdfsFileSystem()), stringsAsFactors = F)

# Verify the data frame input case.
result_frame <- api_frame$fraud_web_scoring(
  Untagged_Transactions = Untagged_Transactions_df,
  Account_Info = Account_Info_df,
  LocalWorkDir = LocalWorkDir,
  HDFSWorkDir = HDFSWorkDir,
  Stage = "Web",
  Username = Sys.info()[["user"]]
)

## To get the data frame result in a readable format: 
rows_number <- length(result_frame$outputParameters$answer$score)
Scores <- data.frame(matrix(unlist(result_frame$outputParameters$answer), nrow = rows_number), stringsAsFactors = F)
colnames(Scores) <- names(result_frame$outputParameters$answer)

# Verify the string input case.
## This alternative is slow and should only be used if the data set to score is too large to fit in memory.
#result_string <- api_string$fraud_web_scoring(
#  Untagged_Transactions = Untagged_Transactions_str,
# Account_Info = Account_Info_str,
#  LocalWorkDir = LocalWorkDir,
#  HDFSWorkDir = HDFSWorkDir,
#  Stage = "Web",
#  Username = Sys.info()[["user"]]
#)

# NOTE: If the api_string takes a very long time to run (> 15 minutes), you can try to kill all the YARN applications first.
# To do so, look for all the currently running YARN applications by running: 
## system("yarn application -list")
# Then kill each one of the applications. For example, if you see application_1498842980780_0027, run: 
## system("yarn application -kill application_1498842980780_0027")