
# Load mrsdeploy package.
library(mrsdeploy)

# Remote login for authentication purpose.
## This would only work if the edge node was configured to host web services. 
remoteLogin(
  "http://localhost:12800",
  username = "admin",
  password = "XXYOURPW",
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

fraud_web_single <- function(Untagged_Transactions_str,
                             Account_Info_str,
                             Account_Num, 
                              Amount,
                              TransId,
                              LocalWorkDir,
                              HDFSWorkDir,
                              Stage = "Web",
                              Username = Sys.info()[["user"]])
{
  
  # Get full transaction details here - in a real world you'd create this entire data.frame
  # from info on the website. 
  # But for our example we'll just grab a static record instead and supply the amount.
    Transaction <- rxImport(RxTextData(file = Untagged_Transactions_str, 
                                        fileSystem = RxHdfsFileSystem()), 
                                        stringsAsFactors = F,
                                        transformObjects = list(trans=TransId),
                                        rowSelection = (transactionID == trans))
    
    # Use the amount supplied instead of static amount.  Assume this is a US purchase.
    Transaction$transactionAmountUSD = Amount
    Transaction$transactionAmount = Amount
    Transaction$currencyCode = "US"
    
    # get Account info for the transaction
    Account <- rxImport(RxTextData(file = Account_Info_str, 
                                    fileSystem = RxHdfsFileSystem()), 
                                    stringsAsFactors = F,
                                    transformObjects = list(acc=Transaction$accountID),
                                    rowSelection = (accountID == acc))
  
    return(in_memory_scoring(Transaction, Account, Stage = Stage)$score)
    
 
}
##############################################################################################################################
## Publish as a Web Service  
##############################################################################################################################

# Specify the version of the web service
version <- "v1.0.1"

api_single <- publishService(
  "fraud_scoring_single",
  code = fraud_web_single,
  model = model_objects,
  inputs = list(Untagged_Transactions_str = "character",
                Account_Info_str = "character",
                Account_Num = "character",
                TransID = "character",
                Amount = "numeric",
                LocalWorkDir = "character",
                HDFSWorkDir = "character",
                Stage = "character",
                Username = "character"),
  outputs = list(answer = "numeric"),
  v = version
)


# Paths to the input data sets on HDFS. 
Untagged_Transactions_str <- "/Fraud/Data/untaggedTransactions_Prod.csv"
Account_Info_str <- "/Fraud/Data/accountInfo.csv"

T1 <- "00DE5CED-3B4A-42AB-857E-99AB1141A1D1" 
T2 <- "FF27DDAE-569F-4D48-B554-D55D57116A68"
Amount = 589.50

# Verify the single score case.
result_frame <- api_single$api_single_Score(
  Untagged_Transactions = Untagged_Transactions_str,
  Account_Info = Account_Info_str,
  TransId = T1,
  Amount = Amount,
  LocalWorkDir = LocalWorkDir,
  HDFSWorkDir = HDFSWorkDir,
  Stage = "Web",
  Username = Sys.info()[["user"]]
)

api <- getService("fraud_scoring_dframe_input", version)
swagger <- api$swagger()
write(swagger, file = "swagger.json") 
