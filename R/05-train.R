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
## Training table
####################################################################################################
rxSetComputeContext(sql)
column_info <- list(Label = list(type = "factor", levels = c("1", "0")),
                    sumPurchaseCount30dPerUser = list(type = "integer"),
                    transactionType = list(type = "factor", levels = c("P")),
                    transactionMethod = list(type = "factor", levels = c("0")),
                    transactionDeviceType = list(type = "factor", levels = c("0")),
                    browserType = list(type = "factor", levels = c("0")),
                    paymentInstrumentType = list(type = "factor", 
                                                 levels = c("CREDITCARD",
                                                            "PAYPAL",
                                                            "DIRECTDEBIT",
                                                            "INICISPAYMENT")),
                    cardType = list(type = "factor", 
                                    levels = c("VISA",
                                               "MC",
                                               "AMEX",
                                               "DISCOVER",
                                               "0",
                                               "JCB")),
                    cardNumberInputMethod = list(type = "factor", levels = c("0")),
                    cvvVerifyResult = list(type = "factor", 
                                           levels = c("M",
                                                      "0",
                                                      "X",
                                                      "P",
                                                      "N",
                                                      "U",
                                                      "Y",
                                                      "S")),
                    responseCode = list(type = "factor", levels = c("0")),
                    purchaseProductType = list(type = "factor", levels = c("0")),
                    sumPurchaseAmount1dPerUser = list(type = "numeric"),
                    sumPurchaseAmount30dPerUser = list(type = "numeric"),
                    sumPurchaseCount1dPerUser = list(type = "integer"),
                    numPaymentRejects1dPerUser = list(type = "integer"),
                    transactionAmountUSD = list(type = "numeric"),
                    transactionAmount = list(type = "numeric"),
                    digitalItemCount = list(type = "integer"),
                    physicalItemCount = list(type = "integer"),
                    accountAge = list(type = "numeric"),
                    paymentInstrumentAgeInAccount = list(type = "numeric"),
                    isProxyIP = list(type = "factor", levels = c("FALSE", "0","TRUE")),
                    isUserRegistered = list(type = "factor", levels = c("FALSE","TRUE")))

training_table <- RxSqlServerData(sqlQuery = "select top 10000 * from training order by Label desc", #downsample. make sure the table schema. here, it's .rgarner instead of standard .dbo 
                                  connectionString = connection_string,
                                  colInfo = column_info)

training_vars <- rxGetVarNames(training_table)
training_vars <- training_vars[training_vars != "Label"]
formula <- as.formula(paste("Label~", paste(training_vars, collapse = "+")))

####################################################################################################
## Boosted tree modeling
####################################################################################################
train_gbt <- function(in_table,out_table,form){
  boosted_fit <- rxBTrees(formula = form,
                          data = in_table,
                          learningRate = 0.2,
                          minSplit = 10,
                          minBucket = 10,
                          nTree = 20, # small number of tree for testing purpose
                          seed = 5,
                          lossFunction = "bernoulli")
  saveRDS(boosted_fit, file="boostedfit.rds")
  boostedfit_raw <- readBin("boostedfit.rds", "raw", n=file.size("boostedfit.rds"))
  boostedfit_char <- as.character(boostedfit_raw)
  rxDataStep(inData = data.frame(x=boostedfit_char), 
             outFile = out_table,
             overwrite = TRUE)
  
}
rxSetComputeContext(sql)
model_table <- RxSqlServerData(table = "trained_model",
                               connectionString = connection_string)
rxExec(train_gbt,
       in_table = training_table,
       out_table = model_table,
       form = formula) 

####################################################################################################
## Cleanup
####################################################################################################
rm(list = ls())
