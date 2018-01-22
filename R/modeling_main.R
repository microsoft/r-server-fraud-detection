##########################################################################################################################################
## This R script will do the following:
## 1. Specify parameters: Full path of the three input tables, SQL Server database name, User ID, Password, and Server Name.
## 2. Source the different scripts for the Development Stage. 

## Input : Full path of the three input tables, database name, User ID, Password, and Server Name.
## Output: Trained model and Predictions on the testing set as well as performance metrics.

##########################################################################################################################################

# Load library. 
library(RevoScaleR)

# Set the working directory to the R scripts location.
# setwd()

##########################################################################################################################################
## SPECIFY INPUTS
##########################################################################################################################################

# Data sets full path. The paths below work if the working directory is set to the R scripts location. 
Untagged_Transactions <- "../Data/Untagged_Transactions.csv"
Account_Info <- "../Data/Account_Info.csv"
#Fraud <- "../Data/Fraud.csv"
Fraud_Transactions <- "../Data/Fraud_Transactions.csv"


# Creating the connection string. Specify:
## Database name. If it already exists, tables will be overwritten. If not, it will be created.
## Server name. If conecting remotely to the DSVM, the full DNS address should be used with the port number 1433 (which should be enabled) 
db_name <- "FraudR"
server <- "localhost"
connection_string <- sprintf("Driver=SQL Server;Server=%s;Database=%s;Trusted_Connection=TRUE", server, db_name)
# Above connection is set up to use your Windows credentials
# To use an id/password instead, add them in the lines below and uncomment 
# user_id <- "XXXYOURID"
# password <- "XXXYOURPW"
# connection_string <- sprintf("Driver=SQL Server;Server=%s;Database=%s;UID=%s;PWD=%s", server, db_name, user_id, password)

##############################################################################################################################
## Database Creation. 
##############################################################################################################################

# Open an Odbc connection with SQL Server master database only to create a new database with the rxExecuteSQLDDL function.

connection_string_master <- sprintf("Driver=SQL Server;Server=%s;Database=master;Trusted_Connection=TRUE", server)
# Or with id/password:
# connection_string_master <- sprintf("Driver=SQL Server;Server=%s;Database=master;UID=%s;PWD=%s", server, user_id, password)

outOdbcDS_master <- RxOdbcData(table = "Default_Master", connectionString = connection_string_master)
rxOpen(outOdbcDS_master, "w")

# Create database if applicable. 
query <- sprintf( "if not exists(SELECT * FROM sys.databases WHERE name = '%s') CREATE DATABASE %s;", db_name, db_name)
rxExecuteSQLDDL(outOdbcDS_master, sSQLString = query)

# Close Obdc connection to master database. 
rxClose(outOdbcDS_master)

##############################################################################################################################
## Odbc connection and SQL Compute Context. 
##############################################################################################################################

# Open an Obdc connection with the SQL Server database that will store the modeling tables. (Only used for rxExecuteSQLddl) 
outOdbcDS <- RxOdbcData(table = "Default", connectionString = connection_string)
rxOpen(outOdbcDS, "w")

# Define SQL Compute Context for in-database computations. 
sql <- RxInSqlServer(connectionString = connection_string)

##############################################################################################################################
## Modeling Pipeline.
##############################################################################################################################

# Step 1: Tagging. 
print("Step 1: Tagging.")
source("./step1_tagging.R")

# Step 2: Splitting & Preprocessing the training set. 
print("Step 2: Splitting and Preprocessing the training set.")
source("./step2_splitting_preprocessing.R")

# Step 3: Feature Engineering. 
print("Step 3: Feature Engineering on the training set.")
source("./step3_feature_engineering.R")

# Step 4: training, preprocessing and feature engineering on the testing set, scoring and evaluation of GBT. 
print("Step 4: Training, Scoring and Evaluating.")
source("./step4_training_evaluation.R")

# Close the Obdc connection used for rxExecuteSQLddl functions. 
rxClose(outOdbcDS)

##########################################################################################################################################
## Function to get the top n rows of a table stored on SQL Server.
## You can execute this function at any time during  your progress by removing the comment "#", and inputting:
##  - the table name.
##  - the number of rows you want to display.
##########################################################################################################################################

display_head <- function(table_name, n_rows){
  table_sql <- RxSqlServerData(sqlQuery = sprintf("SELECT TOP(%s) * FROM %s", n_rows, table_name), connectionString = connection_string)
  table <- rxImport(table_sql)
  print(table)
}

# table_name <- "insert_table_name"
# n_rows <- 10
# display_head(table_name, n_rows)


