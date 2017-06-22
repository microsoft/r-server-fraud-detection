<#
.SYNOPSIS
Script to do fraud detection, using SQL Server and MRS. 
#>

[CmdletBinding()]
param(

[parameter(Mandatory=$true,ParameterSetName = "LoS")]
[ValidateNotNullOrEmpty()] 
[String]    
$ServerName = "",

[parameter(Mandatory=$true,ParameterSetName = "LoS")]
[ValidateNotNullOrEmpty()]
[String]
$DBName = "",

[parameter(Mandatory=$true,ParameterSetName = "LoS")]
[ValidateNotNullOrEmpty()]
[String]
$username ="",


[parameter(Mandatory=$true,ParameterSetName = "LoS")]
[ValidateNotNullOrEmpty()]
[String]
$password ="",

[parameter(Mandatory=$false,ParameterSetName = "LoS")]
[ValidateNotNullOrEmpty()]
[String]
$dataPath = ""
)

$scriptPath = Get-Location
$filePath = $scriptPath.Path+ "\"

if ($dataPath -eq "")
{
$parentPath = Split-Path -parent $scriptPath
$dataPath = $parentPath + "\Data\"
}

##########################################################################
# Function wrapper to invoke SQL command
##########################################################################
function ExecuteSQL
{
param(
[String]
$sqlscript
)
    Invoke-Sqlcmd -ServerInstance $ServerName  -Database $DBName -Username $username -Password $password -InputFile $sqlscript -QueryTimeout 200000
}

##########################################################################
# Function wrapper to invoke SQL query
##########################################################################
function ExecuteSQLQuery
{
param(
[String]
$sqlquery
)
    Invoke-Sqlcmd -ServerInstance $ServerName  -Database $DBName -Username $username -Password $password -Query $sqlquery -QueryTimeout 200000
}

##########################################################################
# Check if the SQL server/database exists
##########################################################################
$query = "IF NOT EXISTS(SELECT * FROM sys.databases WHERE NAME = '$DBName') CREATE DATABASE $DBName"
Invoke-Sqlcmd -ServerInstance $ServerName -Username $username -Password $password -Query $query -ErrorAction SilentlyContinue
if ($? -eq $false)
{
    Write-Host -ForegroundColor Red "Failed the test to connect to SQL server: $ServerName database: $DBName !"
    Write-Host -ForegroundColor Red "Please make sure: `n`t 1. SQL Server: $ServerName exists;
                                     `n`t 2. SQL user: $username has the right credential for SQL server access."
    exit
}

$query = "USE $DBName;"
Invoke-Sqlcmd -ServerInstance $ServerName -Username $username -Password $password -Query $query 

##########################################################################
# Development
##########################################################################

## create tables
Write-Host -ForeGroundColor 'green' ("Create SQL table.")
$script = $filePath + "step0_CreateTables.sql"
ExecuteSQL $script

## upload .csv files to SQL table
# populate untagged table
Write-Host -ForeGroundColor 'green' ("Populate untaggedTransactions table.")
$destination = $dataPath + "untaggedTransactions.csv"
$tableName = $DBName + ".dbo.Untagged_Transactions"
$tableSchema = $dataPath + "untaggedTransactions.xml"
bcp $tableName format nul -c -x -f $tableSchema  -U $username -S $ServerName -P $password  -t ',' 
bcp $tableName in $destination -t ',' -S $ServerName -f $tableSchema -F 2 -C "RAW" -U $username -P $password 
# populate fraud table
Write-Host -ForeGroundColor 'green' ("Populate fraud table.")
$destination = $dataPath + "fraudTransactions.csv"
$tableName = $DBName + ".dbo.Fraud"
$tableSchema = $dataPath + "fraudTransactions.xml"
bcp $tableName format nul -c -x -f $tableSchema  -U $username -S $ServerName -P $password  -t ',' 
bcp $tableName in $destination -t ',' -S $ServerName -f $tableSchema -F 2 -C "RAW" -U $username -P $password
# populate accountInfo table
Write-Host -ForeGroundColor 'green' ("Populate accountInfo table.")
$destination = $dataPath + "accountInfo.csv"
$tableName = $DBName + ".dbo.Account_Info"
$tableSchema = $dataPath + "accountInfo.xml"
bcp $tableName format nul -c -x -f $tableSchema  -U $username -S $ServerName -P $password  -t ',' 
bcp $tableName in $destination -t ',' -S $ServerName -f $tableSchema -F 2 -C "RAW" -U $username -P $password

## create stored procedure for utility function
Write-Host -ForeGroundColor 'Cyan' (" Creating utility function...")
$script = $filepath + "UtilityFunctions.sql"
ExecuteSQL $script

## sort accountInfo table
# create the stored procedure for sorting accountInfo table
$script = $filepath + "SortAcctTable.sql"
ExecuteSQL $script
# invoke the stored procedure for sorting accountInfo table
Write-Host -ForeGroundColor 'Cyan' (" Sorting Account_Info table...")
$query = "Exec sortAcctTable 'Account_Info'"
ExecuteSQLQuery $query

## merge with account info
# create the stored procedure for merging account info
$script = $filepath + "Step1_MergeAcctInfo.sql"
ExecuteSQL $script
# invoke the stored procedure for merging account info
Write-Host -ForeGroundColor 'Cyan' (" Merging with account info...")
$query = "EXEC MergeAcctInfo 'Untagged_Transactions'"
ExecuteSQLQuery $query

## tagging
# create the stored procedure for tagging
$script = $filepath + "Step2_Tagging.sql"
ExecuteSQL $script
# invoke the stored procedure for tagging
Write-Host -ForeGroundColor 'Cyan' (" Tagging on account level...")
$query = "EXEC Tagging 'Untagged_Transactions_Acct', 'Fraud'"
ExecuteSQLQuery $query

## split data
# create the stored procedure for splitting data
$script = $filepath + "Step3_SplitData.sql"
ExecuteSQL $script
# invoke the stored procedure for splitting data
Write-Host -ForeGroundColor 'Cyan' (" Splitting data into training and testing...")
$query = "EXEC SplitData 'Tagged'"
ExecuteSQLQuery $query

## data process
# create the stored procedure for processing data
$script = $filepath + "Step4_Preprocess.sql"
ExecuteSQL $script
# invoke the stored procedure for processing data
Write-Host -ForeGroundColor 'Cyan' (" Creating view for processing data, will be execute later...")
$query = "EXEC Preprocess 'Tagged_Training'"
ExecuteSQLQuery $query

## save transaction to historical table
# create the stored procedure for saving data to historical table
$script = $filepath + "Step5_Save2History.sql"
ExecuteSQL $script
# invoke the stored procedure for saving data to historical table
Write-Host -ForeGroundColor 'Cyan' (" Saving data to historical table...")
$query = "Exec Save2TransactionHistory 'Tagged_Training_Processed' ,'1'"
ExecuteSQLQuery $query

## create risk tables
# create the stored procedure for creating risk tables
$script = $filepath + "CreateRiskTable.sql"
ExecuteSQL $script
$script = $filepath + "Step6_CreateRiskTables.sql"
ExecuteSQL $script
# invoke the stored procedure for creating risk tables
Write-Host -ForeGroundColor 'Cyan' (" Creating risk tables...")
$query = "EXEC CreateRiskTable_ForAll"
ExecuteSQLQuery $query

## feature engineering
# create the stored procedure for feature engineering
$script = $filepath + "Step7_FeatureEngineer.sql"
ExecuteSQL $script
# invoke the stored procedure for feature engineering
Write-Host -ForeGroundColor 'Cyan' (" Creating views for feature engineering will be executed later...")
$query = "EXEC FeatureEngineer 'Tagged_Training_Processed'"
ExecuteSQLQuery $query

## training 
# create the stored procedure for training
$script = $filepath + "Step8_Training.sql"
ExecuteSQL $script
# invoke the stored procedure for training
Write-Host -ForeGroundColor 'Cyan' (" Training...")
$query = "EXEC TrainModelR 'Tagged_Training_Processed_Features'"
ExecuteSQLQuery $query

## prediction
# create the stored procedure for prediction
$script = $filepath + "Step9_Prediction.sql"
ExecuteSQL $script
# invoke the stored procedure for prediction
Write-Host -ForeGroundColor 'Cyan' (" Batch Scoring on testing set...")
$query = "EXEC PredictR 'Tagged_Testing', 'Predict_Score', '0'"
ExecuteSQLQuery $query

## Evaluation
# create the stored procedure for evaluation
$script = $filepath + "Step10A_Evaluation.sql"
ExecuteSQL $script
$script = $filepath + "Step10B_Evaluation_AUC.sql"
ExecuteSQL $script
# invoke the stored procedure for evaluation
Write-Host -ForeGroundColor 'Cyan' (" Evaluation on account level and transaction level...")
$query = "EXEC EvaluateR 'Predict_Score'"
ExecuteSQLQuery $query
$query = "EXEC EvaluateR_auc 'Predict_Score'"
ExecuteSQLQuery $query

## Scoring one transaction
# create the stored procedure for Parsing string
$script = $filepath + "ParseString.sql"
ExecuteSQL $script
# create the stored procedure for Scoring one transaction
$script = $filepath + "ScoreOneTrans.sql"
ExecuteSQL $script
# invoke the stored procedure for scoring one transaction
Write-Host -ForeGroundColor 'Cyan' (" Scoring one transaction...")
$query = "exec ScoreOneTrans 'C34F7C20-6203-42F5-A41B-AF26177345BE,A1055521358474530,2405.33,2405.33,USD,NULL,20130409,102958,14,A,P,NULL,NULL,NULL,92.97,dubayy,0,ae,FALSE,NULL,en-US,CREDITCARD,AMEX,NULL,NULL,NULL,33071,FL,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,0,4,NULL'"
ExecuteSQLQuery $query

Write-Host -ForeGroundColor 'green' ("Finished!")
