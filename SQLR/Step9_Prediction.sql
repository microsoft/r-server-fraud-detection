/* 
This script will create the stored procedure to do the following:
1. uniform transactionTime to 6 digits if necessary
2. preprocess data 
3. save transaction data to historical table
4. feature engineering
5. scoring 
5. save the scored data set to a sql table

input parameters:
@table = the table of data to be scored
@getacctflag = the flag to indicate if merge with accountInfo table is needed: '1'=yes, '0'=no
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS PredictR
GO

create procedure PredictR @table nvarchar(max),
                          @getacctflag nvarchar(max)
as
begin

/* merge with accountInfo table if getacctflag = '1' */
declare @mergeacct nvarchar(max) = '';
set @mergeacct = 'if cast(' + @getacctflag + ' as int) = 1 
begin
 EXEC MergeAcctInfo ' + @table + '
end'
exec sp_executesql @mergeacct

/* select @table into @table_acct if getacctflag = '0' */
declare @renametable nvarchar(max) = '';
set @renametable = 
'if cast(' + @getacctflag + ' as int) = 0 
begin
  drop table if exists ' + @table + '_acct
  select * into ' + @table + '_acct from ' + @table + '
end'
exec sp_executesql @renametable

/* add a fake Label if Label doesn't exist */
declare @addlabel nvarchar(max) = '';
set @addlabel = '
IF NOT EXISTS(SELECT 1 FROM sys.columns 
          WHERE Name = N''Label''
          AND Object_ID = Object_ID(N''' + @table + '_acct''))
BEGIN
    alter table ' + @table + '_acct add Label int not null default(-1)
END'
exec sp_executesql @addlabel

/* preprocessing by calling the stored procedure 'Preprocess' */
declare @preprocess nvarchar(max)
set @preprocess = 'exec Preprocess ' + @table + '_acct'
exec sp_executesql @preprocess

/* save transactions to history table */
declare @sql_save2history nvarchar(max)
set @sql_save2history = 'exec Save2TransactionHistory ' + @table + '_acct_processed, ''0'''
exec sp_executesql @sql_save2history

/* feature engineering by calling the stored procedure 'FeatureEngineer' */
declare @fe_query nvarchar(max) 
set @fe_query = 'exec FeatureEngineer ' + @table + '_acct_processed'
exec sp_executesql @fe_query

/* specify the query to select data to be scored. This query will be used as input to following R script */
declare @GetData2Score nvarchar(max) 
set @GetData2Score =  'select * from ' + @table + '_acct_processed_features where Label<=1';

/* R script to do scoring and save scored dataset into sql table */
exec sp_execute_external_script @language = N'R',
                                  @script = N'
## Get the trained model
# Define connectioin string
connection_string <- c("Driver=SQL Server;Server=localhost;Database=OnlineFraudDetection;Trusted_Connection=true;")
# Create an Odbc connection with SQL Server using the name of the table storing the model
OdbcModel <- RxOdbcData(table = "sql_trained_model", connectionString = connection_string) 
# Read the model from SQL.  
boosted_fit <- rxReadObject(OdbcModel, "Gradient Boosted Tree")

## Point to testing data in sql server
test_sql <- RxSqlServerData(sqlQuery = sprintf("%s", inquery),
							connectionString = connection_string)

## Specify the pointer to output table
Predictions_gbt_sql <- RxSqlServerData(table = "sql_predict_score", connectionString = connection_string)

## Set the Compute Context to SQL.
sql <- RxInSqlServer(connectionString = connection_string)
#rxSetComputeContext(sql) 

## Scoring
rxPredict(modelObject = boosted_fit,
          data = test_sql,
		  outData = Predictions_gbt_sql,
          type = "response",
		  overwrite = T,
		  predVarNames = "Score",
		  extraVarsToWrite = c("accountID", "TransDateTime", "transactionAmountUSD", "Label"))

',
  @params = N' @inquery nvarchar(max)' ,
  @inquery = @GetData2Score
 ;
end