/* 
This script will create the stored procedure to do the following:
1. uniform transactionTime to 6 digits if necessary
2. preprocess data 
3. save transaction data to historical table
4. feature engineering
5. scoring 
5. save the scored data set to a sql table

input parameters:
@inputtable = the table of data to be scored
@outputtable = the table stores the scored data
@getacctflag = the flag to indicate if merge with accountInfo table is needed: '1'=yes, '0'=no
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS PredictR
GO

create procedure PredictR @inputtable nvarchar(max),
                          @outputtable nvarchar(max),
                          @getacctflag nvarchar(max)
as
begin

/* merge with accountInfo table if getacctflag = '1' */
declare @mergeacct nvarchar(max) = '';
set @mergeacct = 'if cast(' + @getacctflag + ' as int) = 1 
begin
 EXEC MergeAcctInfo ' + @inputtable + '
end'
exec sp_executesql @mergeacct

/* select @inputtable into @table_acct if getacctflag = '0' */
declare @renametable nvarchar(max) = '';
set @renametable = 
'if cast(' + @getacctflag + ' as int) = 0 
begin
  drop table if exists ' + @inputtable + '_Acct
  select * into ' + @inputtable + '_Acct from ' + @inputtable + '
end'
exec sp_executesql @renametable

/* add a fake label if label doesn't exist */
declare @addlabel nvarchar(max) = '';
set @addlabel = '
IF NOT EXISTS(SELECT 1 FROM sys.columns 
          WHERE Name = N''label''
          AND Object_ID = Object_ID(N''' + @inputtable + '_Acct''))
BEGIN
    alter table ' + @inputtable + '_Acct add label int not null default(-1)
END'
exec sp_executesql @addlabel

/* preprocessing by calling the stored procedure 'Preprocess' */
declare @preprocess nvarchar(max)
set @preprocess = 'exec Preprocess ' + @inputtable + '_Acct'
exec sp_executesql @preprocess

/* save transactions to history table */
declare @sql_save2history nvarchar(max)
set @sql_save2history = 'exec Save2TransactionHistory ' + @inputtable + '_Acct_Processed, ''0'''
exec sp_executesql @sql_save2history

/* feature engineering by calling the stored procedure 'FeatureEngineer' */
declare @fe_query nvarchar(max) 
set @fe_query = 'exec FeatureEngineer ' + @inputtable + '_Acct_Processed'
exec sp_executesql @fe_query

/* specify the query to select data to be scored. This query will be used as input to following R script */
declare @GetData2Score nvarchar(max) 
set @GetData2Score =  'select * from ' + @inputtable + '_Acct_Processed_Features where label<=1';

/* Get the database name*/
DECLARE @database_name varchar(max) = db_name();

/* R script to do scoring and save scored dataset into sql table */
exec sp_execute_external_script @language = N'R',
                                  @script = N'
## Get the trained model
# Define connectioin string
connection_string <- paste("Driver=SQL Server;Server=localhost;Database=", database_name, ";Trusted_Connection=true;", sep="")
# Create an Odbc connection with SQL Server using the name of the table storing the model
OdbcModel <- RxOdbcData(table = "Trained_Model", connectionString = connection_string) 
# Read the model from SQL.  
boosted_fit <- rxReadObject(OdbcModel, "Gradient Boosted Tree")

## Point to testing data in sql server
test_sql <- RxSqlServerData(sqlQuery = sprintf("%s", inquery),
							connectionString = connection_string,
							stringsAsFactors = TRUE)

## Specify the pointer to output table
Predictions_gbt_sql <- RxSqlServerData(table = outputtable, connectionString = connection_string)

## Set the Compute Context to SQL.
sql <- RxInSqlServer(connectionString = connection_string)
#rxSetComputeContext(sql) 

## Scoring
library("MicrosoftML")
rxPredict(modelObject = boosted_fit,
          data = test_sql,
		  outData = Predictions_gbt_sql,
		  overwrite = T,
		  extraVarsToWrite = c("accountID", "transactionDateTime", "transactionAmountUSD", "label"))

'
 , @params = N' @inquery nvarchar(max), @database_name varchar(max), @outputtable nvarchar(max)'
 , @inquery = @GetData2Score
 , @database_name = @database_name
 , @outputtable = @outputtable
 ;
end