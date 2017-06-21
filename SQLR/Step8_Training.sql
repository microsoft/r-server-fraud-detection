/* 
This script will create stored procedure to do the following:
1. down sample the majority
2. train a gradient boosted tree model
3. save the trained model into a sql table 

input parameters:
@table = the table used as training set
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS TrainModelR
GO

create procedure TrainModelR @table nvarchar(max)
as
begin

/* Create an empty table to be filled with the trained models */
if exists 
(select * from sysobjects where name like 'Trained_Model') 
truncate table Trained_Model
else
create table Trained_Model ( 
 id varchar(200) not null,
 value varbinary(max)
 --,constraint unique_id3 unique(id)
);

/* down sample the majority by: 
1. sort the data by label and accountID in descent order 
2. select the top 10000 rows
*/
declare @GetTrainData nvarchar(max) 
set @GetTrainData =  'select top 10000 * from ' + @table + ' where label<=1 order by label DESC, accountID';

/*Get the database name*/
DECLARE @database_name nvarchar(max) = db_name();

/* R script to train GBT model and save the trained model into a sql table */
execute sp_execute_external_script
  @language = N'R',
  @script = N' 
  # define the connection string
  connection_string <- paste("Driver=SQL Server;Server=localhost;Database=", database_name, ";Trusted_Connection=true;", sep="")

  # Set the Compute Context to SQL for faster training.
  sql <- RxInSqlServer(connectionString = connection_string)
  rxSetComputeContext(sql)

  ## Point to testing data in sql server
  train_sql <- RxSqlServerData(sqlQuery = sprintf("%s", inquery),
							   connectionString = connection_string,
							   stringsAsFactors = TRUE)

  ## make equations
  variables_all <- rxGetVarNames(train_sql)
  variables_to_remove <- c("label", "accountID", "transactionDateTime")
  training_variables <- variables_all[!(variables_all %in% variables_to_remove)]
  equation <- paste("label ~ ", paste(training_variables, collapse = "+", sep=""), sep="")

  ## train GBT model
  library("MicrosoftML")
  boosted_fit <- rxFastTrees(formula = as.formula(equation),
                             data = train_sql,
                             type = c("binary"),
                             numTrees = 100,
                             learningRate = 0.2,
                             splitFraction = 5/24,
                             featureFraction = 1,
                             minSplit = 10,
							 randomSeed = 5)

  ## save the trained model in sql server 
  # set the compute context to local for tables exportation to SQL
  rxSetComputeContext("local")
  # Open an Odbc connection with SQL Server. 
  OdbcModel <- RxOdbcData(table = "Trained_Model", connectionString = connection_string) 
  rxOpen(OdbcModel, "w") 
  # Write the model to SQL.  
  rxWriteObject(OdbcModel, "Gradient Boosted Tree", boosted_fit)
 
  '
  , @params = N' @inquery nvarchar(max), @database_name varchar(max)'
  , @inquery = @GetTrainData
  , @database_name = @database_name
 ;
end