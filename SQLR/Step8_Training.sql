/* 
This script will create stored procedure to do the following:
1. down sample the majority
2. train a gradient boosted tree model
3. save the trained model into a sql table 

input parameters:
@table = the table used as training set
*/

use [OnlineFraudDetection]
go

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
(select * from sysobjects where name like 'sql_trained_model') 
truncate table sql_trained_model
else
create table sql_trained_model ( 
 id varchar(200) not null,
 value varbinary(max),
 constraint unique_id3 unique(id)
);

/* down sample the majority by: 
1. sort the data by Label and accountID in descent order 
2. select the top 10000 rows
*/
declare @GetTrainData nvarchar(max) 
set @GetTrainData =  'select top 10000 * from ' + @table + ' where Label<=1 order by Label DESC, accountID';

/* R script to train GBT model and save the trained model into a sql table */
execute sp_execute_external_script
  @language = N'R',
  @script = N' 
  train <- InputDataSet

  ## make the Label as factor
  train$Label <- as.factor(train$Label)

  ## make equations
  names <- colnames(train)[which(colnames(train) != c("Label","accountID","transactionDate","transactionTime_new","TransDateTime"))]
  equation <- paste("Label ~ ", paste(names, collapse = "+", sep=""), sep="")

  ## train GBT model
  boosted_fit <- rxBTrees(formula = as.formula(equation),
                          data = train,
                          learningRate = 0.2,
                          minSplit = 10,
                          minBucket = 10,
                          nTree = 100,
                          seed = 5,
                          lossFunction = "bernoulli")

  ## save the trained model in sql server
  # define the connection string
  connection_string <- c("Driver=SQL Server;Server=localhost;Database=OnlineFraudDetection;Trusted_Connection=true;")
  # set the compute context to local for tables exportation to SQL
  rxSetComputeContext("local")
  # Open an Odbc connection with SQL Server. 
  OdbcModel <- RxOdbcData(table = "sql_trained_model", connectionString = connection_string) 
  rxOpen(OdbcModel, "w") 
  # Write the model to SQL.  
  rxWriteObject(OdbcModel, "Gradient Boosted Tree", boosted_fit)
 
  ',
  @input_data_1 = @GetTrainData 
 ;
end