/*
This script will create stored procedure to calculate AUC

parameters:
@table= the scored data to be evaluated
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS dbo.EvaluateR_auc 
GO

create procedure dbo.EvaluateR_auc @table nvarchar(max)
as
begin

/* create table to store AUC value */
if exists 
(select * from sysobjects where name like 'Performance_Auc') 
truncate table Performance_Auc
else
create table Performance_Auc ( 
AUC float
);

/* specify the query to select data to be evaluated. this query will be used as input for following R script */
declare @GetScoreData nvarchar(max) 
set @GetScoreData =  'select * from ' + @table

/* R script to calculate AUC */
insert into Performance_Auc
exec sp_execute_external_script @language = N'R',
                                  @script = N'
 Predictions <- InputDataSet
 Predictions$label <- as.numeric(as.character(Predictions$label))

 # Compute the AUC. 
 ROC <- rxRoc(actualVarName = "label", predVarNames = "Probability.1", data = Predictions, numBreaks = 1000)
 AUC <- rxAuc(ROC)
 OutputDataSet <- as.data.frame(AUC)
',
  @input_data_1 = @GetScoreData
;
end