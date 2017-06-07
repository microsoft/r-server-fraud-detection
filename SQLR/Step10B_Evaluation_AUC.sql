/*
This script will create stored procedure to calculate AUC

parameters:
@table= the scored data to be evaluated
*/

use [OnlineFraudDetection]
go

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
(select * from sysobjects where name like 'sql_performance_auc') 
truncate table sql_performance_auc
else
create table sql_performance_auc ( 
AUC float
);

/* specify the query to select data to be evaluated. this query will be used as input for following R script */
declare @GetScoreData nvarchar(max) 
set @GetScoreData =  'select * from ' + @table

/* R script to calculate AUC */
insert into sql_performance_auc
exec sp_execute_external_script @language = N'R',
                                  @script = N'
 library(ROCR)
 scored_data <- InputDataSet
 pred <- prediction(scored_data$Score, scored_data$Label)
 auc = as.numeric(performance(pred,"auc")@y.values)
 OutputDataSet <- as.data.frame(auc)
',
  @input_data_1 = @GetScoreData
;
end