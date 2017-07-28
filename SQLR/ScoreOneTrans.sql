/*
This script creates the stored procedure to score one transaction by invoking the following store procedure:
1. ParseStr: parse the input string and save to a sql table
2. PredictR:  preprocess, feature engineer, and scoring the parsed transaction
*/

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS ScoreOneTrans
GO

create procedure ScoreOneTrans @inputstring VARCHAR(MAX)
as
begin

/* invoke ParseStr */
declare @invokeParseStr nvarchar(max)
set @invokeParseStr ='
exec ParseStr ''' + @inputstring + ''''
exec sp_executesql @invokeParseStr

/* invoke PredictR */
declare @invokePredictR nvarchar(max)
set @invokePredictR ='
exec PredictR ''Parsed_String'', ''Predict_Score_Single_Transaction'',''1''
'
exec sp_executesql @invokePredictR
SELECT  [Probability.1]  FROM [Fraud].[dbo].[Predict_Score_Single_Transaction]

end 