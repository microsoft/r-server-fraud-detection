/* 
This script will create stored procedure to split data on account level 

input parameter:
@table = table to be splitted
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS SplitData
GO

create procedure SplitData @table varchar(max)
as
begin


/* hash accountID into 100 bins and split */

declare @hashacctNsplit nvarchar(max)
set @hashacctNsplit ='
DROP TABLE IF EXISTS sql_tagged_training
DROP TABLE IF EXISTS sql_tagged_testing
DROP TABLE IF EXISTS temp

select *,
abs(CAST(CAST(HashBytes(''MD5'', accountID) AS VARBINARY(64)) AS BIGINT) % 100) as hashcode 
into temp
from ' + @table + '

select * into sql_tagged_training
from temp
where hashcode > 30

select * into sql_tagged_testing
from temp
where hashcode <= 30'

exec sp_executesql @hashacctNsplit

end