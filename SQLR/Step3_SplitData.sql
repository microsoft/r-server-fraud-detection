/* 
This script will create stored procedure to split data on account level 

input parameter:
@table = table to be splitted
*/

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
DROP TABLE IF EXISTS Tagged_Training
DROP TABLE IF EXISTS Tagged_Testing
DROP TABLE IF EXISTS Temp

select *,
abs(CAST(CAST(HashBytes(''MD5'', accountID) AS VARBINARY(64)) AS BIGINT) % 100) as hashcode 
into Temp
from ' + @table + '

select * into Tagged_Training
from Temp
where hashcode > 30

select * into Tagged_Testing
from Temp
where hashcode <= 30

drop table Temp'

exec sp_executesql @hashacctNsplit

end