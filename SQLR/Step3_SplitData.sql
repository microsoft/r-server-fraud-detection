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
DROP TABLE IF EXISTS Hash_Id

select accountID,
abs(CAST(CAST(HashBytes(''MD5'', accountID) AS VARBINARY(64)) AS BIGINT) % 100) as hashCode 
into Hash_Id
from ' + @table + '

select * into Tagged_Training
from ' +@table + '
where accountID in (select accountID from Hash_Id where hashCode <= 70)

select * into Tagged_Testing
from ' +@table + '
where accountID in (select accountID from Hash_Id where hashCode > 70)'

exec sp_executesql @hashacctNsplit

end