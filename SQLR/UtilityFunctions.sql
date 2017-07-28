/*
This script will create functions which will be used
*/

set ansi_nulls on
go

set quoted_identifier on
go

/* create the function to uniform transactionTime to 6 digits */
IF object_id(N'FormatTime', N'FN') IS NOT NULL
    DROP FUNCTION FormatTime
GO

create function dbo.FormatTime (@strTime varchar(255) ) 
returns varchar(255)
as
begin
  declare @strTimeNew varchar(255)
  set @strTimeNew = 
  case
    when len(@strTime) = 5 then concat('0',@strTime)
    when len(@strTime) = 4 then concat('00',@strTime)
    when len(@strTime) = 3 then concat('000',@strTime)
    when len(@strTime) = 2 then concat('0000',@strTime)
    when len(@strTime) = 1 then concat('00000',@strTime)
   else @strTime
  end
  return(@strTimeNew)
end
go
