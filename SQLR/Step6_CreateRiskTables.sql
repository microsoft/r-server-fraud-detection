/*
This script will create stored procedure to create all risk tables
*/

use [OnlineFraudDetection]
go

set ansi_nulls on
go

set quoted_identifier on
go

DROP PROCEDURE IF EXISTS CreateRiskTable_ForAll
GO

create procedure CreateRiskTable_ForAll
as
begin

/* create a table to store names of variables and risk tables. will be used as reference in the loop later */ 
if exists 
(select * from sysobjects where name like 'sql_risk_var') 
truncate table sql_risk_var
else
create table dbo.sql_risk_var (ID int,var_names varchar(255), table_names varchar(255));

insert into sql_risk_var values (1, 'transactionCurrencyCode', 'sql_risk_transactionCurrencyCode');
insert into sql_risk_var values (2, 'localHour', 'sql_risk_localHour');
insert into sql_risk_var values (3, 'ipState', 'sql_risk_ipState');
insert into sql_risk_var values (4, 'ipPostCode', 'sql_risk_ipPostCode');
insert into sql_risk_var values (5, 'ipCountryCode', 'sql_risk_ipCountryCode');
insert into sql_risk_var values (6, 'browserLanguage', 'sql_risk_browserLanguage');
insert into sql_risk_var values (7, 'paymentBillingPostalCode', 'sql_risk_paymentBillingPostalCode');
insert into sql_risk_var values (8, 'paymentBillingState', 'sql_risk_paymentBillingState');
insert into sql_risk_var values (9, 'paymentBillingCountryCode', 'sql_risk_paymentBillingCountryCode');
insert into sql_risk_var values (10, 'accountPostalCode', 'sql_risk_accountPostalCode');
insert into sql_risk_var values (11, 'accountState', 'sql_risk_accountState');
insert into sql_risk_var values (12, 'accountCountry', 'sql_risk_accountCountry');

/* create all risk tables by looping over all variables in reference table and executing CreateRiskTable stored procedure */
DECLARE @name_1 NVARCHAR(100)
DECLARE @name_2 NVARCHAR(100)
DECLARE @getname CURSOR

SET @getname = CURSOR FOR
SELECT var_names,
	   table_names
FROM   sql_risk_var
OPEN @getname
FETCH NEXT
FROM @getname INTO @name_1,@name_2
WHILE @@FETCH_STATUS = 0
BEGIN
    EXEC CreateRiskTable @name_1,@name_2 -- create risk table by calling stored procedure CreateRiskTable
    FETCH NEXT
    FROM @getname INTO @name_1, @name_2
END

CLOSE @getname
DEALLOCATE @getname
end