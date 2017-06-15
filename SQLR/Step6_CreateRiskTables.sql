/*
This script will create stored procedure to create all risk tables
*/

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
(select * from sysobjects where name like 'Risk_Var') 
truncate table Risk_Var
else
create table dbo.Risk_Var (ID int,var_names varchar(255), table_names varchar(255));

insert into Risk_Var values (1, 'transactionCurrencyCode', 'Risk_TransactionCurrencyCode');
insert into Risk_Var values (2, 'localHour', 'Risk_LocalHour');
insert into Risk_Var values (3, 'ipState', 'Risk_IpState');
insert into Risk_Var values (4, 'ipPostCode', 'Risk_IpPostCode');
insert into Risk_Var values (5, 'ipCountryCode', 'Risk_IpCountryCode');
insert into Risk_Var values (6, 'browserLanguage', 'Risk_BrowserLanguage');
insert into Risk_Var values (7, 'paymentBillingPostalCode', 'Risk_PaymentBillingPostalCode');
insert into Risk_Var values (8, 'paymentBillingState', 'Risk_PaymentBillingState');
insert into Risk_Var values (9, 'paymentBillingCountryCode', 'Risk_PaymentBillingCountryCode');
insert into Risk_Var values (10, 'accountPostalCode', 'Risk_AccountPostalCode');
insert into Risk_Var values (11, 'accountState', 'Risk_AccountState');
insert into Risk_Var values (12, 'accountCountry', 'Risk_AccountCountry');

/* create all risk tables by looping over all variables in reference table and executing CreateRiskTable stored procedure */
DECLARE @name_1 NVARCHAR(100)
DECLARE @name_2 NVARCHAR(100)
DECLARE @getname CURSOR

SET @getname = CURSOR FOR
SELECT var_names,
	   table_names
FROM   Risk_Var
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