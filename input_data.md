---
layout: default
title: Input Data
---

## CSV File Description
--------------------------
XXXX
The **Data** folder contains the following data:
* **Loan.csv** and **Borrower.csv**: data sets with 100K rows of the simulated data used to build the end-to-end Loan Credit Risk solution.
* **Loan_Prod.csv** and **Borrower_Prod.csv**: data sets with about 20 rows of the simulated data used in the Production pipeline.

**Loan.csv** and **Loan_Prod.csv** contain the following fields:
<table class="table table-compressed table-striped">
  <tr><th>Field</th><th>Type</th><th>Description</th></tr>
  <tr><td>loanId</td><td>Integer</td><td>Unique Id of the loan</td></tr>
  <tr><td>memberId</td><td>Integer</td><td>Unique Id of the borrower</td></tr>
  <tr><td>date</td><td>Date</td><td>a) Historical data: the loan approval date <br/>b) Production data: the loan application date.  Format: M/D/YYYY (e.g. 3/9/2016, 3/13/2016)</td></tr>
  <tr><td>purpose</td><td>String</td><td>Purpose of the loan e.g., debtconsolidation</td></tr>
  <tr><td>isJointApplication</td><td>String</td><td>Flag about the nature of the application (joint or individual)</td></tr>
  <tr><td>loanAmount</td><td>Float</td><td>Total amount to be borrowed</td></tr>
  <tr><td>term</td><td>String</td><td>Number of months of payments on the loan e.g., 36 months</td></tr>
  <tr><td>interestRate</td><td>String</td><td>Interest Rate on the loan e.g., 7.21 %</td></tr>
  <tr><td>monthlyPayment</td><td>Float</td><td>Monthly payment owed by the borrower</td></tr>
  <tr><td>grade</td><td>String</td><td>Loan grade (risk-related) e.g. A2</td></tr>
  <tr><td>loanStatus</td><td>String</td><td>Status of the loan (Label) Values taken: Current, Charged Off (This field is not present in the Loan_Prod.csv file)</td></tr>
</table>

**Borrower.csv** and **Borrower_Prod.csv** contain the following fields:
<table class="table table-compressed table-striped">
  <tr><th>Field</th><th>Type</th><th>Description</th> </tr>
  <tr><td>memberId</td><td>Integer</td><td>Unique Id of the borrower</td></tr>
 <tr><td>residentialState</td><td>String</td><td>Residential state of the borrower
e.g., MA</td></tr>
 <tr><td>yearsEmployment</td><td>String</td><td>Number of years of employment of the borrower
e.g., 10+ years</td></tr>
 <tr><td>homeOwnership</td><td>String</td><td>Home ownership status of the borrower
Values taken: own, rent, mortgage</td></tr>
 <tr><td>annualIncome</td><td>Float</td><td>Annual income of the borrower</td></tr>
 <tr><td>incomeVerified</td><td>String</td><td>Flag indicating if the income was verified or not</td></tr>
<tr><td>dtiRatio</td><td>Float</td><td>Debt to income ratio: borrower’s total monthly debt payments (without mortgage and the requested loan) divided by the monthly income.  It is expressed in percentage</td></tr>
 <tr><td>lengthCreditHistory</td><td>Integer</td><td>Length of the credit history in terms of years</td></tr>
 <tr><td>numTotalCreditLines</td><td>Integer</td><td>Total number of credit lines in the borrower's credit file</td></tr>
 <tr><td>numOpenCreditLines</td><td>Integer</td><td>Number of open credit lines in the borrower's credit file</td></tr>
 <tr><td>numOpenCreditLines1Year</td><td>Integer</td><td>Number of credit lines in the borrower's credit file that were opened in the past year</td></tr>
 <tr><td>revolvingBalance</td><td>Float</td><td>Total credit revolving balance</td></tr>
 <tr><td>revolvingUtilizationRate</td><td>Float</td><td>Amount of credit the borrower is using relative to all available revolving credit e.g., 7.30%</td></tr>
 <tr><td>numDerogatoryRec</td><td>Integer</td><td>Number of derogatory public records (includes tax liens, bankruptcies, and other judgements such as civil lawsuits)</td></tr>
 <tr><td>numDelinquency2Years</td><td>Integer</td><td>Number of 30+ days past-due incidences of delinquency in the borrower's credit file for the past 2 years</td></tr>
 <tr><td>numChargeoff1year</td><td>Integer</td><td>Number of charge-offs within 1 year</td></tr>
 <tr><td>numInquiries6Mon</td><td>Integer</td><td>Number of inquiries in past 6 months</td></tr>
 </table>

