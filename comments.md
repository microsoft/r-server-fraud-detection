The Fraud Detection data description page (https://microsoft.github.io/r-server-fraud-detection/input_data.html)
You still have the Loan variables here. I guess you are waiting for Xinwei to send you the dictionary? He mentioned that he has them. @Xinwei Xue
 
The template contents (https://microsoft.github.io/r-server-fraud-detection/contents.html)
The R scripts names and descriptions should be updated. The description at the beginning also should be updated because you talk about "simulated data", while here we have real data. No simulation like in the other solutions.
 
 
Typical Workflow page (https://microsoft.github.io/r-server-fraud-detection/Typical.html?platform=onp) or (https://microsoft.github.io/r-server-fraud-detection/Typical.html)
1- I see lots of references to Loans (do a control + F to find them).
2- In step 2 you explain the evaluation metrics. I think these are good. Can you add them to the R and SQL descriptions, in the evaluation step ? 

Here are my comments on the part of “For the Database Analyst – Operationalize with SQL”:
 
1.       Steps before training are all implemented in pure sql now. Training, prediction and evaluation are implemented with sql stored procedures with embedded R code.
2.       The name of power shell script is “OnlineFraudDetection.ps1”, not “SQLR-Fraud-Detection.ps1”.
3.       The details of all tables are for Loan, not for fraud.
4.       Step1: Account_Info will be sorted in descent order of accountID and recordDateTime (I will change it in the readme file as well).


ME:
**CHECK PBI - Scores is now Predict_Scores**
Make this aka link:
http://aka.ms/fraud-detection-hdi
need R/install_server.R
FIND OUT # of transactions... add to tables.md