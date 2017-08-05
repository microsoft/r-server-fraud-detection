---
layout: default
title: For the Database Analyst
---
<div class="alert alert-success" role="alert"> This page describes the 
<strong>
<span class="cig">{{ site.cig_text }}</span>
<span class="onp">{{ site.onp_text }}</span>
</strong>
solution.
{% include sqlchoices.md %}
</div> 

## For the Database Analyst - Operationalize with SQL
------------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
        <li><a href="#system-requirements">System Requirements</a></li>
        <li><a href="#workflow-automation">Workflow Automation</a></li>
        <li><a href="#step0">Step 0: Data Preparation</a></li>
        <li><a href="#step1">Step 1: Merging with Account Information</a></li>
        <li><a href="#step2">Step 2: Tagging</a></li>
        <li><a href="#step3">Step 3: Splitting Data</a></li>
        <li><a href="#step4">Step 4: Preprocessing</a></li>
        <li><a href="#step5">Step 5: Saving Transactions to Historical Table</a></li>
        <li><a href="#step6">Step 6: Create Risk Tables</a></li>
        <li><a href="#step7">Step 7: Feature Engineering</a></li>
        <li><a href="#step8">Step 8: Model Training</a></li>
        <li><a href="#step9">Step 9: Batch Scoring</a></li>
        <li><a href="#step10">Step 10: Evaluation</a></li>
        <li><a href="#step11">Step 11: Production Scoring</a></li>
        </div>
    </div>
    <div class="col-md-6">
Fraudulent online transactions are becoming more and more of a problem to online merchants. This solution will show how to build a model to detect potential fraudulent transactions so that the transaction may be rejected.  View <a href="input_data.html">more information about the data.</a>
      <p/>
      <p>
 In this template, we implemented all steps in SQL stored procedures: data preprocessing is implemented in pure SQL, while feature engineering, model training, scoring and evaluation steps are implemented with SQL stored procedures with embedded R (Microsoft R Server) code.     This implementation with SQL Server R Services is equivalent to the <a href="https://gallery.cortanaanalytics.com/Collection/Online-Fraud-Detection-Template-1">Azure ML template for Online Fraud Detection</a>.
      </p>

        </div>
</div>

<span class="onp">For businesses that prefers an on-prem solution, the implementation with SQL Server R Services is a great option, which takes advantage of the power of SQL Server and RevoScaleR (Microsoft R Server). 
</span>
<span class="cig">The implementation with SQL Server R Services is a great option, which takes advantage of the power of SQL Server and RevoScaleR (Microsoft R Server). 
</span>

<p/>
All the steps can be executed on SQL Server client environment (such as SQL Server Management Studio). We provide a Windows PowerShell script, SQLR-Fraud-Detection.ps1, which invokes the SQL scripts and demonstrates the end-to-end modeling process.

## System Requirements
-----------------------

The following are required to run the scripts in this solution:
<ul>
<li>SQL Server 2016 with Microsoft R Server  (version 9.1.0 or later) installed and configured.  </li>   
<li>The SQL user name and password, and the user configured properly to execute R scripts in-memory.</li> 
<li>SQL Database which the user has write permission and execute stored procedures.</li> 
<li>For more information about SQL server 2016 and R service, please visit: <a href="https://msdn.microsoft.com/en-us/library/mt604847.aspx">https://msdn.microsoft.com/en-us/library/mt604847.aspx</a></li> 
</ul>


## Workflow Automation
-------------------

Follow the [PowerShell instructions](Powershell_Instructions.html) to execute all the scripts described below. [Click here](tables.html) to view the details of all tables created in this solution.

<a id="step0"/>

## Step 0: Data Preparation
-------------------

The following data are provided in the Data directory:

{% include data.md %}

In this step, we'll create four tables. The first three are: `Untagged_Transactions`, `Account_Info` and `Fraud`, corresponding to the three data sets in **Data** folder. Once tables have been created, data is uploaded to these tables using bcp command in the powershell script. The fourth table `Transaction_History` is created for storing historical transactions which will be used to calculate aggregates. This table will be filled in later steps.

### Input:

* untagged data: **untaggedTransactions.csv**
* fraud data: **fraudTransactions.csv**
* account data: **accountInfo.csv**

### Output:

* `Untagged_Transactions` table in SQL server
* `Fraud` table in SQL server
* `Account_Info` table in SQL server
* `Transaction_History` table in SQL server

<a id="step1"/>

## Step 1: Merging with Account Information
-------------------

In this step, we merge the `Untagged_Transactions` table with `Account_Info` table by `accountID` to get account information for each transaction. Before merging, we will create utility functions and table `Account_Info` will be sorted in descent order of `accountID` and `transactionDateTime`.

### Input:

* `Untagged_Transactions` table
* `Account_Info` table

### Output:

* `Untagged_Transactions_Acct` table

### Related Files: 

* **UtilityFunctions.sql**: Create utility functions to uniform transactionTime to 6 digit
* **SortAcctTable.sql**: Create SQL stored procedure named `sortAcctTable` to sort `Account_Info` table.
* **Step1_MergeAcctInfo.sql**: Create stored procedure named `MergeAcctInfo` to merge `Untagged_Transactions` table with `Account_Info` table.

<a id="step2"/>

## Step 2: Tagging
---------------

In this step, we tag the untagged data on account level based on the fraud data. The tagging logic is the following. In fraud data, we group it by account ID and sort by time, thus, we have the fraud time period for each fraud account. For each transaction in untagged data, if the account ID is not in fraud data, this transaction is labeled as non fraud (`label = 0`); if the account ID is in fraud data and the transaction time is within the fraud time period of this account, this transaction is labeled as fraud (`label = 1`); if the account ID is in fraud data and the transaction time is out of the fraud time period of this account, this transaction is labeled as pre-fraud or unknown (`label = 2`) which will be removed later. We will also perform re-formatting for some columns. For example, uniform the `transactionTime` filed to 6 digits. 

### Input:

* `Untagged_Transactions_Acct` table
* `Fraud` table

### Output:

* `Tagged` table

### Related Files:

* **Step2_Tagging.sql**: Create SQL stored procedure named `Tagging` to tag the data in account level.

<a id="step3"/>

## Step 3: Splitting Data
-------------------

In this step, we will hash accountID into 100 different hash code and split the whole data into training(70%) and testing(30%) based on the hash code, e.g., training = hash code <=70 and testing = hash code >70.

### Input: 

* `Tagged` table

### Output:

* `Tagged_Training` table
* `Tagged_Testing` table

### Related Files:

* **Step3_SplitData.sql**: Create SQL stored procedure named `SplitData` to split data into training and testing set.

<a id="step4"/>

## Step 4: Preprocessing
-------------------

In this step, we clean the tagged training data, i.e., filling missing values with 0 and removing transactions with invalid transaction time and amount. 

### Input:

* `Tagged_Training` table

### Output:

* `Tagged_Training_Processed` view

### Related Files:

* **Step4_Preprocess.sql**: Create SQL stored procedure named `Preprocess` to do preprocessing

<a id="step5"/>

## Step 5: Saving Transactions to Historical Table
-------------------

In this step, we save the transactions to `Transaction_History` table which will be used for calculating aggregates.

### Input:

* `Tagged_Training_Processed` view

### Output:

* `Transaction_History` table will be filled

### Related Files:

* **Step5_Save2History.sql**: Create SQL stored procedure named `Save2TransactionHistory` to save transactions to historical table. You may use the flag to control whether the historical table need to be truncated or not. e.g., `Exec Save2TransactionHistory 'Tagged_Training_Processed' ,'1'` means truncating historical table and saving transactions from table `Tagged_Training_Processed` to historical table.

<a id="step6"/>

## Step 6: Create Risk Tables
-------------------

In this step, we create risk tables for bunch of categorical variables, such as location related variables. This is related to the method called "weight of evidence". 

{% include risk_summary.md %}

### Input:

* `Tagged_Training_Processed` view

### Output:

* `Risk_Var` table: a table stores the name of variables to be converted and the name of risk tables
* `Risk_xxx` tables: risk tables for variable xxx.

### Related Files:

* **CreateRiskTable.sql**: Create SQL stored procedure named `CreateRiskTable` to generate one certain risk table for a certain variable.
* **Step6_CreateRiskTables.sql**: Create SQL stored procedure named `CreateRiskTable_ForAll` to generate risk tables for all required variables.

<a id="step7"/>

## Step 7: Feature Engineering
-------------------

This step does feature engineering to training data set. We will generate three groups of new features:

* Binary variables. For example, address mismatch flags.
* Numerical risk variables transformed from categorical variables based on the risk tables.
* Aggregates. For example, completed transactions of a customer in past 30 days.

### Input:

* `Tagged_Training_Processed` view
* `Risk_Var` table
* `Risk_xxx` tables

### Output:

* `Tagged_Training_Processed_Features` view

### Related Files:

* **Step7_FeatureEngineer.sql**: Create SQL stored procedure named `FeatureEngineer` to do feature engineering.

<a id="step8"/>

## Step 8: Model Training
-------------------

In this step, we train a gradient boosting tree model with the training data set.

### Input:

* `Tagged_Training_Processed_Features` view

### Output:

* `Trained_Model` table: stores the trained model object

### Related Files:

* **Step8_Training.sql**: Create SQL stored procedure named `TrainModelR` to train gradient boosting tree model. 

<a id="step9"/>

## Step 9: Batch Scoring
-------------------

In this step we will do the batch scoring on testing data set including

*  Merging with `accountInfo` table if account information doesn't exist
*  Preprocessing
*  Feature engineering 
*  Scoring based on the trained model in last step

### Input:

* `Trained_Model` table
* `Tagged_Testing` table

### Output:

* `Predict_Score` table: table stores the predicted scores.

### Related Files:

* **Step9_Prediction.sql**: Create SQL stored procedure named `PredictR` to do merging, preprocessing, feature engineering and scoring for new coming transactions.

<a id="step10"/>

## Step 10: Evaluation
-------------------

This step evaluates the performance on both account level and transaction level.

### Input:

* `Predict_Score` table

### Output:

* `Performance` table: stores metrics on account level.
* `Performance_Auc` table: stores metrics on transaction level: AUC of ROC curve. 

### Related Files:

* **Step10A_Evaluation.sql**: Create SQL stored procedure named `EvaluateR` to evaluate performance on account level. 
* **Step10B_Evaluation_AUC.sql**: Create SQL stored procedure named `EvaluateR_auc` to evaluate performance on transaction level.

<a id="step11"/>

## Step 11: Production Scoring
-------------------

In this step, we showcase how to score one raw transaction to mimic the real scoring case.  This procedure will be called from our example website when a transaction occurs.  See [Typical Workflow](Typical.html#step5) for more information.

### Input:

* One hard coded raw transaction


### Output:

* `Predict_Score_Single_Transaction` table: table stores the score of the new input transaction above.