#Online Fraud Detection Template implemented on SQL Server R Service
--------------------------
 * **Introduction**
	* **System Requirements**
	* **Workflow Automation**
 * **Step 0: Data Preparation**
 * **Step 1: Merging with Account Information**
 * **Step 2: Tagging**
 * **Step 3: Splitting Data**
 * **Step 4: Preprocessing**
 * **Step 5: Saving Transactions to Historical Table**
 * **Step 6: Create Risk Tables**
 * **Step 7: Feature Engineering**
 * **Step 8: Model Training**
 * **Step 9: Batch Scoring**
 * **Step 10: Evaluation**
 * **Step 11: Production Scoring**

### Introduction:
-------------------------

Fraud detection is an important machine learning application. In this template, the online purchase transaction fraud detection scenario (for the online merchants, detecting whether a transaction is made by the original owner of payment instrument) is used as an example. This on-prem implementation with SQL Server R Servicds is equivalent to the [Azure ML template for Online Fraud Detection](https://gallery.cortanaanalytics.com/Collection/Online-Fraud-Detection-Template-1).

For customers that prefers and on-prem solution, the implementation with SQL Server R Services is a great option, which takes advantage of the power of SQL Server and Microsoft R Server. In this template, we implemented all steps in SQL stored procedures, where data splitting, data pre-processing and feature engineering are implemented in pure SQL, while the model training, scoring and evaluation steps are implemented with SQL stored procedures embedding with R (Microsoft R Server) code. 

All the steps can be executed on SQL Server client environment (such as SQL Server Management Studio), as well as from other applications. We provide a Windows PowerShell script which invokes the SQL scripts and demonstrate the end-to-end modeling process.

### System Requirements
-----------------------

To run the scripts, it requires the following:

 * SQL server (2016 or higher) with Microsoft R server installed and configured;
 * The SQL user name and password, and the user is configured properly to execute R scripts in-memory;
 * SQL Database which the user has write permission and execute stored procedures;
 * For more information about SQL server 2017 and R service, please visit: https://msdn.microsoft.com/en-us/library/mt604847.aspx

### Workflow Automation
-------------------

We provide a Windows PowerShell script to demonstrate the end-to-end workflow. To learn how to run the script, open a PowerShell command prompt, navigate to the directory storing the powershell script and type:

                Get-Help .\OnlineFraudDetection.ps1

To invoke the PowerShell script, type:

                .\OnlineFraudDetection.ps1 -ServerName "Server Name" -DBName "Database Name" -username "User Name" -password "Password"
                
Then, all steps will be executed automatically.

### Step 0: Data Preparation

The following data are provided in the Data directory:

<table style="width:85%">
  <tr>
    <th>File</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>.\Data\Account_Info.csv</td>
    <td>Account information </td>
  </tr>
  <tr>
    <td>.\Data\Fraud_Transactions.csv</td>
    <td>Raw fraud transaction data</td>
  </tr>
  <tr>
    <td>.\Data\Untagged_Transactions.csv</td>
    <td>Raw untagged transaction data without fraud tag</td>
  </tr>
</table>

In this step, we'll create four tables. The first three are: `Untagged\_Transactions`, `Account\_Info` and `Fraud`, corresponding to the three data sets in **Data** folder. Once tables have been created, data is uploaded to these tables using bcp command in the powershell script. The fourth table `Transaction\_History` is created for storing historical transactions which will be used to calculate aggregates. This table will be filled in later steps.

Input:

* untagged data: **Untagged_Transactions.csv**
* fraud data: **Fraud_Transactions.csv**
* account data: **Account_Info.csv**

Output:

* `Untagged\_Transactions` table in SQL server
* `Fraud` table in SQL server
* `Account\_Info` table in SQL server
* `Transaction\_History` table in SQL server

### Step 1: Merging with Account Information
In this step, we merge the `Untagged\_Transactions` table with `Account\_Info` table by `accountID` to get account information for each transaction. Before merging, we will create utility functions and table `Account\_Info` will be sorted in descent order of `accountID` and `transactionDateTime`.

Input:

* `Untagged\_Transactions` table
* `Account\_Info` table

Output:

* `Untagged\_Transactions\_Acct` table

Related files: 

* **UtilityFunctions.sql**: Create utility functions to uniform transactionTime to 6 digit
* **SortAcctTable.sql**: Create SQL stored procedure named `sortAcctTable` to sort `Account\_Info` table.
* **Step1_MergeAcctInfo.sql**: Create stored procedure named `MergeAcctInfo` to merge `Untagged\_Transactions` table with `Account\_Info` table.

### Step 2: Tagging
In this step, we tag the untagged data on account level based on the fraud data. The tagging logic is the following. In fraud data, we group it by account ID and sort by time, thus, we have the fraud time period for each fraud account. For each transaction in untagged data, if the account ID is not in fraud data, this transaction is labeled as non fraud (`label = 0`); if the account ID is in fraud data and the transaction time is within the fraud time period of this account, this transaction is labeled as fraud (1label = 11); if the account ID is in fraud data and the transaction time is out of the fraud time period of this account, this transaction is labeled as pre-fraud or unknown (`label = 2`) which will be removed later. We will also perform re-formatting for some columns. For example, uniform the `transactionTime` filed to 6 digits. 

Input:

* `Untagged\_Transactions\_Acct` table
* `Fraud` table

Output:

* `Tagged` table

Related files:

* **Step2_Tagging.sql**: Create SQL stored procedure named `Tagging` to tag the data in account level.

### Step 3: Splitting Data
In this step, we will hash accountID into 100 different hash code and split the whole data into training(70%) and testing(30%) based on the hash code, e.g., training = hash code <=70 and testing = hash code >70.

Input: 

* `Tagged` table

Output:

* `Tagged_Training` table
* `Tagged_Testing` table

Related files:

* **Step3_SplitData.sql**: Create SQL stored procedure named `SplitData` to split data into training and testing set.

### Step 4: Preprocessing
In this step, we clean the tagged training data, i.e., filling missing values with 0 and removing transactions with invalid transaction time and amount. 

Input:

* `Tagged\_Training` table

Output:

* `Tagged\_Training\_Processed` view

Related files:

* **Step4_Preprocess.sql**: Create SQL stored procedure named `Preprocess` to do preprocessing

### Step 5: Saving Transactions to Historical Table
In this step, we save the transactions to `Transaction\_History` table which will be used for calculating aggregates.

Input:

* `Tagged\_Training\_Processed` view

Output:

* `Transaction\_History` table will be filled

Related files:

* **Step5\_Save2History.sql**: Create SQL stored procedure named `Save2TransactionHistory` to save transactions to historical table. You may use the flag to control whether the historical table need to be truncated or not. e.g., `Exec Save2TransactionHistory 'Tagged\_Training\_Processed' ,'1'` means truncating historical table and saving transactions from table `Tagged\_Training\_Processed` to historical table.

### Step 6: Create Risk Tables
In this step, we create risk tables for bunch of categorical variables, such as location related variables. This is related to the method called "weight of evidence". The risk table stores risk (log of smoothed odds ratio) for each level of one categorical variable. For example, variable `X` has two levels: `A`` and `B`. For each level (e.g., `A`), we compute the following:

* Total number of good transactions, `n\_good(A)`, 
* Total number of bad transactions, `n\_bad(A)`. 
* The smoothed odds, `odds(A) = (n\_bad(A)+10)/(n\_bad(A)+n\_good(A)+100)`. 
* The the risk of level `A`, `risk(A) = log(odds(A)/(1-odds(A))`. 

Thus, the risk table of variable `X` looks like the following:

<table class="table table-condensed">
  <tr>
    <th>X</th>
    <th>Risk</th>
  </tr>
  <tr>
    <td>A</td>
    <td>Risk(A)</td>
  </tr>
  <tr>
    <td>B</td>
    <td>Risk(B)</td>
  </tr>
</table>

With the risk table, we can assign the risk value to each level. This is how we transform the categorical variable into numerical variable. 

Input:

* "Tagged\_Training\_Processed" view

Output:

* "Risk\_Var" table: a table stores the name of variables to be converted and the name of risk tables
* "Risk\_xxx" tables: risk tables for variable xxx.

Related files:

* CreateRiskTable.sql: Create SQL stored procedure named "CreateRiskTable" to generate one certain risk table for a certain variable.
* Step6\_CreateRiskTables.sql: Create SQL stored procedure named "CreateRiskTable_ForAll" to generate risk tables for all required variables.

### Step 7: Feature Engineering
This step does feature engineering to training data set. We will generate three groups of new features:

* Binary variables. For example, address mismatch flags.
* Numerical risk variables transformed from categorical variables based on the risk tables.
* Aggregates. For example, completed transactions of a customer in past 30 days.

Input:

* "Tagged\_Training\_Processed" view
* "Risk\_Var" table
* "Risk\_xxx" tables

Output:

* "Tagged\_Training\_Processed\_Features" view

Related files:

* Step7\_FeatureEngineer.sql: Create SQL stored procedure named "FeatureEngineer" to do feature engineering.


### Step 8: Model Training
In this step, we train a gradient boosting tree model with the training data set.

Input:

* "Tagged\_Training\_Processed\_Features" view

Output:

* "Trained\_Model" table: stores the trained model object

Related files:

* Step8_Training.sql: Create SQL stored procedure named "TrainModelR" to train gradient boosting tree model. 

### Step 9: Batch Scoring
In this step we will do the batch scoring on testing data set including

*  Merging with accountInfo table if account information doesn't exist
*  Preprocessing
*  Feature engineering 
*  Scoring based on the trained model in last step

Input:

* "Trained\_Model" table
* "Tagged\_Testing" table

Output:

* "Predict\_Score" table: table stores the predicted scores.

Related files:

* Step9\_Prediction.sql: Create SQL stored procedure named "PredictR" to do merging, preprocessing, feature engineering and scoring for new coming transactions.


### Step 10: Evaluation
This step evaluates the performance on both account level and transaction level.

Input:

* "Predict\_Score" table

Output:

* "Performance" table: stores metrics on account level.
* "Performance\_Auc" table: stores metrics on transaction level: AUC of ROC curve. 

Related files:

* Step10A\_Evaluation.sql: Create SQL stored procedure named "EvaluateR" to evaluate performance on account level. 
* Step10B\_Evaluation_AUC.sql: Create SQL stored procedure named "EvaluateR\_auc" to evaluate performance on transaction level.

### STEP 11: Production Scoring

In this step, we show case how to score one raw transaction, mimic the real scoring case.  

Input:

* One hard coded raw transaction


Output:

* "Predict\_Score\_Single\_Transaction" table: table stores the score of the new input transaction above.