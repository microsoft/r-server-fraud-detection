---
layout: default
title: For the Data Scientist
---
<div class="alert alert-success" role="alert"> This page describes the 
<strong>
<span class="cig">{{ site.cig_text }}</span>
<span class="onp">{{ site.onp_text }}</span>
<span class="hdi">{{ site.hdi_text }}</span> 
</strong>
solution.
{% include choices.md %}
</div> 

## For the Data Scientist - Develop with R
----------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
            <li><a href="#intro">Fraud Detection</a></li>
            <li><a href="#step0" class="hdi">Step 0: Create Intermediate Directories</a></li>
            <li><a href="#step1">Step 1: Tagging</a></li>
            <li><a href="#step2">Step 2: Splitting and Preprocessing</a></li>
            <li><a href="#step3">Step 3: Feature Engineering</a></li>
            <li><a href="#step4">Step 4: Training, Testing and Evaluating</a></li>
            <li class="sql"><a href="#requirements">System Requirements</a></li>
            <li><a href="#template-contents">Template Contents</a></li>
        </div>
    </div>
    <div class="col-md-6">

        <div class="onp">
        For businesses that prefer an on-prem solution, the implementation with SQL Server R Services is a great option, which takes advantage of the power of SQL Server and RevoScaleR (Microsoft R Server).
        </div> 
        <div class="cig">
        This implementation on Azure SQL Server R Services is a great option which takes advantage of the power of SQL Server and RevoScaleR (Microsoft R Server). 
        </div>
        <div class="hdi">
        HDInsight is a cloud Spark and Hadoop service for the enterprise.  HDInsight is also the only managed cloud Hadoop solution with integration to Microsoft R Server.
        <p></p>
        This solution shows how to pre-process data (cleaning and feature engineering), train prediction models, and perform scoring on an HDInsight Spark cluster with Microsoft R Server. 
        </div>   
    </div>
</div>
<div class="sql">
<p></p>
In this template, we implemented all steps in SQL stored procedures: data preprocessing, and feature engineering are implemented in pure SQL, while data cleaning, and the model training, scoring and evaluation steps are implemented with SQL stored procedures calling R (Microsoft R Server) code. 
<p></p>
</div>

<div class="sql">
Data scientists who are testing and developing solutions can work from the convenience of their R IDE on their client machine, while <a href="https://msdn.microsoft.com/en-us/library/mt604885.aspx">setting the computation context to SQL</a> (see <strong>R</strong> folder for code).  They can also deploy the completed solutions to SQL Server 2016 by embedding calls to R in stored procedures (see <strong>SQLR</strong> folder for code). These solutions can then be further automated by the use of SQL Server Integration Services and SQL Server agent: a PowerShell script (.ps1 file) automates the running of the SQL code.
</div>
<div class="hdi">
Data scientists who are testing and developing solutions can work from the browser-based Open Source Edition of RStudio Server on the HDInsight Spark cluster edge node, while <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-r-server-compute-contexts">using a compute context</a> to control whether computation will be performed locally on the edge node, or whether it will be distributed across the nodes in the HDInsight Spark cluster. 
</div>

<a name="intro">
## {{ site.solution_name }}
--------------------------

Fraudulent online transactions are becoming more and more of a problem to online merchants. This solution will show how to build a model to detect potential fraudulent transactions so that the transaction may be rejected. This implementation with SQL Server R Services is equivalent to the [Azure ML template for Online Fraud Detection](https://gallery.cortanaanalytics.com/Collection/Online-Fraud-Detection-Template-1).


<p></p>
View [more information about the data](input_data.html).

<div class="sql">
<p></p>
In this solution, the final scored database table <code>Scores</code> is created in SQL Server.  This data is then visualized in PowerBI. 
<p></p>
</div>
<div class="hdi">
<p></p>
In this solution, an Apache Hive table will be created to show predicted scores. This data is then visualized in PowerBI. 
<p></p>
</div>

To try this out yourself, visit the [Quick Start](START_HERE.html) page.  

Below is a description of what happens in each of the steps: data preparation, feature engineering, model development, prediction, and deployment in more detail.

<div class="sql">
The file <strong>modeling_main.R</strong> enables the user to define the input and call all the steps. Inputs are: paths to the raw data files, database name, server name, username and password.

The database is created if it does not not already exist, and the connection string as well as the SQL compute context are defined.
</div>



<p><a name="step1"></a></p>

<h2>Step 1: Tagging</h2>
<hr />
<div class="sql">
<p>In this step, the raw data is loaded into SQL in three tables called <code>Untagged_Transactions</code>, <code>Account_Info</code>, and <code>Fraud_Transactions</code>. The date time variable <code>transactionDateTime</code> is created during this upload.</p>

<p>After sorting the table <code>Account_Info</code> into <code>Account_Info_Sort</code> in descendent order of <code>accountID</code>, <code>transactionDateTime</code>, we merge the two tables <code>Untagged_Transactions</code> and <code>Account_Info_Sort</code> into <code>Untagged_Transactions_Account</code>. (SQL queries are used here instead of the <code>rxMerge</code> function of RevoScaleR because it is not yet available for SQL data sources.) We then remove duplicate observations with another SQL query executed through <code>rxExecuteSQLddl</code>.</p>

<p>Finally, we create labels for the untagged transactions by using the Fraud table. This is done by:</p>

<ol>
  <li>
    <p>Aggregating the <code>Fraud</code> table on the account level, creating a start and end datetime.</p>
  </li>
  <li>
    <p>Joining this data with the <code>Untagged_Transactions_Account</code> data with a left join. Start and end time are the NULL for non fraud.</p>
  </li>
  <li>
    <p>Tagging the data: <code>0</code> for non fraud, <code>1</code> for fraud, <code>2</code> for pre-fraud.</p>
  </li>
</ol>

<p>The tagging logic is the following: the <code>Fraud</code> data is grouped by account ID and sorted by time, thus, we have the fraud time period for each fraud account. For each transaction in the untagged data:</p>

<ul>
  <li>if the account ID is not in the fraud data, this transaction is labeled as non fraud (<code>label = 0</code>).</li>
  <li>if the account ID is in the fraud data and the transaction time is within the fraud time period of this account, this transaction is labeled as fraud (<code>label = 1</code>).</li>
  <li>if the account ID is in the fraud data and the transaction time is out of the fraud time period of this account, this transaction is labeled as pre-fraud or unknown (<code>label = 2</code>) which will be removed later.</li>
</ul>

<h3>Input:</h3>
<ul>
  <li>Raw data: <strong>untaggedTransactions.csv</strong>, <strong>accountInfo.csv</strong>, and <strong>fraudTransactions.csv</strong>.</li>
</ul>

<h3>Output:</h3>
<ul>
  <li><code>Tagged</code> SQL table.</li>
</ul>
<h3>Related files:</h3>
<ul>
  <li><strong>step1_tagging.R</strong></li>
</ul>
</div>

<div class="hdi">
</div>



<p><a name="step2"></a></p>

<h2>Step 2: Splitting and Preprocessing</h2>
<hr />

<p>Because feature engineering will require us to compute risk values, and in order to avoid label leakage, the risk values should be computed on a training set. This is why splitting the Tagged data into a training and a testing set is performed before the feature engineering step.</p>

<p>This is done by selecting randomly 70% of <code>accountID</code> to be part of the training set. In order to ensure repeatability and to make sure that the same <code>accountID</code> ends up in the same data set, <code>accountID</code> values are mapped to integers through a hash function, with the mapping and <code>accountID</code> written to the <code>Hash_Id</code> SQL table. We create the <code>Hash_Id</code> table though a SQL query in order to use the same hash function as the SQL Stored Procedures for coherence.</p>

<p>We then create a pointer to the training set, which, at the same time, removes the pre-fraud labels (<code>label = 2</code>), variables not used in the next steps, observations with ID variables missing, and observations where the transaction amount in USD is negative.</p>

<p>After creating this pointer, we apply the <code>clean_preprocess</code> function on it. After detecting the variables with missing values by using the <code>rxSummary</code> function, it wraps the function <code>preprocessing</code> into <code>rxDataStep</code>, and acts on the data as following:</p>

<ul>
  <li>It replaces the missing observations with 0 (or -99 for the variable localHour since 0 already represents a valid value for this variable).</li>
  <li>It fixes some data entries.</li>
  <li>It converts a few variables to numeric to ensure correct computations in the following steps.</li>
</ul>

<p>This function will later be applied to the testing set as well.</p>


<h3>Input:</h3>
<ul>
  <li><code>Tagged</code> SQL table.</li>
</ul>

<h3>Output:</h3>

<ul>
  <li><code>Tagged_Training_Processed</code> SQL table containing the cleaned and preprocessed training set.</li>
  <li><code>Hash_Id</code> SQL table containing the <code>accountID</code> and the mapping through the hash function.</li>
</ul>

<h3>Related files:</h3>

<ul>
  <li><strong>step2_splitting_preprocessing.R</strong></li>
</ul>

<p><a name="step3"></a></p>

<h2>Step 3: Feature Engineering</h2>
<hr />

<p>For feature engineering, we want to design new features:</p>

<ul>
  <li>Risk values for various numeric variables.</li>
  <li>Flags for mismatches between addresses, and flags for high amount transactions.</li>
  <li>Aggregates corresponding to the number and amount of transactions in the past day and 30 days for every transaction per accountID.</li>
</ul>

<p>We first compute the risk values, using the training set, with the function <code>create_risk_table</code>. For a given variable of interest, it uses <code>rxSummary</code> to get the proportion of fraudulent and non-fraudulent transactions for every level of that variable. 
The risk value for a level of the variable will be the log of a smoothed odd fraud rate. The risks are written to SQL tables to be used in feature engineering on the training and testing sets or for batch scoring.</p>

<p>Then, the function <code>assign_risk_and_flags</code> will apply the function <code>assign_risk</code>, wrapped into <code>rxDataStep</code> on the training set. This assigns the risk values for every variable of interest, using the previously created Risk tables. At the same time, <code>rxDataStep</code> creates the address mismatch flags and a flag for high amounts. The output is written to SQL Server in the table <code>Tagged_Training_Processed_Features1</code>. This function will later be applied to the testing set as well.</p>

<p>Finally, we create the aggregates with the function <code>compute_aggregates</code>. They correspond to the number and USD amount of transactions in the past day and 30 days for every transaction per accountID. Although a SQL query can be used, we used R code here for illustrative purposes. The computation follows a standard split-apply-combine process.</p>

<ol>
  <li>Load the intermediate data set Tagged_Training_Processed_Features1 in memory.</li>
  <li>Split the data set into a list of data frames for each accountID.</li>
  <li>Compute aggregates for each accountID with the function <code>aggregates_account_level</code>.</li>
  <li>Combine the results, use zero values when no aggregates, and write the result back to SQL Server.</li>
</ol>

<p>On an accountID level, <code>aggregates_account_level</code> works as follows on a given data frame of transactions corresponding to an <code>accountID</code>:</p>

<ul>
  <li>Perform a cross-apply of the data frame on itself, while only keeping for each <code>transactionID</code>, all the other transactions that occurred in the past 30 days.</li>
  <li>Split the table in 2:</li>
    <ul>
      <li> <code>z1day</code> will conatain the transactions and their corresponding transactions that happened in the past 1 day.</li>
      <li> <code>z30day</code> will conatain the transactions and their corresponding transactions that happened in the past 30 days.</li>
    </ul>
  <li>For each transaction in each of <code>z1day</code> and <code>z30day</code>, we compute the number and total USD amount of the previous transactions.</li>
  <li>The aggregated result is returned as a data frame and is the output of the <code>aggregates_account_level</code> function.</li>
</ul>

<p>The function <code>compute_aggregates</code> will later be used on the testing set as well.</p>

<p>The final data is written to the SQL table <code>Tagged_Training_Processed_Features</code>. Using <code>stringsAsFactors = TRUE</code>, we convert the character variables to factors and get their levels information in the <code>column_info</code> list, to be used as well for the testing set.</p>

<h3>Input:</h3>
<ul>
  <li><code>Tagged_Training_Processed</code> SQL table.</li>
</ul>

<h3>Output:</h3>

<ul>
  <li><code>Tagged_Training_Processed_Features</code> SQL table containing new features.</li>
  <li>Various Risk SQL tables containing the risk values for each level of the variables.</li>
  <li><code>column_info</code> list to be used on the training and testing sets to specify the types of variables and levels of the factors in particular.</li>
</ul>

<h3>Related files:</h3>

<ul>
  <li><strong>step3_feature_engineering.R</strong></li>
</ul>

<p><a name="step4"></a></p>

<h2>Step 4: Training, Testing and Evaluating</h2>
<hr />

<p>After pointing to the training set with the correct variable types (using <code>column_info</code>), we write the formula to be used for the classification.
We build a gradient boosted trees (GBT) model with the <code>rxFastTrees</code> algorithm from the <code>MicrosoftML</code> library. The argument <code>unbalancedSets = TRUE</code> helps deal with the class imbalance that is observed in this data set.
The trained model is serialized and uploaded to a SQL table <code>Models</code> if needed later, through an Odbc connection.</p>

<p>We then point with a query to the raw testing set, and using the previously defined functions <code>clean_preprocess</code>, <code>assign_risk_and_flags</code>, and <code>compute_aggregates</code>, we get the testing set <code>Tagged_Testing_Processed_Features</code> ready for scoring.</p>

<p>Finally, we compute predictions on the testing set, written to the SQL table <code>Predict_Scores</code>. It is uploaded in memory, and various performance metrics are computed.</p>

<ul>
  <li>
    <p><strong>AUC</strong> (Area Under the Curve) for the ROC. This represents how well the model can differenciate between the non-fraudulent transactions and the fraudulent ones given a good decision threshold in the testing set. We draw the ROC, representing the true positive rate in function of the false positive rate for various possible cutoffs.</p>
  </li>
  <li>
    <p>Various account level metrics and graphs.</p>
  </li>
</ul>


<h3>Input:</h3>
<ul>
  <li><code>Tagged_Training_Processed_Features</code> SQL table containing new features and preprocessed training set.</li>
  <li><code>Hash_Id</code> SQL table containing the <code>accountID</code> and the mapping through the hash function.</li>
  <li>Various Risk SQL tables containing the risk values for each level of the variables, to be used for feature engineering on the testing set.</li>
  <li><code>column_info</code> list to be used on the training and testing sets to specify the types of variables and levels of the factors in particular.</li>
</ul>

<h3>Output:</h3>

<ul>
  <li><code>Models</code> SQL table containing the serialized GBT model.</li>
  <li><code>Tagged_Testing_Processed_Features</code> SQL table containing new features and preprocessed testing set.</li>
  <li><code>Predict_Score</code> SQL table containing the predictions made on the testing set.</li>
  <li>Performance metrics and graphs.</li>
</ul>

<h3>Related files:</h3>

<ul>
  <li><strong>step4_training_evaluation.R</strong></li>
</ul>

<div class="hdi">
<h2 id="update">Updating the Production Stage Directory (“Copy Dev to Prod”)</h2>
<hr />
<p>At the end of the main function of the script <strong>development_main.R</strong>, the <strong>copy_dev_to_prod.R</strong> script is invoked in order to copy (overwrite if it already exists) the model, statistics and other data from the Development Stage to a directory of the Production or Web Scoring stage.</p>

<p>If you do not wish to overwrite the model currently in use in a Production stage, you can either save them to a different directory, or set <code>update_prod_flag</code> to <code>0</code> inside the main function.</p>
</div>

<a id="production"/>
<h2>Production Stage</h2>
<hr />
<div class="sql">
The R code from each of the above steps is operationalized in the SQL Server as stored procedures.
See <a href="">For the Database Analyst</a> for more details.

</div>



<p><a name="viz"></a></p>
<h2>Visualize Results</h2>
<hr />
<div class="sql">
The final scores for the test data reside in the table <code>Predict_Score</code> of the <code>Fraud</code> database. The test data itself is in the <code>Tagged_Testing</code> table.  The next step of this solution is to visualize both tables in PowerBI. 
</div>
<div class="hdi">
The final scores for the test data reside in the Hive table <code>Predict_Score</code>. The test data itself is in the <code>Tagged_Testing</code> table.  The next step of this solution is to visualize both tables in PowerBI. 
</div>

<p></p>
<ul>
  <li>See <a href="business-manager.html">For the Business Manager</a> for details of the PowerBI dashboard.</li>
</ul>

<div id="requirements" class="sql">
<h2> System Requirements</h2>

The following are required to run the scripts in this solution:
<ul>
<li>SQL Server 2016 with Microsoft R Server  (version 9.0.1 or later) installed and configured.  </li>   
<li>The SQL user name and password, and the user configured properly to execute R scripts in-memory.</li> 
<li>SQL Database which the user has write permission and execute stored procedures.</li> 
<li>For more information about SQL server 2016 and R service, please visit: <a href="https://msdn.microsoft.com/en-us/library/mt604847.aspx">https://msdn.microsoft.com/en-us/library/mt604847.aspx</a></li> 
</ul>
</div>

<h2 id="template-contents">Template Contents</h2>
<hr />

<ul>
  <li><a href="contents.html">View the contents of this solution template</a>.</li>
</ul>

<p>To try this out yourself:</p>

<ul>
  <li>View the <a href="START_HERE.html">Quick Start</a>.</li>
</ul>

<p><a href="index.html">&lt; Home</a></p>