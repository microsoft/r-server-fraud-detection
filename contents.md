---
layout: default
title: Template Contents
---

## Template Contents
--------------------

The following is the directory structure for this template:

- [**Data**](#copy-of-input-datasets)  This contains the copy of the simulated input data with 100K unique customers. 
- [**R**](#model-development-in-r)  This contains the R code to simulate the input datasets, pre-process them, create the analytical datasets, train the models, identify the champion model and provide recommendations.
- [**Resources**](#resources-for-the-solution-packet) This directory contains other resources for the solution package.
- [**SQLR**](#operationalize-in-sql-2016) This contains T-SQL code to pre-process the datasets, train the models, identify the champion model and provide recommendations. It also contains a PowerShell script to automate the entire process, including loading the data into the database (not included in the T-SQL code).
- [**RSparkCluster**](#hdinsight-solution-on-spark-cluster) This contains the R code to pre-process the datasets, train the models, identify the champion model and provide recommendations on a Spark cluster. 

In this template with SQL Server R Services, two versions of the SQL implementation, and another version for HDInsight implementation:

1. [**Model Development in R IDE**](#model-development-in-r). Run the R code in R IDE (e.g., RStudio, R Tools for Visual Studio).

2. [**Operationalize in SQL**](#operationalize-in-sql-2016). Run the SQL code in SQL Server using SQLR scripts from SSMS or from the PowerShell script.

3. [**HDInsight Solution on Spark Cluster**](#hdinsight-solution-on-spark-cluster).  Run this R code in RStudio on the edge node of the Spark cluster.


### Copy of Input Datasets
----------------------------

{% include data.md %}

###  Model Development in R
-------------------------
These files  in the **R** directory for the SQL solution.  

<table class="table table-striped table-condensed">
<tr><th> File </th><th> Description </th></tr>
<tr><td>FraudDetection.rproj  </td><td>Project file for RStudio or Visual Studio</td></tr>
<tr><td>FraudDetection.rxproj  </td><td>Used with the Visual Studio Solution File</td></tr>
<tr><td>FraudDetection.sln  </td><td>Visual Studio Solution File</td></tr>
<tr><td>{{ site.jupyter_name }}  </td><td> Contains the Jupyter Notebook file that runs all the .R scripts </td></tr>
<tr>
<tr><td> modeling_main.R </td><td> Defines parameters and sources the different scripts for the Development Stage</td></tr>
<tr><td> step1_tagging.R </td><td>Tags transactions on account level  </td></tr>
<tr><td> step2_splitting_preprocessing.R </td><td> Splits the tagged data set into a Training and a Testing set, cleans the training set and performs  preprocessing</td></tr>
<tr><td> step3_feature_engineering.R </td><td> Performs feature engineering  </td></tr>
<tr><td> step4_training_evaluation.R </td><td> Trains a boosted tree classification model on the training set, scores and evaluates on testing set </td></tr>
</table> 


* See [For the Data Scientist](data-scientist.html?path=cig) for more details about these files.
* See [Typical Workflow](Typical.html?path=cig)  for more information about executing these scripts.

### Operationalize in SQL Server 2016 
-------------------------------------------------------

These files are in the **SQLR** directory.

<table class="table table-striped table-condensed">

<tr><th> File </th><th> Description </th></tr>
<tr><td>CreateRiskTable.sql </td><td>Stored procedure to create risk table for each input variable   </td></tr>
<tr><td>.\OnlineFraudDetection.ps1  </td><td>Automates execution of all .sql files and creates stored procedures  </td></tr>
<tr><td>ParseString.sql   </td><td> Stored procedure to parse a string and to a sql table  </td></tr>
<tr><td>example_user.sql  </td><td>Used during initial SQL Server setup to create the user and password and grant permissions </td></tr>
<tr><td>ScoreOneTrans.sql  </td><td> Stored procedure to score one transaction   </td></tr>
<tr><td>SortAcctTable.sql   </td><td> Stored procedure to create recordDateTime column for Account_Info table and sort the table  </td></tr>
<tr><td> Step0_CreateTables.sql  </td><td> Creates initial tables from .csv files  </td></tr>
<tr><td> Step10A_Evaluation.sql  </td><td> Stored procedure to generate fraud account level metrics  </td></tr>
<tr><td> Step10B_Evaluation_AUC.sql  </td><td> Stored procedure to calculate AUC  </td></tr>
<tr><td> Step1_MergeAcctInfo.sql  </td><td> Stored procedure to merge untagged transactions with account level infomation  </td></tr>
<tr><td> Step2_Tagging.sql  </td><td> Stored procedure to tag transactions on account level  </td></tr>
<tr><td> Step3_SplitData.sql  </td><td> Stored procedure to split data on account level   </td></tr>
<tr><td> Step4_Preprocess.sql  </td><td> Stored procedure to clean data and remove prefraud transactions   </td></tr>
<tr><td> Step5_Save2History.sql  </td><td> Stored procedure to save transactions to historical table   </td></tr>
<tr><td> Step6_CreateRiskTables.sql  </td><td> Stored procedure to create all risk tables   </td></tr>
<tr><td> Step7_FeatureEngineer.sql  </td><td> Stored procedure to perform feature engineering  </td></tr>
<tr><td> Step8_Training.sql  </td><td> Stored procedure to train and save a gradient boosted tree model  </td></tr>
<tr><td> Step9_Prediction.sql  </td><td> Stored procedure to score and save results to a sql table  </td></tr>
<tr><td> UtilityFunctions.sql  </td><td> Creates functions which will be used  </td></tr>
<tr><td>   </td><td>   </td></tr>

</table>

* See [ For the Database Analyst](dba.html?path=cig) for more information about these files.
* Follow the [PowerShell Instructions](Powershell_Instructions.html?path=cig) to execute the PowerShell script which automates the running of all these .sql files.



### HDInsight Solution on Spark Cluster
------------------------------------
These files are in the **RSparkCluster** directory.

<table class="table table-striped table-condensed">
<tr><th> File </th><th> Description </th></tr> 
<tr><td> copy_dev_to_prod.R</td><td>Defines function, copy_to_prod, used in development_main.R </td></tr>
<tr><td> data_generation.R</td><td>Used to generate data, used in development_main.R</td></tr>
<tr><td> deployment_main.R</td><td>Deploys web scoring function as a web service</td></tr>
<tr><td> in_memory_scoring.R</td><td>Performs in-memory scoring for batch scoring or for scoring remotely with a web service  </td></tr>
<tr><td> production_main.R</td><td> Scores new data using subset of development steps</td></tr>
<tr><td> step0_directories_creation.R</td><td>Creates initial directories</td></tr>
<tr><td> step1_merge_account_info.R</td><td>Merges the two tables Untagged_Transaction and Account_Info</td></tr>
<tr><td> step2_tagging.R</td><td>Tags transactions on account level  </td></tr>
<tr><td> step3_splitting.R</td><td>Splits the tagged data set into a Training and a Testing set </td></tr>
<tr><td> step4_preprocessing.R</td><td> Performs preprocessing on an input data </td></tr>
<tr><td> step5_create_risk_tables.R</td><td> Creates the risk tables for various character variables </td></tr>
<tr><td> step6_feature_engineering.R </td><td>Performs feature engineering </td></tr>
<tr><td> step7_training.R </td><td> Trains a gradient boosted trees (GBT) model on input data  </td></tr>
<tr><td> step8_prediction.R </td><td> Performs batch scoring and evaluation </td></tr>
<tr><td> step9_evaluation.R</td><td> Performs evaluation on a scored data set </td></tr>
<tr><td> web_scoring_main.R</td><td> Defines and publishes the main web scoring function  </td></tr>
</table>

* See [For the Data Scientist](data-scientist.html?path=hdi) for more details about these files.
* See [Typical Workflow](Typical.html?path=hdi)  for more information about executing these scripts.


### Resources for the Solution Package
------------------------------------

<table class="table table-striped table-condensed">
<tr><th> File </th><th> Description </th></tr>
<tr><td> Images </td><td> Directory of images used for the  Readme.md  in this package </td></tr>
</table>




[&lt; Home](index.html)
