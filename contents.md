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
    <td>01-generate-tagged-data.R</td>
    <td>Tag data as fraud, non-fraud and pre-fraud on account level</td>
  </tr>
  <tr>
    <td>02-data-preprocessing.R</td>
    <td>Preprocess and clean the data. Split the data into training and testing sets</td>
  </tr>
  <tr>
    <td>03-create-risk-table.R</td>
    <td>Create risk table which will be used to assign risks for categorical variables</td>
  </tr>
  <tr>
    <td>04-training-feature-generation.R</td>
    <td>Feature engineering for training set</td>
  </tr>
  <tr>
    <td>05-train.R</td>
    <td>Model training</td>
  </tr>
  <tr>
    <td>06-prediction.R</td>
    <td>Prediction on testing set</td>
  </tr>
  <tr>
    <td>07-evaluation.R</td>
    <td>Evaluate performance</td>
  </tr>
</table> 


* See [For the Data Scientist](data-scientist.html?path=cig) for more details about these files.
* See [Typical Workflow](Typical.html?path=cig)  for more information about executing these scripts.

### Operationalize in SQL Server 2016 
-------------------------------------------------------

These files are in the **SQLR** directory.

<table class="table table-striped table-condensed">

<tr><th> File </th><th> Description </th></tr>
<tr><td>Load_Data.ps1 </td><td>Loads initial data into SQL Server  </td></tr>
<tr><td>Loan_Credit_Risk.ps1  </td><td>Automates execution of all .sql files and creates stored procedures  </td></tr>
<tr><td>create_tables_prod.sql   </td><td>Creates the production tables   </td></tr>
<tr><td>create_user.sql  </td><td>Used during initial SQL Server setup to create the user and password and grant permissions </td></tr>
<tr><td>modeling_proc.sql   </td><td>Stored procedure for the modeling/development pipeline  </td></tr>
<tr><td>production_proc.sql   </td><td>Stored procedure for the production pipeline  </td></tr>
<tr><td> step1_data_processing.sql  </td><td> Replaces Missing values in dataset with the modes or means </td></tr>
<tr><td> step2a_splitting.sql </td><td> Splits the analytical dataset into Train and Test</td></tr>
<tr><td> step2b_feature_engineering.sql </td><td> Performs Feature Engineering and creates the Analytical Dataset</td></tr>
<tr><td> step3a_training.sql</td><td> Trains a Logistic Regression model</td></tr>
<tr><td> step3b_scoring.sql </td><td> Scores data using the Logistic Regression model</td></tr>
<tr><td> step3c_evaluating.sql </td><td> Evaluates the model </td></tr>
<tr><td> step4_operational_metrics.sql </td><td> Computes operational metrics and performs scores transformations  </td></tr>
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
<tr><td> development_main.R</td><td> Full development process</td></tr>
<tr><td> in_memory_scoring.R</td><td>Performs in-memory scoring for batch scoring or for scoring remotely with a web service  </td></tr>
<tr><td> production_main.R</td><td> Scores new data using subset of development steps</td></tr>
<tr><td> step0_directories_creation.R</td><td>Creates initial directories</td></tr>
<tr><td> step1_preprocessing.R</td><td>Merges data and then cleans the merged data sets: replace NAs with the global mean (numeric variables) or global mode (character variables), used in both development and production</td></tr>
<tr><td> step2_feature_engineering.R</td><td>Performs Feature Engineering, used in both development and production  </td></tr>
<tr><td> step3_train_score_evaluate.R</td><td>Builds the logistic regression classification model, scores the test data and evaluates, used in both development and production </td></tr>
<tr><td> step4_operational_metrics.R</td><td> Computes operational metrics and performs scores transformations in development, uses metrics and performs score transformations in production </td></tr>


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
