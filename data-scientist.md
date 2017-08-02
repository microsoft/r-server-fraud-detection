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
        <div class="toc sql">
          <li><a href="#intro">Fraud Detection</a></li>
          <li><a href="#step0" class="hdi">Step 0: Create Intermediate Directories</a></li>
          <li><a href="#step1">Step 1: Tagging</a></li>
          <li><a href="#step2">Step 2: Splitting and Preprocessing</a></li>
          <li><a href="#step3">Step 3: Feature Engineering</a></li>
          <li><a href="#step4">Step 4: Training, Testing and Evaluating</a></li>
          <li class="sql"><a href="#requirements">System Requirements</a></li>
          <li><a href="#template-contents">Template Contents</a></li>
        </div>
        <div class="toc hdi">
          <li><strong>Development Stage</strong></li>
          <ul>
            <li><a href="#step-0">Step 0: Intermediate Directories Creation</a></li>
            <li><a href="#step-1">Step 1: Merge with Account Information</a></li>
            <li><a href="#step-2">Step 2: Tagging</a></li>
            <li><a href="#step-3">Step 3: Splitting</a></li>
            <li><a href="#step-4">Step 4: Preprocessing</a></li>
            <li><a href="#step-4">Step 4: Preprocessing</a></li>
            <li><a href="#step-5">Step 5: Create Risk Tables</a></li>
            <li><a href="#step-6">Step 6: Feature Engineering</a></li>
            <li><a href="#step-7">Step 7: Training</a></li>
            <li><a href="#step-8">Step 8: Prediction</a></li>
            <li><a href="#step-9">Step 9: Evaluation</a></li>
          </ul>
          <li><a href="#updating">Updating the Production Stage Directory</a></li>
          <li><a href="#production">Production Stage</a></li>
          <li><a href="#deploy">Deploy as a Web Service</a></li>
          <li><a href="#using">Using Your Own Data Set</a></li>
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
Data scientists who are testing and developing solutions can work from the convenience of their R IDE on their client machine, while <a href="https://msdn.microsoft.com/en-us/library/mt604885.aspx">setting the computation context to SQL</a> (see <strong>R</strong> folder for code).  They can also deploy the completed solutions to SQL Server 2016 by embedding calls to R in stored procedures (see <strong>SQLR</strong> folder for code). These solutions can then be further automated by the use of SQL Server Integration Services and SQL Server agent: a PowerShell script (.ps1 file) automates the running of the SQL code.
</div>
<div class="hdi">
Data scientists who are testing and developing solutions can work from the browser-based Open Source Edition of RStudio Server on the HDInsight Spark cluster edge node, while <a href="https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-r-server-compute-contexts">using a compute context</a> to control whether computation will be performed locally on the edge node, or whether it will be distributed across the nodes in the HDInsight Spark cluster. 
</div>

<a name="intro">
## {{ site.solution_name }}
--------------------------

{% include typicalintro.md %}

<div class="sql">
This solution will show how to build a model to detect potential fraudulent transactions so that the transaction may be rejected. This implementation with SQL Server R Services is equivalent to the <a  href="https://gallery.cortanaanalytics.com/Collection/Online-Fraud-Detection-Template-1">Azure ML template for Online Fraud Detection</a>.
</div>


<p></p>
View [more information about the data](input_data.html).

<div class="sql">
<p></p>
In this solution, the final scored database table <code>Predict_Scores</code> is created in SQL Server.  This data is then visualized in PowerBI. 
<p></p>
</div>
<div class="hdi">
<p></p>
In this solution, an Apache Hive table will be created to show predicted scores. This data is then visualized in PowerBI. 
<p></p>
</div>

To try this out yourself, visit the [Quick Start](START_HERE.html) page.  


The file 
<strong>
<span class="sql">modeling_main.R </span>
<span class="hdi">development_main.R</span>
</strong>
enables the user to define the input and call all the steps. 
<span class="sql">Inputs are: paths to the raw data files, database name, server name, username and password.</span>

<p class="sql">The database is created if it does not not already exist, and the connection string as well as the SQL compute context are defined.</p>

<div class="hdi">This script also:

<ul>
  <li>Opens the Spark connection.</li>
  <li>Lets the user specify the paths to the working directories on the edge node and HDFS. We assume they already exist.</li>
  <li>Creates a directory, LocalModelsDir, that will store the model and other tables for use in the Production or Web Scoring stages (inside the fraud_dev main function).</li>
  <li>Updates the tables of the Production stage directory, ProdModelDir, with the contents of LocalModelsDir (inside the fraud_dev main function).</li>
</ul>
</div>

<div class="sql">
{% include datascientist_sql.md %}
</div>

<div class="hdi">
{% include datascientist_hdi.md %}
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