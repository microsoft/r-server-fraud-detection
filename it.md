---
layout: default
title: For the IT Administrator
---
<div class="alert alert-success" role="alert"> This page describes the 
<strong>
{{ site.hdi_text }}
</strong>
solution.
</div> 

## For the IT Administrator
------------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
          <li><a href="#system-requirements">System Requirements</a></li>
          <li><a href="#step1">Cluster Maintenance</a></li>
          <li><a href="#workflow-automation">Workflow Automation</a></li>
        <li><a href="#step0">Data</a></li>
        </div>
    </div>
    <div class="col-md-6">

{% include typicalintro.md %}

View <a href="input_data.html">more information about the data.</a>
          </div>
</div>
<p></p>
This solution demonstrates the code with approximately 200,000 transactions. Using HDInsight Spark clusters makes it simple to extend to very large data, both for training and scoring. As you increase the data size you may want to add more nodes but the code itself remains exactly the same.

## System Requirements
-----------------------

This solution uses:

 * [R Server for HDInsight](https://azure.microsoft.com/en-us/services/hdinsight/r-server/)


## Cluster Maintenance
--------------------------

HDInsight Spark cluster billing starts once a cluster is created and stops when the cluster is deleted. <strong>See <a href="hdinsight.html"> these instructions for important information</a> about deleting a cluster and re-using your files on a new cluster. </strong>


## Workflow Automation
-------------------
Access RStudio on the cluster edge node by using the url of the form `http://CLUSTERNAME.azurehdinsight.net/rstudio`  Run the script **development_main.R** followed by **web_scoring_main.R** to perform all the steps of the solution.

 
<a name="step0">

## Data Files
--------------


The following data files are available in the **{{ site.folder_name }}/Data** directory in the storage account associated with the cluster:

 {% include data.md %}


