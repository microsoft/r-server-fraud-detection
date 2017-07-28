---
layout: default
title: HOME
---
{% include typicalintro.md %}

Fraud detection is typically handled as a binary classification problem, but the class population is unbalanced because instances of fraud are usually very rare compared to the overall volume of transactions. Moreover, when fraudulent transactions are discovered, the business typically takes measures to block the accounts from transacting to prevent further losses. Therefore, model performance is measured by using account-level metrics, which is discussed in the [For the Data Scientist](data-scientist.html) page.

<div class="alert alert-success">
<h2>Select the method you wish to explore:</h2>
 <form style="margin-left:30px"> 
    <label class="radio">
      <input type="radio" name="optradio" class="rb" value="cig" > {{ site.cig_text }}, deployed from <a href="{{ site.deploy_url }}">Cortana Intelligence Gallery</a>
    </label>
    <label class="radio">
      <input type="radio" name="optradio" class="rb" value="onp"> {{ site.onp_text }}
    </label>
   <label class="radio">
      <input type="radio" name="optradio" class="rb" value="hdi"> {{ site.hdi_text }}, deployed from <a href="{{ site.deploy_url_hdi }}">Cortana Intelligence Gallery</a>
    </label> 

</form>
</div>
<p></p>

<div class="cig">
On the VM created for you from the <a href="{{ site.deploy_url }}">Cortana Intelligence Gallery</a>, the SQL Server 2016 database <code>{{ site.folder_name }}</code> contains all the data and results of the end-to-end modeling process.  
<img src="images/diagramcig.jpg">

</div>

<div class="onp">
For customers who prefer an on-premise solution, the implementation with SQL Server R Services is a great option that takes advantage of the powerful combination of SQL Server and the R language.  A Windows PowerShell script to invoke the SQL scripts that execute the end-to-end modeling process is provided for convenience. 
<img src="images/diagramonp.jpg">

</div>

<div class="hdi">
This solution shows how to pre-process data (cleaning and feature engineering), train prediction models, and perform scoring on the  HDInsight Spark cluster with Microsoft R Server deployed from the <a href="{{ site.deploy_url_hdi }}">Cortana Intelligence Gallery</a>.
<p></p>
<strong>HDInsight Spark cluster billing starts once a cluster is created and stops when the cluster is deleted. See <a href="hdinsight.html"> these instructions for important information</a> about deleting a cluster and re-using your files on a new cluster.</strong>
<img src="images/diagramhdi.jpg">
</div>





 



