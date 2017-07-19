---
layout: default
title: For the Business Manager
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

## For the Business Manager
------------------------------

XXX
<p></p>
<div class="sql"> 
SQL Server R Services takes advantage of the power of SQL Server 2016 and ScaleR (Microsoft R Server package) by allowing R to run on the same server as the database. It includes a database service that runs outside the SQL Server process and communicates securely with the R runtime. 
<p></p>
This solution shows how to preprocess data, create new features, train R models, and perform predictions in-database. The final table in the SQL Server database provides a predicted value for each borrower. This predicted value, which can be interpreted as a probability of default, can help you determine whether you wish to approve the loan.
<p></p>
</div>
<div class="hdi">
Microsoft R Server on HDInsight Spark clusters provides distributed and scalable machine learning capabilities for big data, leveraging the combined power of R Server and Apache Spark. This solution demonstrates how to develop machine learning models for loan credit risk (including data processing, feature engineering, training and evaluating models), deploy the models as a web service (on the edge node) and consume the web service remotely with Microsoft R Server on Azure HDInsight Spark clusters. 

<p></p>
Hive tables are saved containing the predicted scores during both development and production. This data is then visualized in Power BI.
<p></p>
</div>

The PowerBI dashboard allows you to visualize and use these predicted scores to aid in XXX

### XXX Tab
XXX
<p></p>


 <p></p> 

{% include pbix.md %}


To understand more about the entire process of modeling and deploying this example, see [For the Data Scientist](data-scientist.html).
 

[&lt; Home](index.html)
