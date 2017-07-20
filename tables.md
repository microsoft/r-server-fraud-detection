---
layout: default
title: Description of Database Tables
---

## SQL Database Tables
--------------------------

Below are the different data sets that you will find in the `{{ site.db_name }}` database after deployment. 

<table class="table table-striped table-condensed">
   <tr>
    <th>Table</th>
    <th>Description</th>
  </tr>

<tr><td>Loan</td><td>Raw data about each loan </td></tr>
<tr><td>Borrower</td><td>Raw data about each borrower </td></tr>
<tr><td>Merged</td><td>The merged result of Loan and Borrower </td></tr>
<tr><td>Stats</td><td>Modes or Means of variables of Merged for missing values replacement</td></tr>
<tr><td>Merged_Cleaned</td><td>Merged table with missing values replaced</td></tr>
<tr><td>Bins</td><td>Serialized list of cutoffs used to bin numeric variables</td></tr>
<tr><td>Merged_Features</td><td>Analytical data set: cleaned merged table with new features</td></tr>
<tr><td>Column_Info</td><td>Serialized list of variable information including factors and their levels</td></tr>
<tr><td>Hash_Id</td><td>Loan Ids and Mapping through hash function for splitting</td></tr>
<tr><td>Model</td><td>Serialized version of the trained logistic regression</td></tr>
<tr><td>Logistic_Coeff</td><td>Coefficients of the logistic regression formula in decreasing order of magnitude</td></tr>
<tr><td>Predictions_Logistic</td><td>Predictions made on the testing set</td></tr>
<tr><td>Metrics</td><td>Performance metrics of the evaluated model</td></tr>
<tr><td>Operational_Metrics</td><td>Percentiles of the scores, corresponding score thresholds and bad rates among scores higher than thresholds</td></tr>
<tr><td>Scores_Average</td><td>Average of the predicted scores used in scores transformation</td></tr>
<tr><td>Scores</td><td>Scores computed on the testing set, transformed with Operational_Metrics</td></tr>
  
</table>
