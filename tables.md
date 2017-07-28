---
layout: default
title: Description of Database Tables
---

## SQL Database Tables
--------------------------

Below are the different data sets that you will find in the `{{ site.db_name }}` database after deployment. 

<table class="table table-striped table-condensed">
   <tr>
    <th>File</th>
    <th>Description</th>
  </tr>
  <tr>
    <td>Account_Info</td>
    <td>Raw account data. </td>
  </tr>
    <tr>
    <td>Untagged_Transactions</td>
    <td>Raw transactions data without tags and without account data. </td>
  </tr>
    <tr>
    <td>Fraud</td>
    <td>Raw fraudulent transactions data. </td>
  </tr>
  <tr>
    <td>Account_Info_Sort </td>
    <td>Account_Info table, sorted in ascending order of accountID and descending order of the records date.</td>
  </tr>
  <tr>
    <td>Untagged_Transactions_Acct</td>
    <td>Untagged transactions with account information.</td>
  </tr>
  <tr>
    <td>Tagged</td>
    <td>Tagged transactions with account information.</td>
  </tr>
    <tr>
    <td>Hash_Id</td>
    <td>Each account ID hashed to an integer.</td>
  </tr>
    <tr>
    <td>Tagged_Training</td>
    <td>Training set with tagged data.</td>
  </tr>
    <tr>
    <td>Tagged_Training_Preprocessed (View)</td>
    <td>Preprocessed training set.</td>
  </tr>
  <tr>
    <td>Tagged_Training_Preprocessed_Features1 (View)</td>
    <td>Intermediate preprocessed training set with new features.</td>
  </tr>
  <tr>
    <td>Tagged_Training_Preprocessed_Features (View)</td>
    <td>Preprocessed training set with new features.</td>
  </tr>
  <tr>
    <td>Risk Tables</td>
    <td>Each one contains the risk value for every level of a given variable, computed on the training set.</td>
  </tr>
  <tr>
    <td>Risk_Var</td>
    <td>Names of the risk tables and variables they correspond to.</td>
  </tr>
  <tr>
    <td>Transaction_History</td>
    <td>Historical data to be used for aggregates computation.</td>
  </tr>
  <tr>
    <td>Trained_Model</td>
    <td>Serialized GBT model trained on the training set.</td>
  </tr>
  <tr>
    <td>Tagged_Testing</td>
    <td>Testing set with tagged data.</td>
  </tr>
  <tr>
    <td>Tagged_Testing_Preprocessed (View)</td>
    <td>Preprocessed testing set.</td>
  </tr>
  <tr>
    <td>Tagged_Testing_Preprocessed_Features1 (View)</td>
    <td>Intermediate preprocessed testing set with new features.</td>
  </tr>
  <tr>
    <td>Tagged_Testing_Preprocessed_Features (View)</td>
    <td>Preprocessed testing set with new features.</td>
  </tr>
  <tr>
    <td>Predict_Score</td>
    <td>Predicted scores on the testing set.</td>
  </tr>
  <tr>
    <td>Performance_Auc</td>
    <td>Area under the curve (AUC) for predictions on the testing set.</td>
  </tr>
  <tr>
    <td>Performance</td>
    <td>Account level evaluation metrics for predictions on the testing set.</td>
  </tr>
  <tr>
    <td>Parsed_String</td>
    <td>Single transaction parsed into variables.</td>
  </tr>
  <tr>
    <td>Parsed_String_Acct</td>
    <td>Single transaction parsed into variables, with added account information. </td>
  </tr>
  <tr>
    <td>Parsed_String_Acct_Preprocessed (View)</td>
    <td>Single transaction parsed and preprocessed.</td>
  </tr>
  <tr>
    <td>Parsed_String_Acct_Preprocessed_Features1 (View)</td>
    <td>Intermediate preprocessed single transaction parsed with new features.</td>
  </tr>
  <tr>
    <td>Parsed_String_Acct_Preprocessed_Features (View)</td>
    <td>Single transaction parsed, preprocessed, and with new features.</td>
  </tr>
  <tr>
    <td>Predict_Score_Single_Transaction</td>
    <td>Predicted score for the single transaction.</td>
  </tr>
</table>