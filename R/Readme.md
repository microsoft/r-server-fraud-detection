This is the R (Microsoft R Server) code for Online Fraud Detection template using SQL Server R Services. This code runs on a local R IDE (such as RStuio, R Tools for Visual Studio), and the computation is done in SQL Server (by setting compute context).

In this template, the online purchase transaction fraud detection scenario (for the online merchants, detecting whether a transaction is made by the original owner of payment instrument) is used as an example. This on-prem implementation with SQL Server R Servicds is equivalent to the [Azure ML template for Online Fraud Detection](https://gallery.cortanaanalytics.com/Collection/Online-Fraud-Detection-Template-1).

For customers who prefers an on-prem advanced anlaytics solution, the on-prem implementation with SQL Server R Services is an alternative, which takes advantage of the power of SQL Server and RevScaleR (Micorosoft R Server).

The template solves the fraud detection as a **binary classification** problem.

It consists of the following files:

<table style="width:85%">
  <tr>
    <th>File</th>
    <th>Description</th>
  </tr>
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

