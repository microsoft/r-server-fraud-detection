---
layout: default
title: PowerShell Instructions
---
<div class="alert alert-success" role="alert"> This page describes the
<strong>
<span class="cig">{{ site.cig_text }}</span>
<span class="onp">{{ site.onp_text }}</span>
</strong>
solution.
{% include sqlchoices.md %}
</div>

### PowerShell Instructions
---------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
            <li> <a href="#setup">Setup</a></li>
            <li> <a href="#execute-powershell-script">Execute PowerShell Script</a></li>
            <li> <a href="#review-data">Review Data</a></li>
            <li> <a href="#visualizing-results">Visualizing Results</a> </li>
            <li> <a href="#other-steps">Other Steps</a></li>
        </div>
    </div>
    <div class="col-md-6">
        If you have deployed a VM through the  
        <a href="http://aka.ms/campaignoptimization">Azure AI Gallery</a>, all the steps below have already been performed and your database on that machine has all the resulting tables and stored procedures.  Skip to the <a href="Typical.html?platform=cig">Typical Workflow</a> for a description of how these files were first created in R by a Data Scientist and then deployed to SQL stored procedures.
    </div>
</div>

If you are configuring your own server, or if you want to reset your VM to its initial state, continue with the steps below to run the PowerShell script.

## Setup 
-----------

First, make sure you have set up your SQL Server by  <a href="SetupSQL.html">following these instructions</a>.  Then proceed with the steps below to run the solution template using the automated PowerShell file. 

## Execute PowerShell Script
----------------------------

Running this PowerShell script will create the data tables and stored procedures for the the operationalization of this solution in R in the `{{ site.db_name }}` database.  It will also execute these procedures to create full database with results of the steps  – dataset creation, modeling, and scoring as described  [here](dba.html).


1. Log onto the machine that contains the SQL Server you wish to use.

1. Install [Git](https://gitforwindows.org/) if it is not already present.  During the install, check the box to add LFS support.

1. If you wish to install the sample website to demonstrate using the model, install [node.js](https://nodejs.org/en/) if it is not already present.

1. Download  <a href="https://raw.githubusercontent.com/Microsoft/r-server-fraud-detection/master/Resources/ActionScripts/FraudSetup.ps1" download>FraudSetup.ps1</a> to your computer.

1. Open a command or PowerShell window as Administrator.

1. CD to the directory where you downloaded the above .ps1 file and execute the command:

    .\SetupVM.ps1

1. Answer the prompts.  Make sure to accept installation of NuGet if prompted.

1. This will make the following modification to your SQL Server:
    * Installs the SQL Server PowerShell module. If this is already installed, it will update it if necessary.
    * Changes Authentication Method to Mixed Mode, which is needed in this version of the solution.
    * Creates the SLQRUserGroup for running R and Python code.
    * Reconfigures SQL Server to allow running of external scripts.
    * Creates a user with provided username and password
    * Elevates user's credentials to SA.
    * Clones the solution code and data into the c:\Solutions\{{ site.folder_name }} directory
    * Creates the solution database `{{ site.db_name }}` and configures an ODBC connection to the database.
    * Executes the stored procedure `Initial_Run_Once_R` to run the entire workflow for this solution.
    * Installs the sample website if [node.js](https://nodejs.org/en/) is installed.

## Review Data
--------------

Once the PowerShell script has completed successfully, log into the SQL Server Management Studio to view all the datasets that have been created in the `{{ site.db_name }}`  database.
Hit `Refresh` if necessary.

* View [more information](tables.html)  about each of the tables created in the `{{ site.db_name }}` database.

* Right click on `{{ site.db_name }}.dbo.Predict_Scores` and select `View Top 1000 Rows` to preview the testing scored data.

## Visualizing Results 
---------------------

You've now  uploaded and processed transaction data, created models and evaluated the model  as described  [here](data-scientist.html). 

Let's look at our current results. Proceed to <a href="Visualize_Results.html">Visualizing Results with PowerBI</a>.

## Other Steps
----------------

You've just completed the fully automated solution by executing PowerShell scripts.

See the [Typical Workflow](Typical.html) for a description of how these files were first created in R by a Data Scientist and then incorporated into the SQL stored procedures that you just deployed.