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

## PowerShell Instructions
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
        <a href="{{ site.deploy_url }}">Cortana Intelligence Gallery</a>, all the steps below have already been performed and your database on that machine has all the resulting tables and stored procedures.  Skip to the <a href="Typical.html?path=onp">Typical Workflow</a> for a description of how these files were first created in R by a Data Scientist and then deployed to SQL stored procedures.
    </div>
</div>

If you are configuring your own server, continue with the steps below to run the PowerShell script.

## Setup
-----------

First, make sure you have <a href="SetupSQL.html">set up your SQL Server</a>.  Then proceed with the steps below to run the solution template using the automated PowerShell files. 

## Execute PowerShell Script
----------------------------

Running this PowerShell script will create stored procedures for the the operationalization of this solution.  It will also execute these procedures to create full database with results of the steps  – dataset creation, modeling, and scoring as described in the [For the Database Analyst](dba.html) page.



1.	Click on the windows key on your keyboard. Type the words `PowerShell`.  Right click on Windows Powershell to and select `Run as administrator` to open the PowerShell window.


2.	In the Powershell command window, type the following command:
  
    ```
    Set-ExecutionPolicy Unrestricted -Scope Process
    ```

    Answer `y` to the prompt to allow the following scripts to execute.

3. Create a directory on your computer where you will put this solution.  CD to the directory and then clone the repository into it:
    
    ```
    git clone {{ site.code_url }} {{ site.folder_name }}
    ```

4.  CD to the **{{ site.folder_name }}/SQLR** directory.

5. You are now ready to run the PowerShell script.  

    * Use  the following command, inserting your server name, database name, username, and password.
    
        ```
        .\{{ site.ps1_name }} -ServerName "Server Name" -DBName "Database Name" -username "" -password "" 
        ```

    * For example, for a user named <code>rdemo</code> with a password of <code>D@tascience</code> on his local computer, the command would be: 

        ```
        .\{{ site.ps1_name }} -ServerName "localhost" -DBName "{{ site.db_name }}" -username "rdemo" -password "D@tascience" 
        ```


## Review Data
--------------

Once the PowerShell script has completed successfully, log into the SQL Server Management Studio to view all the datasets that have been created in the `{{ site.db_name }}` and the `{{ site.db_name }}_Prod` databases.  
Hit `Refresh` if necessary.
<br/>

* View [more information](tables.html)  about each of the tables created in the `{{ site.db_name }}` database.

* Right click on `{{ site.db_name }}.dbo.Predict_Scores` and select `View Top 1000 Rows` to preview the testing scored data.

* Right click on `{{ site.db_name }}_Prod.dbo.Prod_Scores` and select `View Top 1000 Rows` to preview the production scored data.


## Visualizing Results 
---------------------

You've now  uploaded and processed transaction data, created models and evaluated the model  as described  [here](data-scientist.html). 

Let's look at our current results. Proceed to <a href="Visualize_Results.html">Visualizing Results with PowerBI</a>.

## Other Steps
----------------

You've just completed the fully automated solution by executing PowerShell scripts.  

See the [Typical Workflow](Typical.html) for a description of how these files were first created in R by a Data Scientist and then incorporated into the SQL stored procedures that you just deployed.