---
layout: default
title: On-Prem Setup SQL Server
---
<div class="alert alert-success" role="alert"> This page describes the 
<strong> 
{{ site.onp_text }}
</strong>
solution.
</div> 

# On Prem: Setup SQL Server
--------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
            <li><a href="#prepare-your-sql-server-installation">Prepare your SQL Server Installation</a></li>
            <li><a href="#ready-to-run-code">Ready to Run Code</a></li>
        </div>
    </div>
    <div class="col-md-6">
        The instructions on this page will help you to add this solution to your on premises SQL Server 2016 or higher.  
        <p>
        If you instead would like to try this solution out on a virtual machine, visit the <a href="{{ site.deploy_url }}">Azure AI Gallery</a> and use the Deploy button.  All the configuration described below will be done for you, as well as the initial deployment of the solution. </p>
    </div>
</div>

## Prepare your SQL Server Installation
-------------------------------------------

The rest of this page assumes you are configuring your on premises SQL Server 2016 or higher for this solution.

If you need a trial version of SQL Server 2017, see [What's New in SQL Server 2017](https://docs.microsoft.com/en-us/sql/sql-server/what-s-new-in-sql-server-2017) for download or VM options. 

For more information about SQL server 2017 and R service, please visit: <a href="https://msdn.microsoft.com/en-us/library/mt604847.aspx">https://msdn.microsoft.com/en-us/library/mt604847.asp</a>

Complete the steps in the Set up SQL Server R Services (In-Database) Instructions. The set up instructions file can found at  <a href="https://msdn.microsoft.com/en-us/library/mt696069.aspx" target="_blank"> https://msdn.microsoft.com/en-us/library/mt696069.aspx</a>

* If you are using SQL Server 2016, make sure R Services (In-Database) is installed. 
* If you are using SQL Server 2017, make sure Machine Learning Services (In-Database) is installed.

## Ready to Run Code 
---------------------

* See <a href="Powershell_Instructions.html">PowerShell Instructions</a> to install and run the code for this solution.