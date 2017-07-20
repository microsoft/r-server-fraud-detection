---
layout: default
title: Setup for Local Code Execution
---

<div class="alert alert-success" role="alert"> This page describes the 
<strong>
{{ site.cig_text }}
</strong>
solution.
</div> 

## Setup for Local Code Execution

You can execute code on your local computer and push the computations to the SQL Server on the VM  that was created by the Cortana Intelligence Gallery. But first you must perform the following steps. 

## On the VM: Configure VM for Remote Access

Connect to the VM to perform the following steps.

You must open the Windows firewall on the VM to allow a connection to the SQL Server. To open the firewall, execute the following command in a PowerShell window on the VM:

    netsh advfirewall firewall add rule name="SQLServer" dir=in action=allow protocol=tcp localport=1433 

       
## On your local computer:  Install R Client and Obtain Code

Perform these steps on your local computer.

* If you use your local computer you will need to [install R Client](https://msdn.microsoft.com/en-us/microsoft-r/r-client-get-started#installrclient) on your local machine.  

* If you use Visual Studio, you can add <a href="https://www.visualstudio.com/vs/rtvs/">R Tools for Visual Studio</a>.  Otherwise you might want to try <a href="rstudio.html">R Studio</a>. 

* Also, on your local computer you will need a copy of the solution code.  Open a PowerShell window, navigate to the directory of your choice, and execute the following command:  

    git clone {{ site.code_url }} {{ site.folder_name }}

* This will create a folder **{{ site.folder_name }}** containing the full solution package.

## On your local computer:  Install Packages

There are R Packages required for this solution.  The following steps will install these packages into your R Client.

1.  Open a terminal window or Windows PowerShell window.

2.  CD to the **{{ site.folder_name }}/R** directory.

3.  Run the following to install the required packages into SQL Server:

    ```
    Rscript install_local.R
    ```

<a href="Typical.html#step2">Return to Typical Workflow<a>