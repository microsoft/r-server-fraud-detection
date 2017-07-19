---
layout: default
title: Operationalization with R Server
---
<div class="alert alert-success" role="alert"> This page describes the 
<strong>
{{ site.hdi_text }} 
</strong>
solution.
</div> 

## Configuring Operationalization with R Server
---------------------------------------
To access R Server Operationalization administrative services from your local computer, you must first connect to the edge node using the steps below.   

## Connect to Edge Node

* **Windows users:**
For instructions on how to use PuTTY to connect to your HDInsight Spark cluster, visit the
[Azure documentation](http://go.microsoft.com/fwlink/p/?LinkID=620303#connect-to-a-linux-based-hdinsight-cluster).  Your edge node address is of the form `CLUSTERNAME-ed-ssh.azurehdinsight.net`.  

* **Linux, Unix, and OS X users**
For instructions on how to use the terminal to connect to your HDInsight Spark cluster, visit this [Azure documentation](http://go.microsoft.com/fwlink/p/?LinkID=619886).  The edge node address is of the form `sshuser@CLUSTERNAME-ed-ssh.azurehdinsight.net`

* **All platforms:** Your login name and password are the ones you created when you deployed this solution from the [Cortana Intelligence Gallery](http://aka.ms/loan-credit-risk-hdi)


## Configure Deployment Server

* Once you have connected to the edge node you can access the Administration Utilities for the web server with:

```
sudo dotnet /usr/lib64/microsoft-deployr/9.1.0/Microsoft.DeployR.Utils.AdminUtil/Microsoft.DeployR.Utils.AdminUtil.dll
```

Your server has been configuered with a password of  `D@tascience2017` for the  `admin` user.  You can use this utitlity to change the password if you wish. (If you do so, you will need to change the password in the  <strong>deployment_main.R</strong> script.)

You can also use this utility to check on the status of the web server. 

* Enter `6` to select "6. Run diagnostic tests";
* Enter `a` to select “A. Test configuration”;
* Provide username as `admin` and the password you just created;
* You should see “Overall Health: pass”;
* Now press `e` followed by ‘8’ to exit this tool




 

<a href="Typical.html#step3">Return to Typical Workflow<a>