---
layout: default
title: Visualizing Results with PowerBI
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

## Visualizing Results with PowerBI
-----------------------------------


<div class="alert alert-info cig" >
This page shows how to refresh data for the PowerBI file on your VM.  If you want to instead download the file to your own computer, first open the firewall on your VM by execute the following command in a PowerShell window on the VM:
<pre class="highlight">
netsh advfirewall firewall add rule name="SQLServer" dir=in action=allow protocol=tcp localport=1433 
</pre>
<p/>
Then follow the <a href="Visualize_Results.html?path=onp">{{ site.onp_text }} instructions</a>. 
</div>  
<p/>
These instructions show you how to replace the cached data in the PowerBI dashboard with data from your 
<span class="cig"><strong>{{ site.cig_text }}</strong> solution. </span>
<span class="onp"><strong>{{ site.onp_text }}</strong> solution, or any SQL Server other than `localhost`. </span>
<span class="hdi"><strong>{{ site.hdi_text }}</strong> solution. </span>
  
<ol>
<li class="cig">Open the <strong>{{ site.pbix_name }}</strong> file from the <strong>Desktop/{{ site.folder_name }}</strong> folder on your VM. </li>
<li class="onp">Download and open the <a href="{{ site.pbix_sqldownload_url }}" target="_blank">{{ site.pbix_name }}</a> file</li>
<li class="hdi">Download and open the <a href="{{ site.pbix_hdidownload_url }}" target="_blank">{{ site.hdipbix_name }}</a> file</li>
<li class="onp hdi">Click on <code>Edit Query</code></li>
<li class="onp hdi">Select the first query (<code>TestData</code>) and then click on the <code>Advanced Editor</code> in the toolbar.</li>
<li class="onp">Replace <code>localhost</code> with your server name and click on <code>Done</code>. (If you are using an Azure VM, such as the one deployed by Cortana Ingelligence Gallery, use the IP address for the server name.)</li>
<li class="hdi">Replace <code>sdglcr2</code> with your cluster name and click on <code>Done</code>. (The full address will be <strong>http://CLUSTERNAME.azurehdinsight.net</strong>.)
<img src="images/scoresdata.png"> </li>
<li class="onp hdi">After a moment you'll see an alert asking for Permision to run the query.  Click on <code>Edit Permission</code> </li>


<li class="cig">Press <code>Refresh</code> on the top toolbar. </li>

<li class="sql">On the Native Database Query dialog, click the <code>Run</code> button.</li>

<li  class="onp">On the Please specify how to connect alert, click on <code>Edit Credentials</code></li>
<li class="sql">Next you will see a SQL Server database login. Select the second tab,  <code>Database</code> on the left.</li>
<li class="sql">Enter your username and password (username <code>rdemo</code>, password <code>D@tascience</code> are the defaults for this solution, use these unless you've changed them.)  Then select the <code>Connect</code> button.</li>
<li class="hdi">Enter your username and password,  Then select the <code>Connect</code> button.</li>
<li class="sql">Select <code>OK</code> on the Encrypting Support alert.</li>
<li class="onp hdi">The TestData table will appear.</li>
<li class="onp"> Select each of the remaining queries on the left.  For each one use the <code>Advanced Editor</code> in the toolbar and replace <code>localhost</code> with your server name. You won't need to provide credentials again.</li>
<li class="hdi"> Select each of the next two queries on the left (Operational_Metrics and ScoresData_Prod).  For each one use the <code>Advanced Editor</code> in the toolbar and replace <code>sdglcr2</code> with your cluster name. You won't need to provide credentials again.</li>
<li class="onp hdi">Now close the Query Editor.  Select <code>Yes</code> to the prompt to apply your changes.</li>
<li> You are now viewing data from your <span class="sql">SQL Database</span><span class="hdi">Hive tables</span>, rather than the imported data that was part of the initial solution package.</li>
<li>Subsequent clicks of <code>Refresh</code> will work without further authentication steps.</li>
<li>When you close the file, save your changes.</li>
</ol>

<div class="alert alert-info onp" role="alert">
Return to the <a href="Visualize_Results.html?path=cig">{{ site.cig_text }} instruction</a> if you have deployed your solution from the Cortana Intelligence Gallery.
</div>

[&lt; Home](index.html)