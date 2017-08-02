

<h2> Step 3: Operationalize with Debra <span class="sql">and Danny</span></h2>
---------------------

Debra has completed her tasks.  <span class="sql">She has connected to the SQL database, executed code from her R IDE that pushed (in part) execution to the SQL machine to create the fraud model.
</span>
<span class="hdi">She has executed code from RStudio that pushed (in part) execution to Hadoop to create the fraud model.
</span> 
She has preprocessed the data, created features, built and evaluated a model.  Finally, she created a summary dashboard which she will hand off to Bernie - see below.
<p></p>

<div class="sql">
Now that we have a model, we will want to use it to predict future fraudulant behavior. Danny now operationalizes the code in the database as stored procedures, using embedded R code, or SQL queries.  You can see these procedures by logging into SSMS and opening the <code>Programmability>Stored Procedures</code> section of the <code>{{ site.db_name }}</code> database.  Find more details about these procedures on the <a href="dba.html">For the Database Analyst</a> page.
<p></p>
Finally, Danny has created a PowerShell script that will re-run the all the steps to train the model.
You can find this script in the <strong>SQLR</strong> directory, and execute it yourself by following the <a href="Powershell_Instructions.html">PowerShell Instructions</a>.  
<span class="cig">As noted earlier, this was already executed when your VM was first created.
</span>
<span class="onp"> As noted earlier, this is the fastest way to execute all the code included in this solution.  (This will re-create the same set of tables and models as the above R scripts.)
</span>
</div>


<div class="hdi">
<p></p>
Now that we have evaluated the model, it is time to put it to use in predicting fraud during an online transaction. 
Debra now creates an analytic web service  with <a href="https://msdn.microsoft.com/en-us/microsoft-r/operationalize/about">R Server Operationalization</a> that incorporates these same steps: data processing, feature engineering, and scoring.
<p/>
 <strong>web_scoring_main.R</strong> will create a web service and test it on the edge node.  
<p/>
<div class="alert alert-info" role="alert">
The operationalization server has been configured for you on the edge node of your cluster.
Follow <a href="deployr.html">instructions here</a> if you wish to connect to the edge node and/or use the admin utility.
</div>
</div>
