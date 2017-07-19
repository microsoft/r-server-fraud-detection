---
layout: default
title: Typical Workflow 
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

## Typical Workflow 
--------------------------------------------------------------

<div class="row">
    <div class="col-md-6">
        <div class="toc">
        <li><a href="#step1">Step 1: Server Setup and Configuration</a></li>
        <li><a href="#step2">Step 2: Data Prep and Modeling</a></li>
        <li><a href="#step3">Step 3: Operationalize</a></li>
        <li><a href="#step4">Step 4: Deploy and Visualize</a></li>
        </div>
    </div>

<div class="col-md-6">

 {% include typicalintro.md %}

</div>
</div>

<p/>
{% include typicalintro1.md %}

To demonstrate a typical workflow, we'll introduce you to a few personas.  You can follow along by performing the same steps for each persona.  

<div class="onp">
<div class="alert alert-info" role="alert"> 
NOTE: If you’re just interested in the outcomes of this process we have also created a fully automated solution that simulates the data, trains and scores the models by executing PowerShell scripts. This is the fastest way to deploy the solution on your machine. See <a href="Powershell_Instructions.html">PowerShell Instructions</a> for this deployment.
<p>
If you want to follow along and have <strong>not</strong> run the PowerShell script, you must to first create a database table in your SQL Server.  You will then need to replace the connection_string in the <strong>modeling_main.R</strong>  file with your database and login information.</p></div>
</div>

 <a name="step1" id="step1"></a>
 

     {% include step1.md %} 


<div class="cig">
<p/><p>
This step has already been done on your deployed Cortana Intelligence Gallery VM.
</p>
</div>

<div class="onp">     
<p>
You can perform these steps in your environment by using the instructions  <a href="SetupSQL.html">to Setup your On-Prem SQL Server</a>. 
</p>
</div>

<div class="hdi">
<p/><p>
The cluster has been created and data loaded for you when you used the <code>Deploy</code> button in the <a href="{{ site.deploy_url_hdi }}">Cortana Intelligence Gallery</a>. <strong>Once you complete the walkthrough, you will want to delete this cluster as it incurs expense whether it is in use or not - see <a href="hdinsight">HDInsight Cluster Maintenance</a> for more details.</strong>
</p>
</div>


 <a name="step2" id="step2"></a>

## Step 2: Data Prep and Modeling with Debra the Data Scientist
-----------------------------------------------------------------

Now let's meet Debra, the Data Scientist. Debra's job is to use historical data to predict a model to detect fraud. 
<span class="sql">Debra's preferred language for developing the models is using R and SQL. She uses Microsoft R Services with SQL Server 2016 as it provides the capability to run large datasets and also is not constrained by memory restrictions of Open Source R.
</span>
<span class="hdi">Debra will develop these models using <a href="https://azure.microsoft.com/en-us/services/hdinsight/">HDInsight</a>, the managed cloud Hadoop solution with integration to Microsoft R Server.
</span>  


<div class="sql">
Debra will work on her own machine, using  <a href = "https://msdn.microsoft.com/en-us/microsoft-r/install-r-client-windows">R Client</a> to execute these R scripts. <span class="cig">R Client is already installed on the VM.</span>  She will also use an IDE to run R.  
</div>



<div class="hdi">
<p/>
<a name="rstudiologin"></a>

Debra will develop her R scripts in  the Open Source Edition of RStudio Server, installed on her cluster's edge node.  You can follow along on <a href="{{ site.deploy_url_hdi }}">your own cluster deployed by Cortana Analytics Gallery</a>.  Access RStudio by using the url of the form: <br/> <code>http://CLUSTERNAME.azurehdinsight.net/rstudio</code>. 
<p/>
<div class="alert alert-info" role="alert">
When you first visit the url to access RStudio, you will see two different logins.  Use the username and  password you created when you deployed the HDInsight solution for both of these prompts.
</div>



</div>

<div class="alert alert-info cig" role="alert">
On your VM, both R Tools for Visual Studio and RStudio are installed.  (You will  have to either log in or create a new account for if you wish to use Visual Studio.)  
<p></p>
OPTIONAL: You can execute the R code on your local computer if you wish, but you must first <a href="local.html">prepare both the VM and your computer</a>.  Additionally you can view and execute the R code  <a href="jupyter.html">in a Jupyter Notebook on the VM</a>.
</div>

<div class="onp">
<p/>
You can use your favorite IDE to follow along.  If you use Visual Studio, you can add <a href="https://www.visualstudio.com/vs/rtvs/">R Tools for Visual Studio</a>.  Otherwise you might want to try <a href="rstudio.html">R Studio</a>. 
</div>

<div class="sql">
<p/>
Now that Debra's environment is set up, she  opens her IDE and creates a Project.  To follow along with her, open the <strong>{{ site.folder_name }}/R</strong> directory on <span class="cig">the VM desktop </span> <span class="onp">your computer</span>.  

There you will see three files with the name <strong>{{ site.solution_name }}</strong>

<ul>
<li>If you use Visual Studio, double click on the Visual Studio SLN file.</li>
<li>If you use RStudio, double click on the "R Project" file.</li>
</ul>
</div>


    {% include step2.md %}



 <a name="step3" id="step3"></a>

   {% include step3.md %}


<a name="step4" id="step4"></a>

    {% include step4.md %}

    