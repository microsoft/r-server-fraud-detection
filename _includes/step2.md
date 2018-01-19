
You are ready to follow along with Debra as she creates the model needed for this solution. 
<span class="sql"> 
If you are using Visual Studio, you will see these file in the <code>Solution Explorer</code> tab on the right. In RStudio, the files can be found in the <code>Files</code> tab, also on the right. 
</span> 

<div class="sql">
<strong>modeling_main.R</strong> is used to define the input and call all these steps. The inputs are pre-poplulated with the default values created for a VM from the Azure AI Gallery.  You must  change the values accordingly for your implementation if you are not using the default server (<code>localhost</code> represents a server on the same machine as the R code). If you are connecting to an Azure VM from a different machine, the server name can be found in the Azure Portal under the "Network interfaces" section - use the Public IP Address as the server name.
</div>
<div class="hdi">
<strong>development_main.R</strong> is used to define the input and call all these steps.
</div>
<p></p>
The steps to create and evaluate the model are described in detail on the <a href="data-scientist.html">For the Data Scientist</a> page. Open and execute the file 
<strong>
<span class="sql">modeling_main.R</span>
<span class="hdi">development_main.R</span>
</strong> 
to run all these steps.  You may see some warnings regarding <code>strptime</code> and <code>rxClose</code>. You can ignore these warnings.
<p></p>
<div class="alert alert-info" role="alert">
    In <span class="sql">both Visual Studio and</span> RStudio, there are multiple ways to execute the code from the R Script window.  The fastest way <span class="sql">for both IDEs</span> is to use Ctrl-Enter on a single line or a selection.  Learn more about  <span class="sql"><a href="http://microsoft.github.io/RTVS-docs/">R Tools for Visual Studio</a> or</span> <a href="https://www.rstudio.com/products/rstudio/features/">RStudio</a>.
</div>


After executing this code, you can examine the ROC curve for the Gradient Boosted Tree model in the Plots pane. This gives a transaction level metric on the model. 

 {% include metrics.md %}
