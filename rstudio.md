---
layout: default
title: Using RStudio with R Server
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

## Install RStudio
<div class="onp">
If you don't have RStudio <a href="https://www.rstudio.com/products/rstudio/download2/" target="_blank">get it here</a>.
</div>
<div class="cig">
RStudio is already installed on your VM and configured for you.  
</div>
<div class="hdi">
RStudio is already installed and configured on the edge node of your cluster.  To install it on your local computer <a href="https://www.rstudio.com/products/rstudio/download2/" target="_blank">get it here</a>.
</div>

## Set Up RStudio for R Server
RStudio needs to use R Server for the code in this solution.  Follow the instructions below to set up RStudio to use R Server and/or to verify that you are using the correct version.  
<div class="hdi">(These steps are is not necessary for the version on the cluster edge node.)</div>
<ol>
<li>Launch RStudio.</li>
<li> Update the path to R.</li>
<ol type="a">
<li>From the <code>Tools</code> menu, choose <code>Global Options</code>.</li>
<li>In the General tab, update the path to R to point to R Server:</li>
<ul><li>On the VM deployed from <a href="{{ site.deploy_url }}">Azure AI Gallery</a> the path is <code>C:\Program Files\Microsoft SQL Server\130\R_SERVER</code></li>
<li>If you installed R Server on your own computer, the path is <code>C:\Program Files\Microsoft\R Client\R_SERVER\bin\x6b</code></li></ul>
</ol>
<li>If you changed the path, exit RStudio. When you relaunch RStudio, R Client will now be the default R engine.</li>
</ol>


 

<a href="Typical.html#step2">Return to Typical Workflow for Azure AI Gallery Deployment<a>