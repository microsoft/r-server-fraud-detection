The metric used for assessing accuracy (performance) depends on how the original cases are processed. If each case is processed on a transaction by transaction basis, you can use a standard performance metric, such as transaction-based ROC curve or AUC. 

However, for fraud detection, typically account-level metrics are used, based on the assumption that once a transaction is discovered to be fraudulent (for example, via customer contact), an action will be taken to block all subsequent transactions.

A major difference between account-level metrics and transaction-level metrics is that, typically an account confirmed as a false positive (that is, fraudulent activity was predicted where it did not exist) will not be contacted again during a short period of time, to avoid inconveniencing the customer.

The industry standard fraud detection metrics are ADR vs AFPR and VDR vs AFPR for performance, and transaction level performance, as defined here:
<p></p>
<ul>
<li> 
ADR – Fraud Account Detection Rate. The percentage of detected fraud accounts in all fraud accounts.
</li>
<li>
VDR - Value Detection Rate. The percentage of monetary savings, assuming the current fraud transaction triggered a blocking action on subsequent transactions, over all fraud losses.
</li>
<li>
AFPR - Account False Positive Ratio. The ratio of detected false positive accounts over detected fraud accounts.
</li>
</ul>
<p></p>
You can see these plots as well in the Plots pane after running 
<strong>
<span class="sql">modeling_main.R </span>
<span class="hdi">development_main.R</span>
</strong>.


