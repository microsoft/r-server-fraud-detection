
<h2> Step 4: Deploy and Visualize with Bernie the Business Analyst </h2>
----------------------------------------------------------------

Now that the predictions are created  we will meet our last persona - Bernie, the Business Analyst. Bernie will use the Power BI Dashboard to examine the test data to find an appropriate score cutoff, then use that cutoff value for a new set of loan applicants.

{% include pbix.md %}





### Test Data Tab 

<img src="images/test.jpg">
The output scores from the model have been binned according to the percentiles: the higher the percentile, and the most likely the risk of default. Bernie uses the checkboxes showing these percentiles at the top right to find a suitable level of risk for extending a loan.  He starts by unchecking all boxes, which shows the entire test set.  Then starting at the top (99%), he checks consecutive boxes to view characteristics of those loans whose scores fall into that percentile or above. This  corresponds to a specific choice of a score cutoff value which is shown directly below the checkboxes. For example, for percentiles of 81 and above, the score cutoff is .4680, which means that all scores higher than 0.4933 will be classified as bad. Among those loans classified as bad, the real or expected bad rate is indicated in the box below (40%). 

The Loan Summary table divides those loans classified as bad in two: those that were indeed bad (Bad Loan = Yes) and those that were in fact good although they were classified as bad (Bad Loan = No). For each of those 2 categories, the table shows the number, total and average amount, and the average interest rate of the loans. This allows you to see the expected impact of choosing this cutoff value.

### New Loans Tab 
<img src="images/prod.jpg">
Now Bernie switches to the New Loans tab to view some scored potential loans.  He uses the .4933 cutoff value and views information about these loans.  He sees he will reject 9 of the 22 potential loans based on this critera.

<div class="alert alert-info" role="alert">
The PowerBI file has cached data in it.  You can use these <a href="Visualize_Results.html">steps to refresh the PowerBI data</a>.
</div>  
