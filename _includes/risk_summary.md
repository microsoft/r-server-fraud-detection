<p>The risk table stores risk (log of smoothed odds ratio) for each level of one categorical variable. For example, variable <code>X</code> has two levels: <code>A</code> and <strong>B</strong>. For level <strong>A</strong>, we compute the following:</p>

<ul>
  <li>Total number of good transactions, <code>n_good(A)</code>,</li>
  <li>Total number of bad transactions, <code>n_bad(A)</code>.</li>
  <li>The smoothed odds, <code>odds(A) = (n_bad(A)+10)/(n_bad(A)+n_good(A)+100)</code>.</li>
  <li>The the risk of level <code>A</code>, <code>Risk(A) = log(odds(A)/(1-odds(A))</code>.</li>
</ul>

<p>Similarly, we can compute the risk value for level <strong>B</strong>. Thus, the risk table of variable <code>X</code> is constructed as the following:</p>

<table class="table">
  <tr>
    <th>X</th>
    <th>Risk</th>
  </tr>
  <tr>
    <td>A</td>
    <td>Risk(A)</td>
  </tr>
  <tr>
    <td>B</td>
    <td>Risk(B)</td>
  </tr>
</table>

<p>With the risk table, we can assign the risk value to each level. This is how we transform the categorical variable into numerical variable.</p>
