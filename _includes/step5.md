
<h2>Step 5: Use the Model during Online Transactions</h2>
<hr />

{% include website.md %}  


<p>This solution contains an example of a website that does just that.  The example is not meant to be production-quality code, it is meant simply to show how a website might make use of such a model. The example shows the purchase page of a transaction, with the ability to try the same simulated purchase from multiple accounts.</p>

<p>To try out this example site, you must first start the lightweight webserver for the site. Open a terminal window or powershell window and type the following command, substituting your own values for <span class="onp">the path and </span> username/password:</p>

<pre class="highlight"><code>
    cd C:\Solutions\Fraud\Website
    node server.js YOUR_SQL_USERNAME YOUR_SQL_PASSWORD
</code></pre>


<p>You should see the following response:</p>

<pre class="highlight"><code>
    Example app listening on port 3000!
    DB Connection Success
</code></pre>


<p>Now leave this window open and open the url <a href="http://localhost:3000">http://localhost:3000</a> in your browser.</p>

<p>This site is set up to mimic a sale on a website.  “Log in” by selecting an account and then add some items to your shopping cart.  Finally, hit the <code class="highlighter-rouge">Purchase</code> button to trigger the model scoring.  If the model returns a low probability for the transaction, it is not likely to be fraudulent, and the purchase will be accepted. However, if the model returns a high probability, you will see a message that explains the purchaser must contact a support representative to continue.</p>

<p>You can view the model values by opening the Console window on your browser.</p>

<ul>
  <li>For Edge or Internet Explorer: Press <code>F12</code> to open Developer Tools, then click on the Console tab.</li>
  <li>For FireFox or Chome: Press <code>Ctrl-Shift-i</code> to open Developer Tools, then click on the Console tab.</li>
</ul>

<p>Use the <code>Log In</code> button on the site to switch to a different account and try the same transaction again.  (Hint: the account number that begins with a “9” is most likely to have a high probability of fraud.)</p>

<p>See more details about this example see <a href="web-developer.html">For the Web Developer</a>.</p>
