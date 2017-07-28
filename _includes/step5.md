
## Step 5: Use the Model during Online Transactions
----------------------------------------------------------------


{% include website.md %}  

To try out this example site, you must first start the lightweight webserver for the site. Open a terminal window or powershell window and type the following command, substituting your own values for <span class="onp">the path and </span> username/password:

```
    cd C:\Solutions\Fraud\Website
    node server.js YOUR_SQL_USERNAME YOUR_SQL_PASSWORD
```

You should see the following response:

```
    Example app listening on port 3000!
    DB Connection Success
```

Now leave this window open and open the url [http://localhost:3000](http://localhost:3000) in your browser.  

This site is set up to mimic a sale on a website.  "Log in" by selecting an account and then add some items to your shopping cart.  Finally, hit the `Purchase` button to trigger the model scoring.  If the model returns a low probability for the transaction, it is not likely to be fraudulent, and the purchase will be accepted. However, if the model returns a high probability, you will see a message that explains the purchaser must contact a support representative to continue. 

You can view the model values by opening the Console window on your browser.

* For Edge or Internet Explorer: Press `F12` to open Developer Tools, then click on the Console tab.
* For FireFox or Chome: Press `Ctrl-Shift-i` to open Developer Tools, then click on the Console tab.


Use the `Log In` button on the site to switch to a different account and try the same transaction again.  (Hint: the account number that begins with a "9" is most likely to have a high probability of fraud.)

See more details about this example see [For the Web Developer](web-developer.html).