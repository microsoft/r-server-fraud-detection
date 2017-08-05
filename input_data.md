---
layout: default
title: Input Data
---

## CSV File Description
--------------------------
The **Data** folder contains the following data:
* **untaggedTransactions.csv**:  transaction data from on an online store
* **accountInfo.csv**: Anonomyzed account information
* **fraudTransactions.csv**: transactions identified as fraud                                                                                          

**untaggedTransactions.csv** contain the following fields:


<table class="table table-compressed table-striped">
    <tr><th>Columns</th><th>Type</th><th>Description</th></tr>
    <tr><td>transactionID</td><td>String</td><td>Unique transaction Id</td></tr>
    <tr><td>accountID</td><td>String</td><td>Unique account Id</td></tr>
    <tr><td>transactionAmountUSD</td><td>Double</td><td>Transaction amount in USD
    e.g., 12345.00</td></tr>
    <tr><td>transactionAmount</td><td>Double</td><td>Transaction amount in currency expressed in transactionCurrencyCode
    e.g., 12345.00</td></tr>
    <tr><td>transactionCurrencyCode</td><td>String</td><td>Currency code of the transaction.
    3 alphabet letters, e.g., USD</td></tr>
    <tr><td>transactionCurrencyConversionRate</td><td>Double</td><td>Conversion rate to US Dollars,
    e.g. 1.0000 for USD to USD</td></tr>
    <tr><td>responseCode</td><td>String</td><td>response code from card issuer payment authorization</td></tr>
    <tr><td>digitalItemCount</td><td>integer</td><td>Number of digital items purchased. (e.g. music, ebook, software, etc, that can be directly downloaded online)</td></tr>
    <tr><td>physicalItemCount</td><td>integer</td><td>Number of physical items purchased (that needs to be shipped)</td></tr>
    <tr><td>purchaseProductType</td><td>String</td><td>Type of product purchased</td></tr>
    <tr><td>shippingAddress</td><td>String</td><td>shipping street address</td></tr>
    <tr><td>shippingPostalCode</td><td>String</td><td>shipping postal code</td></tr>
    <tr><td>shippingCity</td><td>String</td><td>shipping city</td></tr>
    <tr><td>shippingState</td><td>String</td><td>shipping state</td></tr>
    <tr><td>shippingCountry</td><td>String</td><td>shipping country (3-alpha)</td></tr>
    <tr><td>cvvVerifyResult</td><td>String</td><td>M-- CVV2 Match<br/>
    N-- CVV2 No Match<br/>
    P--Not Processed<br/>
    S--Issuer indicates that CVV2 data should be present on the card, but the merchant has indicated data is not present on the card<br/>
    U--Issuer has not certified for CVV2 or Issuer has not provided Visa with the CVV2 encryption keys
    Empty--Transaction failed because wrong CVV2 number was entered or no CVV2 number was entered</td></tr>
    <tr><td>paymentInstrumentID</td><td>String</td><td>ID of payment Instrument:
    e.g. credit card number (hashed or encrypted)
    e.g. paypal account Id</td></tr>
    <tr><td>paymentBillingAddress</td><td>String</td><td>Street Address , hashed or encrypted</td></tr>
    <tr><td>paymentBillingPostalCode</td><td>String</td><td>payment billing postal code</td></tr>
    <tr><td>paymentBillingState</td><td>String</td><td>payment billing state</td></tr>
    <tr><td>paymentBillingCountryCode</td><td>String</td><td>payment billing country code</td></tr>
    <tr><td>paymentBillingName</td><td>String</td><td>Name, hashed or encrypted, 
    needs to be consistent with other names</td></tr>
    <tr><td>isProxyIP</td><td>String</td><td>Whether the IP address is a proxy or not</td></tr>
    <tr><td>browserType</td><td>String</td><td>I -- IE<br/>
    C -- Chrome<br/>
    F -- Firefox<br/>
    O -- Other</td></tr>
    <tr><td>browserLanguage</td><td>String</td><td>Similar to country code</td></tr>
    <tr><td>paymentInstrumentType</td><td>String</td><td>Type of payments:<br/>
    C -- Credit Card<br/>
    D -- Debit Card<br/>
    P  -- Paypal<br/>
    K  -- Check<br/>
    H -- Cash<br/>
    O -- Other</td></tr>
    <tr><td>cardType</td><td>String</td><td>Type of cards
    M -- Magnetic
    C   -- Chip</td></tr>
    <tr><td>cardNumberInputMethod</td><td>String</td><td>Input method of payment instrument number:<br/>
    K -- Keyed<br/>
    S -- Swiped<br/>
    C --- Chip<br/>
    D  -- Contactless</td></tr>
    <tr><td>transactionDeviceType</td><td>String</td><td>P -- PC<br/>
    M -- Mobile  Devices<br/>
    C -- Console (e.g. Xbox, DVD)<br/>
    O -- Other</td></tr>
    <tr><td>transactionDeviceId</td><td>String</td><td>Mac Address, or Hardware
     ID like serial number</td></tr>
    <tr><td>transactionIPaddress</td><td>String</td><td>Full IP Address for IPv4: 
    000.000.000.000 </td></tr>
    <tr><td>ipState</td><td>String</td><td>State of IP address originated from
    2 alphabet letters</td></tr>
    <tr><td>ipPostcode</td><td>String</td><td>Postal Code of IP address originated from</td></tr>
    <tr><td>ipCountryCode</td><td>String</td><td>Country code of IP address originated from</td></tr>
    <tr><td>transactionDate</td><td>String</td><td>Date when transaction occured Typically in the time zone of the processor,
    Format: yyyymmdd, e.g., 20000101</td></tr>
    <tr><td>transactionTime</td><td>String</td><td>Time when transaction occurred. Typically in the time zone of processing end.
    Format: hhmmss, eg. 153059</td></tr>
    <tr><td>localHour</td><td>Integer</td><td>The hour in local time. Value of 0-23</td></tr>
    <tr><td>transactionScenario</td><td>String</td><td>A -- Authorization
    O --  Others</td></tr>
    <tr><td>transactionType</td><td>String</td><td>Type of tranacation:<br/>
    P -- Purchase<br/>
    R -- Refund<br/>
    T -- Transfer<br/>
    O -- Other</td></tr>
    <tr><td>transactionMethod</td><td>String</td><td>I -- Internet (Online) Order<br/>
    P  -- Phone order<br/>
    M -- Mail order<br/>
    O  -- Other</td></tr>
    </table>

**accountInfo.csv** contain the following fields:

<table class="table table-compressed table-striped">
<tr><th>Columns</th><th>Type</th><th>Description</th></tr>
<tr><td>transactionDate</td><td>String</td><td>Date when transaction occured Typically in the time zone of the processor.
Format: yyyymmdd, e.g., 20000101</td></tr>
<tr><td>transactionTime</td><td>String</td><td>Time when transaction occurred. Typically in the time zone of processing end.  
Format: hhmmss, eg. 153059</td></tr>
<tr><td>accountOwnerName</td><td>String</td><td>User name (hashed/encrypted)</td></tr>
<tr><td>accountAddress</td><td>String</td><td>User street address</td></tr>
<tr><td>accountPostalCode</td><td>String</td><td>User postal code</td></tr>
<tr><td>paymentInstrumentAgeInAccount</td><td>Double</td><td>Age of payment instrument in the account </td></tr>
<tr><td>numPaymentRejects1dPerUser</td><td>Integer</td><td>Number of payment rejection in one day of this user</td></tr>
<tr><td>accountCity</td><td>String</td><td>User city</td></tr>
<tr><td>accountState</td><td>String</td><td>User state</td></tr>
<tr><td>accountCountry</td><td>String</td><td>User country (3-alpha)</td></tr>
<tr><td>accountOpenDate</td><td>String</td><td>Account open date. 
Format: yyyymmdd</td></tr>
<tr><td>accountAge</td><td>Integer</td><td>Age of user account in number of days</td></tr>
<tr><td>isUserRegistered</td><td>String</td><td>Whether the user is registered or not</td></tr>
</table>


**fraudTransactions.csv** contain the following fields:
<table class="table table-compressed table-striped">
<tr><th>Columns</th><th>Type</th><th>Description</th></tr>
<tr><td>transactionID</td><td>String</td><td>Unique transaction Id</td></tr>
<tr><td>accountID</td><td>String</td><td>Unique account Id</td></tr>
<tr><td>transactionAmount</td><td>Double</td><td>Transaction amount in currency expressed in transactionCurrencyCode
e.g., 12345.00</td></tr>
<tr><td>transactionCurrencyCode</td><td>String</td><td>Currency code of the transaction.
3 alphabet letters, e.g., USD</td></tr>
<tr><td>transactionDate</td><td>String</td><td>Date when transaction occured Typically in the time zone of the processor.
Format: yyyymmdd, e.g., 20000101</td></tr>
<tr><td>transactionTime</td><td>String</td><td>Time when transaction occurred. Typically in the time zone of processing end.  
Format: hhmmss, eg. 153059</td></tr>
<tr><td>localHour</td><td>Integer</td><td>The hour in local time. Value of 0-23</td></tr>
<tr><td>transactionDeviceId</td><td>String</td><td>Mac Address, or Hardware
 ID like serial number</td></tr>
<tr><td>transactionIPaddress</td><td>String</td><td>Full IP Address for IPv4: 
000.000.000.000 </td></tr>
</table>