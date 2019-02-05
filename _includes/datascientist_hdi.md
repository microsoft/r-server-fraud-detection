<h2 id="step-0">Step 0: Intermediate Directories Creation</h2>
<hr />

<p>In this step, we create or clean intermediate directories both on the edge node and HDFS. These directories will hold all the intermediate processed data sets in subfolders.</p>

<h3>Related Files</h3>

<ul>
  <li><strong>step0_directories_creation.R</strong></li>
</ul>

<h2 id="step-1">Step 1: Merge with Account Information</h2>
<hr />

<p>In this step, we convert the untagged transaction data and account info data into hive table and merge them by the account ID using hive command. Then every transaction will get corresponding account information. The duplicated rows will be removed as well. If this step is used in “production scoring” or “web scoring” stage, we will add a fake label column to the data for rxPredict function work properly later.</p>

<h3>Input</h3>

<ul>
  <li>Two data files: <strong>Untagged_Transactions</strong> and <strong>Account_Info</strong>.</li>
  <li>The working directory on HDFS.</li>
  <li><code>Stage</code>: “Dev” for development, “Prod” for batch scoring, “Web” for web scoring.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Hive table: <code>UntaggedTransactionsAccountUnique</code> (Stage = “Dev”) or <code>TaggedProd</code> (Stage = “Prod” or “Web”)</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step1_merge_account_info.R</strong></li>
</ul>

<h2 id="step-2">Step 2: Tagging</h2>
<hr />

<p>In this step, we tag the untagged data on account level based on the fraud data. The tagging logic is the following. In fraud data, we group it by account ID and sort by time, thus, we have the fraud time period for each fraud account. For each transaction in untagged data, if the account ID is not in fraud data, this transaction is labeled as non fraud (label = 0); if the account ID is in fraud data and the transaction time is within the fraud time period of this account, this transaction is labeled as fraud (label = 1); if the account ID is in fraud data and the transaction time is out of the fraud time period of this account, this transaction is labeled as pre-fraud or unknown (label = 2) which will be removed later.</p>

<h3>Input</h3>

<ul>
  <li><code>Input_Hive_Table</code>: name of the hive table from the merging step with the untagged transactions and account info.</li>
  <li>Path to csv Fraud files with the raw data <strong>Fraud_Transactions.csv</strong></li>
  <li><code>HDFSWorkDir</code>: Working directory on HDFS.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Tagged data.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step2_tagging.R</strong></li>
</ul>

<h2 id="step-3">Step 3: Splitting</h2>
<hr />

<p>In this step, we will hash accountID into 100 different hash code and split the whole data into training(70%) and testing(30%) based on the hash code, e.g., training = <code>hash code &lt;=70</code> and testing = <code>hash code &gt;70</code>. In the same time, transactions with <code>label = 2</code> will be removed.</p>

<h3>Input</h3>

<ul>
  <li>Tagged data set.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Training and Testing sets.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step3_splitting.R</strong></li>
</ul>

<h2 id="step-4">Step 4: Preprocessing</h2>
<hr />

<p>In this step, We use <code>rxSummary</code> function to get the missing information. Missing values of <code>localHour</code> will be filled with <code>-99</code>. Missing values for the rest columns will be filled with <code>0</code>. We also fix some data entries and convert a few variables to numeric.</p>

<h3>Input</h3>

<ul>
  <li><code>HDFSWorkDir</code>: Working directory on HDFS.</li>
  <li><code>HiveTable</code>: Input data name of Hive table to be preprocessed.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Hive table with preprocessed data.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step4_preprocessing.R</strong></li>
</ul>

<h2 id="step-5">Step 5: Create Risk Tables</h2>
<hr />

In this step, we create risk tables for categorical variables, such as location related variables. 

{% include risk_summary.md %}

<p>rxSummary function is used to get the count of fraud and non-fraud for variables need to be converted. Then, for each variable, we combine the count for fraud and non-fraud to calculate risk table. All risk tables will be put into one list and saved to model directory on edge node.</p>

<h3>Input</h3>

<ul>
  <li><code>LocalWorkDir</code> and <code>HDFSWorkDir</code>: working directories on HDFS and local edge node.</li>
  <li><code>HiveTable</code>: name of the Hive table containing the preprocessed training set to be used to create risk tables.</li>
  <li><code>smooth1</code> and <code>smooth2</code>: smoothing parameters used to compute the risk values.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Risk tables embedded in a list <code>Risk_list</code>, saved on the edge node.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step5_create_risk_tables.R</strong></li>
</ul>

<h2 id="step-6">Step 6: Feature Engineering</h2>
<hr />

<p>This step does feature engineering for an input data set. We will generate three groups of new features:</p>

<ul>
  <li>Numerical risk variables transformed from categorical variables based on the risk tables generated in step 5.</li>
  <li>Binary variables. For example, high amount flag and address mismatch flags.</li>
  <li>Aggregates. For example, completed transactions of a customer in past 1 day and 30 days.</li>
</ul>

<p>To calculate the aggregates, since there is no existing rx-function we can use, we have to use regular R functions. In order to make it scalable to big data, we hash the account ID and split the data into small chunks by hash code. Chunks are account ID exclusive so that we can safely apply aggregates calculation to each chunk in parallel. Finally, the output chunks will be combined to one xdf file.</p>

<h3>Input</h3>

<ul>
  <li><code>LocalWorkDir</code> and <code>HDFSWorkDir</code>: working directories on HDFS and local edge node.</li>
  <li><code>HiveTable</code>: name of the Hive table containing the preprocessed data set to which new features will be added.</li>
  <li><code>Stage</code>: “Dev” for development, “Prod” for batch scoring, “Web” for web scoring.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Preprocessed xdf file with new features and correct variable types.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step6_feature_engineering.R</strong></li>
</ul>

<h2 id="step-7">Step 7: Training</h2>
<hr />

<p>In this step, we will train a GBT model with the training data. Note that, the label is imbalanced (much more non-fraud than fraud), and this can be handled by “unbalancedSets” argument in “rxFastTrees” function. The trained model will be saved to the model directory on local edge node.</p>

<h3>Input</h3>

<ul>
  <li><code>LocalWorkDir</code> and <code>HDFSWorkDir</code>: working directories on HDFS and local edge node.</li>
  <li><code>Input_Data_Xdf</code>: training data.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Trained GBT model object.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step7_training.R</strong></li>
</ul>

<h2 id="step-8">Step 8: Prediction</h2>
<hr />

<p>In this step, we do prediction (scoring) on the model created in step 7. If Stage is “Dev” or “Prod”, the model object is loaded from model directory on edge node. If Stage is “Web", the model object is directly passed. In “Dev” Stage, we will also create a hive table for the scored data set. The hive table will be ingested by PowerBI for visualization.</p>

<h3>Input</h3>

<ul>
  <li><code>LocalWorkDir</code> and <code>HDFSWorkDir</code>: working directories on HDFS and local edge node.</li>
  <li><code>Input_Data_Xdf</code>: input data name of xdf file to be scored.</li>
  <li><code>Stage</code>: “Dev” for development, “Prod” for batch scoring, “Web” for web scoring.</li>
</ul>

<h3>Output</h3>

<ul>
  <li>Scored data set.</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step8_prediction.R</strong></li>
</ul>

<h2 id="step-9">Step 9: Evaluation</h2>
<hr />

<p>In this step, we will evaluate the scored data set if the ground truth label exists (thus, only performed in development stage). We create both transaction and account level metrics. For transaction level metrics, <code>rxRoc</code> function is used to get the ROC curve and <code>rxAuc</code> function is used to calculate the AUC. For account level metrics, we import the data in memory and use customized function to get the result.</p>

<h3>Input</h3>

<ul>
  <li><code>HDFSWorkDir</code>: working directories on HDFS and local edge node</li>
  <li><code>Scored_Data_Xdf</code>: scored data set</li>
</ul>

<h3>Output</h3>

<ul>
  <li>AUC.</li>
  <li>Plotted ROC curve.</li>
  <li>Account level metrics and plots</li>
</ul>

<h3>Related Files</h3>

<ul>
  <li><strong>step9_evaluation.R</strong></li>
</ul>

<h2 id="updating">Updating the Production Stage Directory (“Copy Dev to Prod”)</h2>
<hr />

<p>At the end of the main function of the script <strong>development_main.R</strong>, the <strong>copy_dev_to_prod.R</strong> script is invoked in order to copy (overwrite if it already exists) the model, statistics and other data from the Development Stage to a directory of the Production or Web Scoring stage.</p>

<p>If you do not wish to overwrite the model currently in use in a Production stage, you can either save them to a different directory, or set <code>update_prod_flag</code> to <code>0</code>. If you are running the solution at the very first time, make sure to set the flag to 1.</p>

<h2 id="production">Production Stage</h2>
<hr />

<p>In the Production stage, the goal is to perform a batch scoring.</p>

<p>The script <strong>production_main.R</strong> will complete this task by invoking the scripts described above. The batch scoring can be done either:</p>

<ul>
  <li>In-memory : The input should be provided as data frames. All the preprocessing and scoring steps are done in-memory on the edge node (local compute context). In this case, the main batch scoring function calls the R script <strong>in_memory_scoring.R</strong>.</li>
  <li>Using data stored on HDFS: The input should be provided as paths to the Production data sets. All the preprocessing and scoring steps are one on HDFS in Spark Compute Context.</li>
</ul>

<p>When the data set to be scored is relatively small and can fit in memory on the edge node, it is recommended to perform an in-memory scoring because of the overhead of using Spark which would make the scoring much slower.</p>

<p>The script:</p>

<ul>
  <li>Lets the user specify the paths to the Production working directories on the edge node and HDFS (only used for Spark compute context).</li>
  <li>Lets the user specify the paths to the Production data sets <strong>Untagged_Transactions</strong> and <strong>Account_Info</strong> (Spark Compute Context) or point to them if they are data frames loaded in memory on the edge node (In-memory scoring).</li>
</ul>

<p>The computations described in the Development stage are performed, with the following differences:</p>

<ul>
  <li>No tagging step. A fake label column will be given instead.</li>
  <li>No splitting into a training and testing set, since the whole data is used for scoring.</li>
  <li>No creating risk tables. The risk tables generated in development stage will be loaded for use.</li>
  <li>No training. The GBT model created in the Development Stage is loaded and used for predictions</li>
  <li>No model evaluation since usually we don’t have ground truth label for new coming data.</li>
</ul>

<p>Warning: in case you get the following error: “Error: file.exists(inData1) is not TRUE”, you should reset your R session with <code>Ctrl</code> + <code>Shift</code> + <code>F10</code> (or <code>Session</code> -&gt; <code>Restart R</code>) and try running it again.</p>

<h2 id="deploy">Deploy as a Web Service</h2>
<hr />

<p>In the script <strong>web_scoring_main.R</strong>, we define a scoring function and deploy it as a web service so that customers can score their own data sets locally/remotely through the API. Again, the scoring can be done either:</p>

<ul>
  <li>In-memory : The input should be provided as data frames. All the preprocessing and scoring steps are done in-memory on the edge node (local compute context). In this case, the main batch scoring function calls the R script <strong>in_memory_scoring.R</strong>.</li>
  <li>Using data stored on HDFS: The input should be provided as paths to the Production data sets. All the preprocessing and scoring steps are one on HDFS in Spark Compute Context.</li>
</ul>

<p>When the data set to be scored is relatively small and can fit in memory on the edge node, it is recommended to perform an in-memory scoring because of the overhead of using Spark which would make the scoring much slower.</p>

<p>This is done in the following way:</p>
<ol>
<li>
Log into the ML server that hosts the web services as admin. Note that even if you are already on the edge node, you still need to perform this step for authentication purpose.
</li>
<li>
Specify the paths to the working directories on the edge node and HDFS.
</li>
<li>
Specify the paths to the input data sets <strong>Untagged_Transactions</strong> and <strong>Account_Info</strong> or point to them if they are data frames loaded in memory on the edge node.
</li>
<li>
Load the static .rds files needed for scoring and created in the Development Stage. They are wrapped into a list called <code>model_objects</code> which will be published along with the scoring function.
</li>
<li>
Define the web scoring function which calls the steps like for the Production stage.
</li>
<li>
Publish as a web service using the <code>publishService</code> function. Two web services are published: one for the string input (Spark Compute Context) and one for a data frame input (In-memory scoring in local compute context). 
In order to update an existing web service, use the <code>updateService</code> function to do so.
Note that you cannot publish a new web service with the same name and version twice, so you might have to change the version number.
</li>
<li>
Verification:
<ul>
  <li>
    Verify the API locally: call the API from the edge node.
  </li>
  <li>
    Verify the API remotely: call the API from your local machine. You still need to remote login as admin from your local machine in the beginning. It is not allowed to connect to the edge node which hosts the service directly from other machines. The workaround is to open an ssh session with port 12800 and leave this session on. Then, you can remote login. Use getService function to get the published API and call the API on your local R console.
  </li>
</ul>
</li>
</ol>

<h2 id="using">Using Your Own Data Set</h2>
<hr />

<p>A new data set can be used for either or both the Modeling and the Production pipeline. However, for this template to work as is, it should follow these requirements:</p>

<ul>
  <li>The data schema should be the same for files <strong>Untagged_Transactions</strong>, <strong>Account_Info</strong> and <strong>Fraud_Transactions</strong>.</li>
</ul>

