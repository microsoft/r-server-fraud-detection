var express = require('express');
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;


var app = express();
var exphbs  = require('express-handlebars');
app.engine('handlebars', exphbs({defaultLayout: 'main'}));
app.set('view engine', 'handlebars');

app.use(express.static('public'));


//
// DB Connection
//


var con = new Connection({ //fix this with fraud db info
	userName: 'rdemo',
    password: 'D@tascience',
    server: 'localhost',
    // When you connect to Azure SQL Database, you need encrypt: true
     options: {  encrypt: true, database: 'Fraud' }
});

con.on('connect', function(err) {
	console.log('DB Connection ' + (err ? '~~~ Failure ~~~' : 'Success'));
    if (err) console.log(err);
});

//
// Put your routes here
//

// Home Page
app.get('/', function (req, res) {
            res.render('home') 
});

// Test Page
app.get('/test', function (req, res) {
            res.render('test') 
});

// predict function, called from scoreClaim.js



app.get('/predict', function (req, res) {
    var request = new Request('ScoreOneTrans', function(err, rowCount) {
    if (err) {
        console.log(err);
        }  
       // console.log("Rows Returned: " + rowCount )      
    });
    
    var record = req.query.record;
    console.log (record)
    request.on('row', function(col) {
          if (col[0].value === null) {
            console.log('NULL');
          } else {
            // values to return - the predicted probability
            value = col[0].value;   
          }

         res.json({ pred: value });
         request.on('doneInProc', function(rowCount, more) { 
            console.log(rowCount + ' rows returned'); 
            con.close;
      }); 
        
    });  
    // pass the entire record to the stored procedure
    request.addParameter('inputdata', TYPES.VarChar, record);
    con.callProcedure(request);   
    
});

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});