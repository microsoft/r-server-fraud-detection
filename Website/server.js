var express = require('express');
var Connection = require('tedious').Connection;
var Request = require('tedious').Request;
var TYPES = require('tedious').TYPES;

var fs = require('fs');
var util = require('util');
var logFileName = __dirname + '/debug.log';

var app = express();
var exphbs  = require('express-handlebars');
app.engine('handlebars', exphbs({defaultLayout: 'main'}));
app.set('view engine', 'handlebars');

app.use(express.static('public'));




//
// DB Connection
//
var args = process.argv.slice(2);
if (args.length>0) {
    var user = args[0];
    var pw = args[1];    
}
else {
    var user = 'XXYOURSQLUSER';
    var pw = 'XXYOURSQLPW'; 
}


var con = new Connection({ //fix this with fraud db info
	userName: user,
    password: pw,
    server: 'localhost',
    // When you connect to Azure SQL Database, you need encrypt: true
     options: {  encrypt: true, database: 'Fraud_R' }
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

// Kill the server
app.get('/kill', function (req, res) {
     setTimeout(() => process.exit(), 500);
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
      }); 
        
    });  
    // pass the entire record to the stored procedure
    request.addParameter('inputstring', TYPES.VarChar, record);
    con.callProcedure(request); 
    con.close;
  
    
});

//log to file
var logFile = fs.createWriteStream(logFileName, { flags: 'a' });
var logProxy = console.log;
console.log = function (d) { //
    logFile.write(util.format(new Date() + ": " + d || '') + '\r\n');
    logProxy.apply(this, arguments);
};

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});