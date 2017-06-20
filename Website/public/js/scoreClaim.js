// JavaScript Document
var scoreClaim = function(id, amt){ 
    //first get the rest of the data for this id
    record = lookupData(id, amt)
    // call /predict to get res.prob, the probability of returning the shipment
    $.ajax({
    url: '/predict',
    type: 'GET',
    data: { record: record },
    contentType:"application/json; charset=utf-8",
    error: function(xhr, error){
        console.log(xhr); console.log(error);
    }, 
    success: function(res) { 
       console.log("AccountID: " + id  + " transactionAmt: " + amt )
       console.log("Predicted probability: " + res.pred )
            // now use the probability to display one of two message 
            if (res.pred > 0.5) {  //problem with this order; 
                $("#resultArea").html('There is a problem with this order.  Please call 800-555-2222 for more information');
                        $("#resultArea").removeClass('alert-success');
                        $("#resultArea").addClass('alert-danger');
                        

                    } else { // no problem with the order
                $("#resultArea").html('Thank you for submitting your order. You will receive an email with tracking information shortly.');
                        $("#resultArea").removeClass('alert-danger');
                        $("#resultArea").addClass('alert-success');
                    }
            // make sure result is visible
            $("#resultArea").removeClass('hide');
            $("#resultArea").addClass('show');		
            // remove the "click here for status" section
            $("#status").removeClass('show');   
            $("#status").addClass('hide');
            $("#resultArea").fadeIn();
        }   
        
       });	
}	

var lookupData = function(custID, amt){
 amt = parseFloat(amt.replace(/,/g, ''));
// the rest of the record would be looked up in a customer database.
// for this demo we are simply supplying that info directly for our four test accounts
var custData;
switch(custID) {
    case 'A844428158473288':
        custData = 'USD,NULL,20130922,214008,13,A,P,NULL,NULL,NULL,50.37,nevada,89410,us,FALSE,NULL,en-US,CREDITCARD,VISA,NULL,NULL,NULL,89460,NV,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL,2013-09-22 21:40:08.000,NULL,NULL,89460,NULL,NV,US,NULL,14,FALSE,0,0,2013-09-22 21:40:08.000,NULL,NULL,0,6';
        break;
    case 'A1688853039413210':
        custdata = 'USD,NULL,20130929,214541,15,A,P,NULL,NULL,NULL,67.224,iowa,52057,us,FALSE,NULL,en-US,CREDITCARD,MC,NULL,NULL,NULL,52237,IA,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL,2013-09-29 21:45:41.000,NULL,NULL,52057,NULL,IA,US,NULL,1131,FALSE,0,0,2013-09-29 21:45:41.000,NULL,NULL,0,7';
        break;
    case 'A1055520561112300':
        custData = 'USD,NULL,20130929,215450,15,A,P,NULL,NULL,NULL,68.97,oklahoma,73102,us,FALSE,NULL,en-US,CREDITCARD,VISA,NULL,NULL,NULL,73064,OK,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL,2013-09-29 21:54:50.000,NULL,NULL,76548,NULL,TX,US,NULL,2000,FALSE,205.8951389,0,2013-09-29 21:54:50.000,NULL,NULL,0,16';
        break;
    default:
        custData = 'USD,NULL,20130922,214758,16,A,P,NULL,NULL,NULL,206.248,west virginia,25560,us,FALSE,NULL,en-US,CREDITCARD,MC,NULL,NULL,NULL,25703,WV,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL,2013-09-22 21:47:58.000,NULL,NULL,25703,NULL,WV,US,NULL,687,FALSE,5.102777778,0,2013-09-22 21:47:58.000,NULL,NULL,0,16';
} 
var record = 'xxxTRANSID,'+ custID + ',' + amt + ',' + amt + ',' + custData;

return(record);
}
