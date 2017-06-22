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
    case 'A1055521358474530':       
        custData = 'USD,NULL,20130409,102958,14,A,P,NULL,NULL,NULL,92.97,dubayy,0,ae,FALSE,NULL,en-US,CREDITCARD,AMEX,NULL,NULL,NULL,33071,FL,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,0,4,NULL';
        break;
    case 'A914800341525449':
        custData = 'USD,NULL,20130409,122427,7,A,P,NULL,NULL,NULL,108.49,massachusetts,2118,us,FALSE,NULL,en-US,CREDITCARD,VISA,NULL,NULL,NULL,1702,MA,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL';
        break;
    case 'A1688852355371910':
        custData = 'USD,NULL,20130409,110900,6,A,P,NULL,NULL,NULL,99.47,florida,32114,us,FALSE,NULL,en-US,CREDITCARD,VISA,NULL,NULL,NULL,32746,FL,US,NULL,NULL,NULL,NULL,NULL,NULL,M,NULL,1,0,NULL';
        break;
    default:
        custData = 'USD,NULL,20130409,104848,NULL,A,P,NULL,NULL,NULL,121.242,maharashtra,411001,in,FALSE,NULL,en-US,CREDITCARD,VISA,NULL,NULL,NULL,98033,WA,US,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,3,0,NULL';
        break;        
} 

var record = 'xxxTRANSID,'+ custID + ',' + amt + ',' + amt + ',' + custData;

return(record);
}
