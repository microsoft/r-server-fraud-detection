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
// the rest of the record would be looked up in a SQL database, for this demo we are simply supplying the info directly
var record =  '6C0E80FA-6988-4823-B0F5-BA49EBCBD99E,'+ custID + ',' + amt + ',' + amt + ',USD,'
var record = record + '"",20130401,2932,21,A,P,"","","",'
var record = record + '201.8,minas gerais,30000-000,br,False,"",pt-BR,CREDITCARD,VISA,"","","",30170-000,MG,BR,'
var record = record + '"","","","","","",M,"",1,0,"","","",30170-000,"",MG,BR,"",1,False,0.000694444444444444,0,0,0,"",0'
return(record);
}
