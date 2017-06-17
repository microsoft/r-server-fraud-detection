// JavaScript Document

$(document).ready ( function () {
			$("#resultArea").hide();
            $("#status").hide();


		$("#resetBtn").click(function(){
        //	empty the table and change the purchase back to 0.
            $("#myTable > tbody").html("");
             $("#status").removeClass('show');   
             $("#status").addClass('hide');
            document.getElementById('result').innerHTML = "Total Purchase: $0" 
			$("#resultArea").fadeOut();
		});	
        
       
        
        $("#submitBtn").click(function(){ 
            acctID =  $("#claimantID").val(); 
         // check to make sure there is an item in the cart      
        if ($('#myTable tr').length > 0 ) {
            // also make sure the account id is present.
            if (acctID !== '') {
                // show the status message and call scoreClaim
                $("#status").removeClass('hide');
                $("#status").addClass('show');
                $("#status").fadeIn();
                var amt = recalc();
                scoreClaim( acctID, amt);  
                } else {
                    // no account ID present
                $("#status").removeClass('show');   
                $("#status").addClass('hide');
                $("#resultArea").html('Please enter your Account ID and try again.');
                $("#resultArea").removeClass('alert-success');
                $("#resultArea").addClass('alert-danger');
                $("#resultArea").fadeIn();
                }
        } else {
                   // no items in the cart
                $("#status").removeClass('show');   
                $("#status").addClass('hide');
                $("#resultArea").html('You must have at least one item before you can Purchase.');
                $("#resultArea").removeClass('alert-success');
                $("#resultArea").addClass('alert-danger');
                $("#resultArea").fadeIn();
            }
	}); 
    
    
    $(".addItem").click (function(){
        // Adding items to the cart - just hardcoding a few items here
        switch (this.id) {
            case "heart":
                contents = '<tr><td ><img class="img-rounded" src="img/heart.jpg" height="60px" width="60px"></td>'
                contents = contents + '<td>Black and White Diamond Heart</td>'
                contents = contents + '<td>$<span class="val">130</span></td>'
            break;
            case "earrings":
                contents = '<tr><td ><img class="img-rounded" src="img/earrings.jpg" height="60px" width="60px"></td>'
                contents = contents + '<td>Diamond Pave Earrings</td>'
                contents = contents + '<td>$<span class="val">569</span></td>'
            break;
            case "bracelet":
                contents = '<tr><td ><img class="img-rounded" src="img/bracelet.jpg" height="60px" width="60px"></td>'
                contents = contents + '<td>Diamond Tennis Bracelet</td>'
                contents = contents + '<td>$<span class="val">360</span></td>'
            break;  
            case "ring":
                contents = '<tr><td ><img class="img-rounded" src="img/ring.jpg" height="60px" width="60px"></td>'
                contents = contents + '<td>Diamond Engagement Ring</td>'
                contents = contents + '<td>$<span class="val">2100</span></td>'
            break;                      
        }
        contents = contents + '<td><button class="deleteMe btn btn-sm " >x</button></td></tr>'
		$('#myTable > tbody:last-child').append(contents);
	    recalc()
        });
     
     // can't use $(".deleteMe").click here because the items are dynamically added, not all present at the start.    
     $(document).on('click', 'button.deleteMe', function () { 
        $(this).closest('tr').remove();
        recalc();
     });
        
		function formatTotal(x) {
			x = Math.round(x);
    		return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
		}
        
        function recalc(){
            // iterate through all the values in the table (class="val")
               var resultVal = 0.0;
               $(".val").each ( function() {
				   var itemval = $(this).text();
				   itemval = itemval.replace('', "0"); 
                   resultVal += parseFloat ( itemval.replace(/\s/g,'').replace(',','.'));
                });
				resultVal = formatTotal(resultVal);
                document.getElementById('result').innerHTML = "Total Purchase $" + resultVal;
                return(resultVal)
        }
    
            });
            