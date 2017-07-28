// JavaScript Document
			
(function(R) {

	$("#submitBtn").click(function(){		
		exeScript()
	}); 
	
		/* Execute the repository script to get score for this claim */
	var exeScript = function() {
		
		/* callback configuration */
		var callback = {
			scope : this,

			// success callback
			success : function(result) {
				//area.className = '';
				var objs = result.deployr.response.workspace.objects;
				score = objs[0].value;
				
				// Use the score from the script to display the appropriate message
				if (score < 3) {
				$("#resultArea").html(' Thank you for submitting your claim. It has been fast tracked for processing.');
				$("#resultArea").removeClass('alert-danger');
				$("#resultArea").addClass('alert-success');
			} else {
				$("#resultArea").html('Thank you for submitting your claim. Please allow 2-4 weeks for review.');
				$("#resultArea").removeClass('alert-success');
				$("#resultArea").addClass('alert-danger');				
			}
				
			$("#resultArea").fadeIn();

				
			},
			// failure callback
			failure : function(result) {
				var msg = result;
			    
				if (result.deployr) {
					msg = result.deployr.response.error;
					$("#resultArea").html(msg);
				}
			}
		};

		/* configuration input for repository script execution */
		
		//inputList gathers up all the form values and formats them for DeployR
		var inputList = [];
		$(".form-control").each(function() {
		  inputList.push(R.RDataFactory.createString($(this).attr("id"), $(this).val() || ' '));
		});
		
		//send all the form values as inputs, and retrieve 'score' from the script execution
		
		var scriptConfig = {
			filename : 'insuranceFraud',
                        author : 'sheri',
						inputs : inputList,
					robjects: ['score'],
					preloadfilename: 'rtsScoreFraud.R',
					preloadfileauthor: 'sheri',
					blackbox: true
		};
		
		// execute RScript
		R.DeployR.repositoryScriptExecute(scriptConfig, callback);
	};
})(window.Revolution);