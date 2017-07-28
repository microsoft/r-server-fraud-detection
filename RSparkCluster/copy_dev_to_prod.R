##########################################################################################################################################
## This R script will define a function, copy_to_prod, that: 
## 1. Cleans up an already existing directory or create it on the edge node, ProdModelDir. 
## 2. Copies to that directory: Summary statistics, Bins, Logistic Regression model, etc. from the Development directory.  

## Input : DevModelDir: Path to the directory on the edge node storing the summary statistics, bins, model, etc.
##         ProdModelDir: Path o the directory where the contents of DevModelDir should be copied.
## Output: ProdModelDir with data trasferred from DevModelDir.


## It should be applied:
## a) If running the Production stage for the first time. 
## b) If you want to run the Production stage with a newly trained model; the older one will be overwritten.  
##########################################################################################################################################

copy_dev_to_prod <- function(DevModelDir, ProdModelDir){
  
  # Clean or create a new directory in the Prodution directory. 
  if(dir.exists(ProdModelDir)){
    system(paste("rm -rf ", ProdModelDir, sep = "")) # remove the directory if exists
    system(paste("mkdir -p -m 777 ", ProdModelDir, sep = "")) # create a new directory
  } else {
    system(paste("mkdir -p -m 777 ", ProdModelDir, sep = "")) # make new directory if doesn't exist
  }
  
  # Copy the model, statistics and other data from the Development directory to the Production directory. 
  system(paste("cp ", DevModelDir, "/*.rds ", ProdModelDir, sep = ""))
  
}




