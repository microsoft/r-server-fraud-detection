##########################################################################################################################################
## This R script will do the following:
## 1. Create or clean up an intermediate directory, LocalIntermediateDir, on the edge node. 
## 2. Create or clean up an intermediate directory, HDFSIntermediateDir, on HDFS. 

##########################################################################################################################################

# Intermediate folders paths one on the edge node and one on HDFS.
LocalIntermediateDir <- file.path(LocalWorkDir, "temp")
HDFSIntermediateDir <- file.path(HDFSWorkDir,"temp")

# Clean up the folders if they already exist and create them otherwise.
if(dir.exists(LocalIntermediateDir)){
  system(paste("rm -rf ",LocalIntermediateDir,"/*", sep="")) # clean up the directory if exists
} else {
  dir.create(LocalIntermediateDir, recursive = TRUE) # make new directory if doesn't exist
}

if(rxHadoopFileExists(HDFSIntermediateDir)){
  rxHadoopRemoveDir(HDFSIntermediateDir, skipTrash = TRUE)
  rxHadoopMakeDir(HDFSIntermediateDir)
} else {
  rxHadoopMakeDir(HDFSIntermediateDir)
}

# Grant access authority for the edge node intermediate folder.
system(paste("chmod g+s ", LocalIntermediateDir, sep=""))
system(paste("setfacl -d -m g::rwx ", LocalIntermediateDir, sep=""))
system(paste("setfacl -d -m o::rwx ", LocalIntermediateDir, sep=""))