#!/usr/bin/env bash

# put R code in users home directory
git clone  --single-branch  https://github.com/Microsoft/r-server-fraud-detection.git  fraud
cp loans/RSparkCluster/* /home/$1
chmod 777 /home/$1/*.R
rm -rf loans
sed -i "s/XXYOURSQLPW/$2/g" /home/$1/*.R

# Configure edge node as one-box setup for R Server Operationalization
/usr/local/bin/dotnet /usr/lib64/microsoft-r/rserver/o16n/9.1.0/Microsoft.RServer.Utils.AdminUtil/Microsoft.RServer.Utils.AdminUtil.dll -silentoneboxinstall "$2"

# turn off telemetry 
sed -i 's/options(mds.telemetry=1)/options(mds.telemetry=0)/g' /usr/lib64/microsoft-r/3.3/lib64/R/etc/Rprofile.site
sed -i 's/options(mds.logging=1)/options(mds.logging=0)/g' /usr/lib64/microsoft-r/3.3/lib64/R/etc/Rprofile.site
