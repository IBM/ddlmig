#!/bin/bash
######################################################################## 
# Written by Vikram Khatri
######################################################################## 

JAVA=$(db2jdkp)/bin/java

if [[ ! -f $JAVA ]] ; 
then 
	echo Java not found. Exiting ...
	exit 1
fi

mkdir -p logs

LOGFILE=./logs/migrkit$(date +%Y%m%d%H%M%S).log
SRCDBPROPS=./src.properties
DSTDBPROPS=./dst.properties
DARGS="-DSRCDB=$SRCDBPROPS -DDSTDB=$DSTDBPROPS"
CLPATH="-cp ./migrkit.jar:./db2jcc4.jar:./db2jcc_license_cu.jar"

PROG=com.ibm.migr.data.GenInitialObjects
CHECKDSTDB=false

# Output directory
SCHEMA=EDW5P1

$JAVA -Xmx4096m $CLPATH $DARGS $PROG $SCHEMA $CHECKDSTDB | tee $LOGFILE
