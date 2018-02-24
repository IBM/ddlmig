#!/bin/bash
######################################################################## 
# Copyright(r) IBM Corporation
# Written by Vikram Khatri vikram.khatri@us.ibm.com
#
######################################################################## 

JAVA=$(db2jdkp)/bin/java

if [[ ! -f $JAVA ]] ; then 
	echo Java not found. Exiting ...
	exit 1
fi

if [ $# -eq 0 ] ; then
    echo "Usage: $0 <DDLLogFileToAnalyze>"
    exit 1
fi

LOGFILENAME=$1

PROG=com.ibm.migr.data.AnalyzeLog
CLPATH="-cp ./migrkit.jar:$CLASSPATH"

$JAVA $CLPATH $PROG $LOGFILENAME

