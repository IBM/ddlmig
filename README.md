# ddlmig
Db2 DDL migration and data migration through HPU
================================================

This tool is used to migrate DDL and Data from a Db2 system to another Db2 system such as IBM Integrated Analytics System.

1. The tool is designed to allow each partitioned and local indexes (as many as possible) will reside on their own table spaces on target Db2 System.
2. Each non-partitioned index will reside on its own table space which will have name of index in the table space.
3. The table space naming conventions are :

  * "SCHEMA_TABLE_DATA" for data and "SCHEMA_TABLE_IDX"  for index for local indexes.
  * "SCHEMA_TABLE_PARTNAME_DATA" for data
  * "SCHEMA_TABLE_PARTNAME_IDX" for partitioned data and indexes.
  * "SCHEMA_INDEXNAME_IDX" for non-partitioned indexes (each for separate index)

4. The overall storage groups are defined as follows:

```
CREATE STOGROUP stogroupdata ON '/data/data1','/local/data2';
CREATE STOGROUP stogroupidx ON '/data/idx1';
CREATE STOGROUP stogrouptemp ON '/data/temp';
```
5. The buffer pools are just limited to 3 as shown:
```
CREATE BUFFERPOOL BP_DATA_32K PAGESIZE 32768;
CREATE BUFFERPOOL BP_IDX_32K PAGESIZE 32768;
CREATE BUFFERPOOL BP_TEMP_32K PAGESIZE 32768;
```
The name of the storage group and their mount points and buffer pools can be specified through src.properties file. For example:

Note: These are the best practices and it is not necessary that you have to adhere to these. The tool is designed to make moving parts as less as possible and still gain maximum benefits.

The main contentious point could be each table and index in their own table space. When we have choices, you will get as many differing opinions.

From recoverability perspective, it makes things much easier. With automatic storage, the management of table spaces is not a hassle any more and it gives a much better recoverability options.  

src.properties
==============
```
server=<IPAddressOfTheSourceDatabase>
port=<DB2SourceDatabasePortNumber>
dbname=<DB2SourceDatabaseName>
userid=<DB2SourceDatabaseUserID>
# You can also specify plain text password instead of encrypted password.
# To generate the encrypted password, use crypt.sh to generate the encrypted password.
password=<EncryptedDB2SourceDatabasePassword> # use utility crypt.sh to encrypt the password
autoCommit=true
fetchSize=1000
enableSysplexWLB=false
CONNECTNODE=0
currentSchema=<DB2SourceDatabaseSchemaNameForMigration>
applicationName=<ApplicationName-WriteAnyName>
clientUser=<WriteAnyNameforClientUser>
clientAccountingInformation=<WriteAnyNameforAccounting>
clientHostname=<WriteSourceServerHostName>
# If all schema required then specify ALL otherwise use comma separated list
migrationSchemaList=<CommaSeparatedListofSchema|ALL>
# Initial size increment - increase by a factor
initialSizeIncrement=0.50 # This is the factor by which you would get the size of initial table space
# How may target MLN are there
targetMLNCount=<TotalNumberofDatabasePartitions> # Just count number of lines in db2nodes.cfg and that is MLN count
# if bufferpool is defined, we will use this and not from the source
dataBufferpool=BP_DATA_32K,32768 # Name of the data bufferpool and size
idxBufferpool=BP_IDX_32K,32768 # Name of the index bufferpool and size
tempBufferpool=BP_TEMP_32K,32768 # Name of the temporary bufferpool and size
# extent of target tablespaces extent size
extentSize=8
# if mapdbpg is defined then we will use this and not from the source
mapdbpg=ALLDATANODES:DATANODES:1,2,3,45,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52|SINGLEDATANODE:FIRSTHOST:0
stogroupdata=stogroupdata|'/data/data1','/local/data1' # The paths for data storage group. Command separated list for multiple
stogroupidx=stogroupidx|'/local/idx1' # The paths for index storage group. Command separated list for multiple
stogrouptemp=stogrouptemp|'/data/temp1' # The paths for temporary storage group. Command separated list for multiple
```
The initial size of the table space is calculated from the source database. This is an important step to allocate required space upfront so that Db2 and GPFS are not in conflict. This makes things faster for the data load. It is a good practice. The src.properties file has a paratemter `initialSizeIncrement` through which you can scale up or down the initial size calculation.

DDL Migration
=============

`migr.sh` calls `com.ibm.migr.data.GenInitialObjects` and it expects source and database connection informations. The JDBC drivers must on the class path.

HPU Migration
=============

The Db2 High Performance Unload is used as this is the fastest method to unload and load data.

The HPU has capabilities in which it will run on source database on each database partition. It will then transfer data using inetd to the target. The HPU must be running on each node on the target as xinetd daemon listening on a specific port.

When HPU sends data from source to multiple targets using inetd/xinetd protocol, it will invoke LOAD utility on target and copy the data from tcp/ip socket to the pipe so that LOAD can consume the data.

The HPU will repartition data on source and send the desired data to the target database partition so this method is the best when MLN topology is changing from source to the target.

`hpumigr.sh` calls `com.ibm.migr.data.GenHPUScripts` and it will detect the source MLN and reads target MLN count from the src.properties and generates migration scripts with proper syntax.
