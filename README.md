# ddlmig
Db2 DDL migration and data migration through HPU
================================================

This tool is used to migrate DDL and Data from a Db2 system to another Db2 system such as IBM Integrated Analytics System.

1. The tool is designed to allow each partitioned and local indexes (as many as possible) will reside on their own table spaces on target Db2 System.
2. Each non-partitioned index will reside on its own table space which will have name of index in the table space.
3. The table space naming conventions are:

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
The initial size of the table space is calculated from the source database. This is an important step to allocate required space upfront so that Db2 and GPFS are not in conflict. This makes things faster for the data load. It is a good practice. The src.properties file has a parameter `initialSizeIncrement` through which you can scale up or down the initial size calculation.

## HPU Installation

HPU needs to be installed on all hosts on source and target. Please make sure that the HPU version is same on all hosts on source and target.

The example given here is on IBM Integrated Analytics System (IIAS).

* Copy xinetd rpm somewhere on `/opt/ibm/appliance/storage/scratch` directory on host.

* Copy HPU binary for the GA version and any fix pack available in the same directory on host.

* Get into docker container

 ```
docker exec -it dashDB bash
```
* Optional: Create `runall` script and copy this to all hosts in the system. The example here is for one third rack which has 3 machines. The full rack will have 7 machines.

```
# cat /bin/runall
#!/bin/bash

NODES="node0101-fab node0102-fab node0103-fab"

for host in $NODES
do
   echo Running command on $host
   ssh $host "$@"
done

# chmod +x  /bin/runall
# scp /bin/runall node0102-fab:/bin
# scp /bin/runall node0103-fab:/bin
```

* Install xinetd and HPU on all hosts

```
# runall "rpm -ivh /scratch/xinetd-2.3.15-13.el7.ppc64le.rpm"			  
# runall "systemctl enable xinetd"
# runall "systemctl start xinetd"
```
## Install HPU on all hosts

Assuming that you have copied HPU GA and Fixpack in `/scratch/hpu`
```
# runall /scratch/hpu/HPU6100/install_hpu.sh -d /opt/ibm/HPU/V6.1 -s
# runall /scratch/hpu/HPU6101/install_hpu.sh -d /opt/ibm/HPU/V6.1 -s
```
Add 3 entries in `/etc/xinet.d/db2hpudm61` for better Performance
```
cps             = 5000 10
instances       = 5000
per_source      = 100
```

For example:

```
service db2hpudm61
{
    disable         = no
    flags           = REUSE
    socket_type     = stream
    protocol        = tcp
    wait            = no
    user            = root
    server          = /opt/ibm/HPU/V6.1/bin/db2hpudm
    server_args     = --tophpu /opt/ibm/HPU/V6.1 --loglevel 3 --inetd --logfile /var/log/hpu/db2hpudm61.log
    log_on_failure += USERID HOST
    log_on_success += USERID PID HOST DURATION
    cps             = 5000 10
    instances       = 5000
    per_source      = 100
}
```

And, do not forget to add this to all hosts.

```
# scp /etc/xinetd.d/db2hpudm61 node0102-fab:/etc/xinetd.d/db2hpudm61
# scp /etc/xinetd.d/db2hpudm61 node0103-fab:/etc/xinetd.d/db2hpudm61
```

Restart the service
```
# systemctl stop xinetd
# systemctl start xinetd
```

Change owner of `/opt/ibm/HPU/V6.1/cfg` directory on all hosts to the instance owner so that you do not have to depend upon root access
```
# runall "chown -R dbpemon.db2iadm1 /opt/ibm/HPU/V6.1/cfg"
```

Create, `db2hpu.cfg` file with the following parameters.

```
# HPU default configuration
bufsize=xxxxxx
db2dbdft=BLUDB
db2instance=db2inst1
doubledelim=binary
netservice=db2hpudm61
allow_unlimited_memory=yes
keepalive_time=10
maxthreads=8
maxselects=10
mig_pipe_timeout=60
min_extent_per_thread=4
use_stats=true
nbcpu=8
umask=022
```

Create softlink for db2hpu in `bin`
```
runall "ln -s /opt/ibm/HPU/V6.1/bin/db2hpu /bin/db2hpu"
```
Check the version
```
# runall "db2hpu -version"
```
Check the daemon running
```
# runall "netstat -a | grep -i hpu"
```
Login as you inside the container. Create `.db2hpu` in the home directory of user who is going to do the migration.
```
# su - db2psc
$ mkdir .db2hpu
$ cd .db2hpu
$ touch db2hpu.creds
$ db2hpu --credentials local
INZM059I Optim High Performance Unload for DB2 for Linux, UNIX and Windows 06.01.00.001(170531)
Management of credentials for 'local' type connections:
  - do you want to create or remove data (1/2)? 1
  - is it a new section (Y/N)? y
  - provide a section name: db2inst1
  - provide a user name: db2psc
Do you want to validate your data (Y/N)? y
INZM061I Credentials of connections created for 'db2inst1'
```
Do this on all hosts

Create a map file so that HPU on host can map source to the target. Login as root
```
# cd /opt/ibm/HPU/V6.1/cfg
# cat db2hpu.map
node0101-fab=srchost401
node0102-fab=srchost402
node0103-fab=srchost403
```
The first entry is the host name of the target (as it is in `db2nodes.cfg`) and the second entry is the host name of the source machine.

## DDL Migration

`migr.sh` calls `com.ibm.migr.data.GenInitialObjects` and it expects source and database connection informations. The JDBC drivers must on the class path.

## HPU Migration

The Db2 High Performance Unload is used as this is the fastest method to unload and load data.

The HPU has capabilities in which it will run on source database on each database partition. It will then transfer data using inetd to the target. The HPU must be running on each node on the target as xinetd daemon listening on a specific port.

When HPU sends data from source to multiple targets using inetd/xinetd protocol, it will invoke LOAD utility on target and copy the data from tcp/ip socket to the pipe so that LOAD can consume the data.

The HPU will repartition data on source and send the desired data to the target database partition so this method is the best when MLN topology is changing from source to the target.

`hpumigr.sh` calls `com.ibm.migr.data.GenHPUScripts` and it will detect the source MLN and reads target MLN count from the src.properties and generates migration scripts with proper syntax.
