package com.ibm.migr.db2;

public class DBGroupData
{
	public String dstDBPartitionGroupName, srcDBPartitionGroupName, dstPartitions, srcPartitions;
	public boolean exists = false;
	
	public DBGroupData()
	{
	}
	
	public DBGroupData(String srcDBPartitionGroupName, String dstDBPartitionGroupName, String dstPartitions)
	{
		this.dstDBPartitionGroupName = dstDBPartitionGroupName;
		this.srcDBPartitionGroupName = srcDBPartitionGroupName;
		this.dstPartitions = dstPartitions;
	}

	public String getdstSQL()
	{
		String sql = "CREATE DATABASE PARTITION GROUP " + dstDBPartitionGroupName + " ON DBPARTITIONNUMS ("+dstPartitions+");";
		return (exists) ? "--"+sql : sql;
	}
	
	public String getsrcSQL()
	{
		String sql = "CREATE DATABASE PARTITION GROUP " + srcDBPartitionGroupName + " ON DBPARTITIONNUMS ("+srcPartitions+");";
		return (exists) ? "--"+sql : sql;
	}
}
