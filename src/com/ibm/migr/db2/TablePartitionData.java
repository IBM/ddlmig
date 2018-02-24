/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

public class TablePartitionData
{
	public long rowsInserted = 0, rowsUpdated = 0, rowsDeleted = 0;
	public int dataPartitionID;
	public String dataPartition;
	public long size;
}
