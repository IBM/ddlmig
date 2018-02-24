/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

public class TSData
{
	private float data = 0L, index = 0L;
	
	public TSData(long data, long index)
	{
		this.data = data;
		this.index = index;
	}
	
	public String toString()
	{
		return "data size = " + data + " index size = " + index;
	}
	
	public float getData()
	{
		return data;
	}
	
	public float getIndex()
	{
		return index;
	}
}
