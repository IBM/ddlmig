/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

public class BufferpoolData
{
	public String bufferpoolName;
	public int pageSize;
	public boolean exists = false;
	
	public BufferpoolData()
	{
		
	}
	
	public BufferpoolData(String bufferpoolName, String pageSize)
	{
		this.bufferpoolName = bufferpoolName;
		this.pageSize = Integer.valueOf(pageSize);
	}

	public String getSQL()
	{
		String genStr = "CREATE BUFFERPOOL "+ bufferpoolName+" PAGESIZE " + pageSize + ";";
		return (exists) ? "--"+genStr : genStr;
	}
}
