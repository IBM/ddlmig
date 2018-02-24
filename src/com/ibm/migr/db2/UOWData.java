/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

import com.ibm.migr.utils.U;

/**
 * @author Vikram
 *
 */
public class UOWData
{
	private String regex = U.sqlTerminator+"(?=([^\"']*[\"'][^\"']*[\"'])*[^\"']*$)";
	private String uow;
	private SQLData[] sqlData;
	private String[] sqlType;
	private int size;
	
	public UOWData(String uow)
	{
		this.uow = uow;
		String[] sqls = uow.split(regex);
		size = sqls.length;		
		sqlData = new SQLData[size];
		sqlType = new String[size];
		for (int i = 0; i < size; ++i)
		{
			sqlData[i] = new SQLData(sqls[i]);
		}
	}
	
	public SQLData getSQLData(int index)
	{
		return sqlData[index];
	}
	
	public void setSQLType(int index, String type)
	{
		sqlType[index] = type;
	}

	public String getSQLType(int index)
	{
		return sqlType[index];
	}
	
	public String getUOW()
	{
		return uow;
	}
	
	public String getSQL(int index)
	{
		return sqlData[index].getSQL();
	}
	
	public int size()
	{
		return size;
	}
}
