/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

import java.util.ArrayList;

/**
 * @author Vikram. Server Data bean to hold connection specific params in a bean
 *
 */
public class ServerData
{
	public String server;
	public int port;
	public String userid;
	public String password;
	public String dbname;
	public String currentSchema;
	public String clientUser, clientHostname, clientAccountingInformation, applicationName;
	public int fetchSize = 1000;
	public boolean autoCommit, isOpen = false;
	public int dataBufferSize = 10000;
	public String migrationSchemaList = null;
	public int dataBuffer;
	public int numCPU;
	
	public ArrayList<SchemaData> getMigrationSchemaList()
	{
		ArrayList<SchemaData> list = new ArrayList<SchemaData>();
		if (migrationSchemaList != null)
		{
			String[] str = migrationSchemaList.split(",");
			for (int i = 0; i < str.length; ++i)
			{
				list.add(new SchemaData(str[i]));
			}
		}
		return list;
	}
	
	public String getMigrationSchemaInList()
	{
		if (migrationSchemaList == null || migrationSchemaList.length() == 0)
			return "('')";
		else
		{
			StringBuilder b = new StringBuilder();
			String[] list = migrationSchemaList.trim().split(",");
			for (int i = 0; i < list.length; ++i) 
			{
				if (list[i].startsWith("'") && list[i].endsWith("'"))
				{
					list[i] = list[i].toUpperCase();
				} else if (list[i].startsWith("\"") && list[i].endsWith("\""))
				{
					;
				} else
				{
					list[i] = "'" + list[i].toUpperCase() + "'";
				}
			}
			b.append("(");
			for (int i = 0; i < list.length; ++i) 
			{
				if (i > 0)
				   b.append(",");
				b.append(list[i]);
			}
			b.append(")");
			return b.toString();
		}
	}
}
