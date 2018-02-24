/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.migr.utils.U;

public class Db2lookInfoData
{
	public String objSchema, schemaUnderMigration, schemaID;
	public String objName;
	public String objType;
	public String tabType;
	public String sqlOperation;
	public String sqlStmt;
	public String objAttribute;
	public boolean exists = false;
	public String fileName;
	public TableMapping table;
	
	public String getSQLStatement()
	{		
		return getSQLStatement(U.sqlTerminator);
	}

	public String getSQLStatement(String terminator)
	{		
		StringBuffer sql = new StringBuffer();
		String t = ((exists) ? "--" : "") + terminator + U.linesep;
		if (exists)
		{
			Pattern p = Pattern.compile("^.*$", Pattern.MULTILINE);
			Matcher m = p.matcher(sqlStmt);
			while (m.find())
			{
				String n = m.group(0);
				sql.append("-- " + n + U.linesep);
			}
			return sql.toString() + t;
		} else
		{
			return sqlStmt + t;
		}
	}
	
	public void setName(String tok, String boilerPlate)
	{
		fileName = schemaUnderMigration.trim() + "_" + tok + ".sql";
		U.truncateFile(U.getOutputDirName() + U.filesep + fileName);
		U.WriteFile(fileName, boilerPlate);
	}
	
	public String getName()
	{
		return fileName;
	}
}
