/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

public class SchemaData
{
	public String schema, owner, remarks;
	public int id;
	public boolean selected = false;
	
	public SchemaData()
	{
		;
	}
	
	public String getSchemaID()
	{
		return String.format("S%03d", id);
	}
	
	public SchemaData(String schema)
	{
		this.schema = schema;
	}
	
	public String getSQL()
	{
		String sql;
		sql = "CREATE SCHEMA \"" + schema + "\"";
		if (!owner.equalsIgnoreCase("SYSIBM"))
			sql += " AUTHORIZATION \"" + owner + "\"";
		return sql;		
	}
	
	public String getRemark()
	{
		return (remarks == null || remarks.length() == 0) ? null :
			"COMMENT ON SCHEMA \"" + schema + "\" IS '" + remarks.replaceAll("'", "\\\\'") + "'";
	}
}
