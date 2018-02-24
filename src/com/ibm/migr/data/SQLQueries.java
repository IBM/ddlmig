/**
 * Author Vikram Khatri
 */
package com.ibm.migr.data;

import static com.ibm.migr.utils.Log.LEVEL_DEBUG;
import static com.ibm.migr.utils.Log.debug;
import static com.ibm.migr.utils.Log.error;
import static com.ibm.migr.utils.Log.info;
import static com.ibm.migr.utils.Log.set;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.migr.db2.BufferpoolData;
import com.ibm.migr.db2.DBGroupData;
import com.ibm.migr.db2.Db2lookInfoData;
import com.ibm.migr.db2.IndexData;
import com.ibm.migr.db2.SchemaData;
import com.ibm.migr.db2.SequenceData;
import com.ibm.migr.db2.TSData;
import com.ibm.migr.db2.TableMapping;
import com.ibm.migr.db2.TablePartitionData;
import com.ibm.migr.db2.db2;
import com.ibm.migr.utils.RunCommand;
import com.ibm.migr.utils.U;

public class SQLQueries
{
	private ArrayList<ArrayList<String>> listFileNames = new ArrayList<ArrayList<String>>();
	private Hashtable<String, TableMapping> hashTables = new Hashtable<String, TableMapping>();
	private ArrayList<TableMapping> listTables = null;
	private ArrayList<DBGroupData> listDBGroupData = new ArrayList<DBGroupData>();
	private ArrayList<BufferpoolData> listBufferpools = new ArrayList<BufferpoolData>();
	private ArrayList<Integer> tokenList = new ArrayList<Integer>();
	private db2 conn;
	private HashMap<String, String> authColumnsMap;

	public SQLQueries(db2 conn)
	{
		this.conn = conn;
	}

	public ArrayList<SchemaData> getSchemas()
	{
		return conn.getSelectedSchemaList();
	}

	/*
	 * public ArrayList<SchemaData> getAllSchema() { return
	 * conn.getSelectedSchemaList(); }
	 */

	public String getInstanceName()
	{
		return conn.getInstanceName();
	}

	public String getServerName()
	{
		return conn.getServerName();
	}

	public ArrayList<SequenceData> getSequences()
	{
		ArrayList<SequenceData> list = new ArrayList<SequenceData>();
		ResultSet rs;
		String sql = "select SEQSCHEMA, SEQNAME, NEXTCACHEFIRSTVALUE "
				+ "from syscat.sequences s " + "where origin = 'U'";
		try
		{
			rs = conn.PrepareExecuteQuery(sql);
			while (rs.next())
			{
				SequenceData data = new SequenceData();
				data.schema = rs.getString(1);
				data.name = rs.getString(2);
				data.restartValue = rs.getLong(3);
				list.add(data);
			}
			rs.close();
			conn.queryStatement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return list;
	}

	public String genStorageGroups()
	{
		StringBuffer list = new StringBuffer();
		String sql;
		String[] stogroups;
		if (U.stogroupdata != null && U.stogroupdata.length() > 0)
		{
			stogroups = U.stogroupdata.split("\\|");
			sql = "CREATE STOGROUP " + stogroups[0] + " ON " + stogroups[1]
					+ ";";
			debug(sql);
			list.append(sql + U.linesep);
		}
		if (U.stogroupdata != null && U.stogroupdata.length() > 0)
		{
			stogroups = U.stogroupidx.split("\\|");
			sql = "CREATE STOGROUP " + stogroups[0] + " ON " + stogroups[1]
					+ ";";
			debug(sql);
			list.append(sql + U.linesep);
		}
		if (U.stogroupdata != null && U.stogroupdata.length() > 0)
		{
			stogroups = U.stogrouptemp.split("\\|");
			sql = "CREATE STOGROUP " + stogroups[0] + " ON " + stogroups[1]
					+ ";";
			debug(sql);
			list.append(sql + U.linesep);
		}
		debug("=========================================================");
		return list.toString();
	}

	public String genSchema()
	{
		StringBuffer buffer = new StringBuffer();
		for (SchemaData data : getSchemas())
		{
			if (data.selected)
			{
				buffer.append("CREATE SCHEMA \"" + data.schema + "\""
						+ U.sqlTerminator + U.linesep);
			}
		}
		return buffer.toString();
	}

	private String composeType(String typeSchema, String typeName, int length,
			int scale, int codePage, String pSchemaName)
	{
		String typeStr = "";
		if (!typeSchema.equalsIgnoreCase("SYSIBM")
				&& !typeSchema.equalsIgnoreCase(pSchemaName))
			typeStr += "\"" + typeSchema + "\".";
		if (typeSchema.equalsIgnoreCase("SYSIBM"))
			typeStr += typeName;
		else
			typeStr += "\"" + typeName + "\"";
		if (typeSchema.equalsIgnoreCase("SYSIBM")
				&& (typeName.equalsIgnoreCase("CHARACTER")
						|| typeName.equalsIgnoreCase("VARCHAR")
						|| typeName.equalsIgnoreCase("BLOB")
						|| typeName.equalsIgnoreCase("CLOB")
						|| typeName.equalsIgnoreCase("DECIMAL")))
		{
			if (length == -1)
				typeStr += "()";
			else
			{
				if (typeName.equalsIgnoreCase("DECIMAL"))
					typeStr += "(" + length + "," + scale + ")";
				else
					typeStr += "(" + length + ")";
			}
		}
		if ((typeName.equalsIgnoreCase("CHARACTER")
				|| typeName.equalsIgnoreCase("VARCHAR")) && codePage == 0)
			typeStr += " FOR BIT DATA";
		return typeStr;
	}

	private void BuildAuthColumnList()
	{
		authColumnsMap = new HashMap<String, String>();
		ResultSet rs;
		String key, value, schema, specificname, typeschema, typename, sql = "";

		if (authColumnsMap != null)
			return;

		sql = "SELECT routineschema, specificname, typeschema, typename FROM SYSCAT.ROUTINEPARMS "
				+ "WHERE routineschema IN (" + conn.getSelectedSchemaInListID()
				+ ") AND rowtype IN ('B', 'O', 'P') "
				+ "ORDER BY routineschema, specificname, ordinal ASC";

		try
		{
			rs = conn.PrepareExecuteQuery(sql);
			int i = 0;
			while (rs.next())
			{
				schema = U.trim(rs.getString(1));
				specificname = U.trim(rs.getString(2));
				typeschema = U.trim(rs.getString(3));
				typename = U.trim(rs.getString(4));
				key = schema + "." + specificname;
				value = typeschema + ":" + typename;
				authColumnsMap.put(key, value);
				++i;
			}
			rs.close();
			conn.queryStatement.close();
			info(i + " values cached in authColumnsMap");
		} catch (Exception e)
		{
			error("Error in BuildAuthColumnList", e);
		}
	}

	private String authColumnList(String schema, String colName)
	{
		int objCount = 0;
		String key = schema + "." + colName, columnList = "", typeSchema,
				typeName;

		if (authColumnsMap.containsKey(key))
		{
			String[] tmpVar = ((String) authColumnsMap.get(key)).split(":");
			typeSchema = tmpVar[0];
			typeName = tmpVar[1];
			if (objCount == 0)
			{
				columnList = composeType(typeSchema, typeName, -1, 0, -1,
						schema);
			} else
			{
				columnList = columnList + ","
						+ composeType(typeSchema, typeName, -1, 0, -1, schema);
			}
			objCount++;
		}
		return columnList;
	}

	public String genAuths()
	{
		String sql = "";
		ResultSet rs;
		String granteeType, grantee, schema, name, colName, auth, type;
		StringBuffer authsbuffer = new StringBuffer();
		int objCount = 0;
		
		if (authColumnsMap == null)
		{
			BuildAuthColumnList();
		}

		try
		{
			sql = "select rolename from syscat.roles where rolename not like 'SYS%'";
			rs = conn.PrepareExecuteQuery(sql);
			while (rs.next())
			{
				String role = U.trim(rs.getString(1));
				if (role != null && role.length() > 0)
				{
					String[] roles = role.split("~");
					for (int i = 0; i < roles.length; ++i)
					{
						authsbuffer.append("CREATE ROLE " + roles[i] + U.sqlTerminator + U.linesep);
					}
				}
			}
			rs.close();
			conn.queryStatement.close();
		} catch (SQLException e)
		{
			error("Role auth error", e);
		}

		for (SchemaData data : getSchemas())
		{
			if (data.selected)
			{
				try
				{
					sql = "WITH AUTH AS (SELECT granteetype, grantee, tabschema as schema, tabname as name, "
							+ "'' as colname, auth, 'TABLE' as type "
							+ "FROM SYSCAT.TABAUTH, " + " LATERAL(VALUES "
							+ "    (case when controlauth = 'Y' then 'CONTROL' else NULL end), "
							+ "    (case when alterauth   = 'Y' then 'ALTER'      when alterauth  = 'N' then NULL else 'ALTER      GRANT' end), "
							+ "    (case when deleteauth  = 'Y' then 'DELETE'     when deleteauth = 'N' then NULL else 'DELETE     GRANT' end), "
							+ "    (case when indexauth   = 'Y' then 'INDEX'      when indexauth  = 'N' then NULL else 'INDEX      GRANT' end), "
							+ "    (case when insertauth  = 'Y' then 'INSERT'     when insertauth = 'N' then NULL else 'INSERT     GRANT' end), "
							+ "    (case when selectauth  = 'Y' then 'SELECT'     when selectauth = 'N' then NULL else 'SELECT     GRANT' end), "
							+ "    (case when refauth     = 'Y' then 'REFERENCES' when refauth    = 'N' then NULL else 'REFERENCES GRANT' end), "
							+ "    (case when updateauth  = 'Y' then 'UPDATE'     when updateauth = 'N' then NULL else 'UPDATE     GRANT' end) "
							+ " ) AS A(auth) WHERE tabschema = '"+ data.schema + "' "
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, tabschema as schema, tabname as name, colname, "
							+ "CASE privtype WHEN 'U' THEN 'UPDATE' WHEN 'R' THEN 'REFERENCES' END || ' GRANT' AS auth, "
							+ "'COLUMN' AS type "
							+ "FROM SYSCAT.COLAUTH WHERE tabschema = '"+ data.schema + "' "
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, indschema as schema, indname as name, "
							+ "       CAST(NULL AS VARCHAR(128)) AS colname, "
							+ "       (case when controlauth = 'Y' then 'CONTROL' else NULL end) AS auth, "
							+ "       'INDEX' as type FROM SYSCAT.INDEXAUTH WHERE indschema = '"+ data.schema + "' " 
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, R.routineschema schema, R.routinename as name,"
							+ "       R.specificname as colname, "
							+ "       (case when executeauth = 'Y' then 'EXECUTE' when executeauth = 'N' then NULL else 'EXECUTE    GRANT' end) AS auth, "
							+ "       CASE R.routinetype WHEN 'F' THEN 'FUNCTION' "
							+ "                          WHEN 'P' THEN 'PROCEDURE' "
							+ "                          END as type "
							+ "FROM SYSCAT.ROUTINEAUTH A, SYSCAT.ROUTINES R " 
							+ "WHERE A.schema = '"+ data.schema + "' "
							+ "AND A.schema = R.routineschema "
							+ "AND A.specificname = R.specificname "
							+ "AND A.routinetype IN ('F', 'P') " 
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, '' as schema, "
							+ "       '" + data.schema + "' as name, "
							+ "       CAST(NULL AS VARCHAR(128)) AS colname, "
							+ "       auth, 'SCHEMA' as type "
							+ "FROM SYSCAT.SCHEMAAUTH, LATERAL(VALUES "
							+ "          (case when alterinauth = 'Y' then 'ALTERIN' when alterinauth = 'N' then NULL else 'ALTERIN    GRANT' end), "
							+ "          (case when createinauth = 'Y' then 'CREATEIN' when createinauth = 'N' then NULL else 'CREATEIN   GRANT' end), "
							+ "          (case when dropinauth = 'Y' then 'DROPIN' when dropinauth = 'N' then NULL else 'DROPIN     GRANT' end) "
							+ "       ) AS A(auth) "
							+ "WHERE schemaname = '" + data.schema + "' "
							+ "UNION ALL "
							+ "SELECT granteetype, grantee, seqschema as schema, seqname as name, "
							+ "       CAST(NULL AS VARCHAR(128)) AS colname, auth, 'SEQUENCE' as type "
							+ "FROM SYSCAT.SEQUENCEAUTH, " 
							+ "  LATERAL(VALUES "
							+ "            (case when usageauth = 'Y' then 'USAGE' when usageauth = 'N' then NULL else 'USAGE      GRANT' end), "
							+ "            (case when alterauth = 'Y' then 'ALTER' when alterauth = 'N' then NULL else 'ALTER      GRANT' end) "
							+ "          ) " + "   AS A(auth) "
							+ "WHERE seqschema = '" + data.schema + "' "
							+ ((U.majorSourceDBVersion >= 9
									&& U.minorSourceDBVersion >= 7)
											? "UNION ALL "
													+ "SELECT granteetype, grantee, varschema as schema, varname as name, CAST(NULL AS VARCHAR(128)) AS colname, auth, 'VARIABLE' as type "
													+ "FROM SYSCAT.VARIABLEAUTH, "
													+ "   LATERAL(VALUES "
													+ "      (case when readauth = 'Y' then 'READ' when readauth = 'N' then NULL else 'READ      GRANT' end), "
													+ "      (case when writeauth = 'Y' then 'WRITE' when writeauth = 'N' then NULL else 'WRITE     GRANT' end)) "
													+ "   AS A(auth) "
													+ "WHERE varschema = '"
													+ data.schema + "' "
											: " ")
							+ ((U.majorSourceDBVersion >= 9
									&& U.minorSourceDBVersion >= 5)
											? "UNION ALL "
													+ "SELECT granteetype, grantee, MODULESCHEMA as schema, MODULENAME as name, CAST(NULL AS VARCHAR(128)) AS colname, "
													+ "       (case when EXECUTEAUTH = 'Y' then 'MODULE' when EXECUTEAUTH = 'N' then NULL else 'EXECUTE    GRANT' end) auth, 'EXECUTE' as type "
													+ "FROM SYSCAT.MODULEAUTH "
													+ "WHERE MODULESCHEMA = '"+ data.schema + "' "
													+ "UNION ALL "
													+ "SELECT granteetype, grantee, '' as schema, rolename as name, CAST(NULL AS VARCHAR(128)) AS colname, "
													+ "       (case when admin = 'Y' then 'ROLE       GRANT' else 'ROLE' end) auth, 'ROLE' as type "
													+ "FROM SYSCAT.ROLEAUTH "
													+ "WHERE grantor = '"+ data.schema + "' "
													+ "and rolename NOT LIKE 'SYSROLE%' "
											: " ")
							+ ") SELECT * FROM AUTH WHERE AUTH IS NOT NULL "
							+ "ORDER BY CASE type WHEN 'SCHEMA' THEN 1 "
							+ "WHEN 'TABLE' THEN 2 " + "WHEN 'COLUMN' THEN 3 "
							+ "ELSE 4 END";

					rs = conn.PrepareExecuteQuery(sql);
					while (rs.next())
					{
						granteeType = U.trim(rs.getString(1));
						grantee = U.trim(rs.getString(2));
						schema = U.trim(rs.getString(3));
						name = U.trim(rs.getString(4));
						colName = U.trim(rs.getString(5));
						auth = U.trim(rs.getString(6));
						type = U.trim(rs.getString(7));
						if (auth.length() >= 10)
							authsbuffer.append("GRANT " + auth.substring(0,10));
			        	else
			        		authsbuffer.append("GRANT " + auth);
						if (type.equals("COLUMN"))
							authsbuffer.append(" (\"" + colName + "\")");
						if (!type.equals("ROLE"))
							authsbuffer.append(" ON ");
						if (type.equals("COLUMN") || type.equals("TABLE") || type.equals("ROLE"))
						{
							if (schema != null && schema.length() > 0)
							{
								authsbuffer.append(" \"" + schema + "\".");
							}
							authsbuffer.append("\"" + name + "\" ");
						} else
						{
							if (type.equals("FUNCTION") || type.equals("PROCEDURE"))
							{
								if (colName != null && colName.length() > 0)
								{
									authsbuffer.append("SPECIFIC ");
								}
							}
							authsbuffer.append(type + " ");
							if (schema != null && schema.length() > 0)
							{
								authsbuffer.append(" \"" + schema + "\".");
							}
							if (type.equals("FUNCTION") || type.equals("PROCEDURE"))
							{
							    authsbuffer.append("\"" + colName + "\" ");
							} else
							{
								authsbuffer.append("\"" + name + "\" ");
							}
						} 
						if ((type.equals("FUNCTION") || type.equals("PROCEDURE")))
						{
							if (colName != null && colName.length() > 0)
							{
								;
							} else
							{
								authsbuffer.append("(" + authColumnList(data.schema, colName)+ ")");
							}			
						}
						authsbuffer.append(" TO ");
						if (grantee.equals("PUBLIC"))
							authsbuffer.append(grantee);
						else
						{
							if (granteeType.equals("U"))
								authsbuffer.append("USER \"" + grantee + "\"");
							else if (granteeType.equals("R"))
								authsbuffer.append("ROLE \"" + grantee + "\"");
							else
								authsbuffer.append("GROUP \"" + grantee + "\"");
						}
						if (auth.endsWith("GRANT"))
							authsbuffer.append(" WITH GRANT OPTION");
						authsbuffer.append(U.sqlTerminator + U.linesep);
						if (objCount > 0 && objCount % 500 == 0)
							info(objCount + " # grants extracted for schema " + data.schema);
						objCount++;
					}
					if (objCount > 0)
						info(objCount + " Total # Grants extracted for schema " + data.schema);
					rs.close();
					conn.queryStatement.close();
				} catch (Exception e)
				{
					error("Error in genAuths for " + sql, e);
				}
			}
		}
		return authsbuffer.toString();
	}

	public String genGlobalVariables()
	{
		StringBuffer sb = new StringBuffer();
		ResultSet rs;
		String sql = "", readOnly, remarks, defaultValue;
		String varSchema, varName, typeSchema, typeName;
		int length, scale, codePage;
		String varType = "";
		int objCount = 0;

		try
		{
			sql = "SELECT VARSCHEMA, VARNAME,TYPESCHEMA,TYPENAME,LENGTH, "
					+ "SCALE,CODEPAGE,READONLY,REMARKS,DEFAULT "
					+ "FROM SYSCAT.VARIABLES WHERE VARMODULENAME IS NULL "
					+ "AND OWNERTYPE = 'U'";
			rs = conn.PrepareExecuteQuery(sql);
			while (rs.next())
			{
				varSchema = U.trim(rs.getString(1));
				varName = U.trim(rs.getString(2));
				typeSchema = U.trim(rs.getString(3));
				typeName = U.trim(rs.getString(4));
				length = rs.getInt(5);
				scale = rs.getInt(6);
				codePage = rs.getInt(7);
				readOnly = U.trim(rs.getString(8));
				remarks = U.trim(rs.getString(9));
				defaultValue = U.trim(rs.getString(10));
				if (defaultValue == null)
					defaultValue = "";
				varType = composeType(typeSchema, typeName, length,
						scale, codePage, varSchema);
				if (readOnly.equals("C") && defaultValue.length() > 0)
					sb.append("CREATE OR REPLACE VARIABLE "
							+ U.putQuote(varSchema) + "."
							+ U.putQuote(varName) + " " + varType
							+ " CONSTANT " + defaultValue);
				else if (readOnly.equals("N")
						&& defaultValue.length() > 0)
					sb.append("CREATE OR REPLACE VARIABLE "
							+ U.putQuote(varSchema) + "."
							+ U.putQuote(varName) + " " + varType
							+ " DEFAULT " + defaultValue);
				else
					sb.append("CREATE OR REPLACE VARIABLE "
							+ U.putQuote(varSchema) + "."
							+ U.putQuote(varName) + " " + varType);
				sb.append(U.sqlTerminator + U.linesep);
				if (objCount > 0 && objCount % 100 == 0)
					info(objCount	+ " # Global Variables extracted for schema " + U.schema);
				objCount++;
			}
			rs.close();
			conn.queryStatement.close();
		} catch (SQLException e)
		{
			error("Error in genGlobalVariables", e);
		}
		return sb.toString();
	}

	public String genTempTableSpace()
	{
		String sql = "";
		if (U.stogrouptemp != null && U.stogrouptemp.length() > 0
				&& U.tempBufferpool != null && U.tempBufferpool.length() > 0)
		{
			String[] stogroup = U.stogrouptemp.split("\\|");
			String[] bufferpool = U.tempBufferpool.split(",");
			sql = "CREATE TEMPORARY TABLESPACE TEMPSPACE2 IN DATABASE PARTITION GROUP IBMTEMPGROUP "
					+ "PAGESIZE " + bufferpool[1]
					+ " MANAGED BY AUTOMATIC STORAGE " + "USING STOGROUP "
					+ stogroup[0] + " " + "EXTENTSIZE 32 "
					+ "PREFETCHSIZE AUTOMATIC " + "BUFFERPOOL " + bufferpool[0]
					+ " " + "OVERHEAD INHERIT " + "TRANSFERRATE INHERIT "
					+ "FILE SYSTEM CACHING " + "DROPPED TABLE RECOVERY OFF;";
		}
		debug(sql);
		debug("=========================================================");
		return sql + U.linesep;
	}

	public ArrayList<BufferpoolData> getBufferpools()
	{
		listBufferpools.clear();
		// if U.bufferpool is defined then override the source
		if (U.dataBufferpool != null && U.dataBufferpool.length() > 0)
		{
			String[] bufferpool = U.dataBufferpool.split(",");
			listBufferpools
					.add(new BufferpoolData(bufferpool[0], bufferpool[1]));
			bufferpool = U.idxBufferpool.split(",");
			listBufferpools
					.add(new BufferpoolData(bufferpool[0], bufferpool[1]));
			bufferpool = U.tempBufferpool.split(",");
			listBufferpools
					.add(new BufferpoolData(bufferpool[0], bufferpool[1]));
		} else
		{
			ResultSet rs;
			String sql = "select bpname, pagesize from syscat.bufferpools";
			try
			{
				rs = conn.PrepareExecuteQuery(sql);
				while (rs.next())
				{
					BufferpoolData data = new BufferpoolData();
					data.bufferpoolName = rs.getString(1);
					data.pageSize = rs.getInt(2);
					listBufferpools.add(data);
				}
				rs.close();
				conn.queryStatement.close();
			} catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
		return listBufferpools;
	}

	public ArrayList<DBGroupData> validateDBPartGroups(
			ArrayList<DBGroupData> source)
	{
		for (DBGroupData data : source)
		{
			data.exists = conn.checkDBPartGroups(data.dstDBPartitionGroupName);
		}
		return source;
	}

	public ArrayList<DBGroupData> getDBPartGroups()
	{		
		String dbpgName;
		// If mapdbpg is defined, build list first
		if (U.mapdbpg != null && U.mapdbpg.length() > 0)
		{
			String[] mapdbpg = U.mapdbpg.split("\\|");
			for (int i = 0; i < mapdbpg.length; ++i)
			{
				String[] dbpg = mapdbpg[i].split(":");
				listDBGroupData.add(new DBGroupData(dbpg[0], dbpg[1], dbpg[2]));
			}
		} 
		
		ResultSet rs;
		String sql = "select DBPGNAME, LISTAGG(DBPARTITIONNUM, ',') "
				+ "WITHIN GROUP (ORDER BY DBPARTITIONNUM) AS PARTITIONS "
				+ "from SYSCAT.DBPARTITIONGROUPDEF where in_use = 'Y' "
				+ "group by DBPGNAME";
		try
		{
			rs = conn.PrepareExecuteQuery(sql);
			while (rs.next())
			{
				dbpgName = rs.getString(1);
				if (U.mapdbpg == null || U.mapdbpg.length() == 0)
				{
					DBGroupData data = new DBGroupData();
					data.srcDBPartitionGroupName = data.dstDBPartitionGroupName = dbpgName;
					data.srcPartitions = data.dstPartitions = rs.getString(2);
					listDBGroupData.add(data);
				} else
				{
					int i = 0;
					for (DBGroupData data : listDBGroupData)
					{
						if (data.srcDBPartitionGroupName.equals(dbpgName))
						{
							data.srcPartitions = rs.getString(2);
							listDBGroupData.set(i, data);
						}
						++i;
					}					
				}
			}
			rs.close();
			conn.queryStatement.close();
		} catch (SQLException e)
		{
			error("getDBPartGroups", e);
		}
		return listDBGroupData;
	}

	private boolean checkObjectExistence(String type, String schema,
			String name, String objAttribute)
	{
		// first need to call openObject and do the work here and call
		// closeObject
		return conn.checkObject(type, schema, name, objAttribute);
	}

	public ArrayList<Db2lookInfoData> validatedb2look(
			ArrayList<Db2lookInfoData> list)
	{
		for (Db2lookInfoData data : list)
		{
			data.exists = checkObjectExistence(data.objType, data.objSchema,
					data.objName, data.objAttribute);
		}
		return list;
	}

	public ArrayList<Db2lookInfoData> getdb2lookInfo(int token,
			SchemaData schemaData)
	{
		ArrayList<Db2lookInfoData> list = new ArrayList<Db2lookInfoData>();
		ResultSet rs;
		String sql = "select l.obj_schema, l.obj_name, l.obj_type, l.obj_attribute, "
				+ "(select NVL(t.type,'') " + " from syscat.tables t "
				+ " where t.tabschema = l.obj_schema "
				+ " and t.tabname = l.obj_name), "
				+ "l.sql_operation, l.sql_stmt "
				+ "from systools.db2look_info l " + "where op_token = " + token;

		try
		{
			rs = conn.PrepareExecuteQuery(sql);
			while (rs.next())
			{
				Db2lookInfoData data = new Db2lookInfoData();
				data.schemaID = schemaData.getSchemaID();
				data.objSchema = rs.getString(1).trim();
				data.objName = rs.getString(2);
				data.objType = rs.getString(3);
				data.objAttribute = rs.getString(4);
				data.tabType = rs.getString(5);
				data.sqlOperation = rs.getString(6);
				Clob clob = rs.getClob(7);
				data.sqlStmt = clob.getSubString(1, (int) clob.length());
				if (data.objSchema != null && data.objSchema.length() == 0)
					data.objSchema = data.objName;
				data.schemaUnderMigration = schemaData.schema;
				for (TableMapping tdata : listTables)
				{
					if (data.objType.equals("TABLE")
							&& tdata.tabschema.equals(data.objSchema)
							&& tdata.tabname.equals(data.objName))
					{
						data.table = tdata;
						break;
					} else if (data.objType.equals("INDEX")
							&& tdata.existsIndex(data.objName))
					{
						data.table = tdata;
						break;
					}
				}
				list.add(data);
			}
			rs.close();
			conn.queryStatement.close();
		} catch (SQLException e)
		{
			error("Error in getdb2lookInfo", e);
		}
		return list;
	}

	private TSData initialSize(Matcher m)
	{
		long sizeData = 0L, sizeIndex = 0L;
		while (m.find())
		{
			if (m.group(1).equals("_D"))
			{
				sizeData += Long.valueOf(m.group(2).trim());
			}
			if (m.group(1).equals("_X"))
			{
				sizeIndex += Long.valueOf(m.group(2).trim());
			}
		}
		return new TSData(sizeData, sizeIndex);
	}

	public String printAllTableInfo()
	{
		StringBuffer buf = new StringBuffer();
		for (TableMapping data : listTables)
		{
			debug("Table Data for = " + data.tabname);
			buf.append(data.toString());
		}
		return buf.toString();
	}

	public String getAllGeneratedTablespacesDefn()
	{
		TSData ts;
		String regex = "(?mi)(_X\\b|_D\\b).*?\\bINITIALSIZE\\b\\s+(\\d+).*";
		Pattern p = Pattern.compile(regex);
		StringBuffer buf = new StringBuffer();
		for (TableMapping data : listTables)
		{
			for (String str : data.getPartitionandNonPartitionTableSpacesDefinition())
			{
				buf.append(str + ";" + U.linesep);
			}
			for (String str : data.getNonPartitionedIndexTableSpacesDefinition())
			{
				buf.append(str + ";" + U.linesep);
			}
		}
		ts = initialSize(p.matcher(buf.toString()));
		buf.append("--========================================================="
				+ U.linesep);
		buf.append(String.format("-- Total Data  Table space Initial size = %14.2f KB\n", ts.getData()));
		buf.append(String.format("-- Total Index Table space Initial size = %14.2f KB\n", ts.getIndex()));
		buf.append(String.format("-- Grand Total Table space Initial size = %14.2f KB\n", (ts.getData() + ts.getIndex())));
		buf.append(String.format("-- Grand Total Table space Initial size = %14.2f MB\n", (ts.getData() + ts.getIndex()) / (1024.0)));
		buf.append(String.format("-- Grand Total Table space Initial size = %14.2f GB\n", (ts.getData() + ts.getIndex()) / (1024.0 * 1024.0)));
		buf.append(String.format("-- Grand Total Table space Initial size = %14.2f TB\n", (ts.getData() + ts.getIndex()) / (1024.0 * 1024.0 * 1024.0)));
		buf.append("--========================================================="
				+ U.linesep);
		buf.append(
				String.format("-- Total Number of Table spaces         = %6d\n",
						getTotalTablespacesCount()));
		buf.append("--========================================================="
				+ U.linesep);
		return buf.toString();
	}

	private int countMLN()
	{
		String sql5 = "select count(*) from SYSCAT.DBPARTITIONGROUPDEF where dbpgname = 'IBMDEFAULTGROUP'";
		String mlnCount = conn.ExecuteQueryFirstColumn(sql5);
		return Integer.valueOf(mlnCount);
	}
	
	private int getTableInfo(String schema, int tableID, int mlnCount, String schemaID)
	{
		long mainStart = System.currentTimeMillis();
		Clob c1;
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		
		String key, sql = "select t.tabschema, t.tabname, "
				+ "t.lastused, t.card, t.compression, t.tableorg, t.rowcompmode, t.pctpagessaved, "
				+ "t.partition_mode, t.pmap_id, t.append_mode, t.volatile, "
				+ "t.pctfree, t.type, t.statistics_profile "
				+ "from syscat.tables t "
				+ "where t.type in ('T','G','S','U','W','H','L') "
				+ "and t.tabschema = ? "
				//+ "and t.tabname = 'EDW_ATUL_CONLD_CLM_SUM_HSTRY' "
				+ "order by t.lastused asc "
				+ "with ur";
		
		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				TableMapping map = new TableMapping();
				map.tabschema = rs.getString(1).trim();
				map.tabname = rs.getString(2).trim();
				map.lastused = rs.getDate(3);
				map.card = rs.getLong(4);
				map.compression = rs.getString(5);
				map.tableorg = rs.getString(6);
				map.rowcompmode = rs.getString(7);
				map.pctpagessaved = rs.getString(8);
				map.partitionMode = rs.getString(9);
				map.pmapID = rs.getString(10);
				map.appendMode = rs.getString(11);
				map.isVolatile = rs.getBoolean(12);
				map.pctfree = rs.getShort(13);
				map.type = rs.getString(14);
				map.tableID = tableID;
				map.sourceMLNCount = mlnCount;
				map.schemaID = schemaID;
				c1 = rs.getClob(15);
				if (c1 == null)
					map.statisticalProfile = "";
				else
					map.statisticalProfile = c1.getSubString(1, (int) c1.length());
				map.dataPartSizeList = new ArrayList<TablePartitionData>();
				map.indexPartSizeList = new ArrayList<IndexData>();
				key = map.tabschema + "," + map.tabname;
				hashTables.put(key, map);
				++tableID;
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(String.format(procName + " Count = %04d Elapsed Time = %s", tableID, U.getElapsedTime(mainStart)));
		return tableID;
	}
	
	private void getPartInfo(String schema)
	{
		long mainStart = System.currentTimeMillis();
		int count = 0;
		String procName = new Throwable().getStackTrace()[0].getMethodName();

		String key, sql = "select d.tabschema, d.tabname, t.dbpgname "
				+ "from syscat.datapartitions d, syscat.tablespaces t "
				+ "where t.tbspaceid = d.tbspaceid "
				+ "and d.tabschema = ? "
				//+ "and d.tabname = 'EDW_ATUL_CONLD_CLM_SUM_HSTRY' "
				+ "group by d.tabschema, d.tabname, t.dbpgname";
		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				key = rs.getString(1).trim() + "," + rs.getString(2).trim();
				TableMapping map = hashTables.get(key);
				if (map != null)
				{
					DBGroupData data = new DBGroupData();
					data.srcDBPartitionGroupName = rs.getString(3);
					if (listDBGroupData.size() == 0)
						getDBPartGroups();
					for (DBGroupData dbgdata : listDBGroupData)
					{
						if (data.srcDBPartitionGroupName.equals(dbgdata.srcDBPartitionGroupName))
						{
							data.dstPartitions = dbgdata.dstPartitions;
							data.srcPartitions = dbgdata.srcPartitions;
							data.dstDBPartitionGroupName = dbgdata.dstDBPartitionGroupName;
						}
					}
					map.dbpgData = data;
					hashTables.put(key, map);
				}
				++count;
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(String.format(procName + " Count = %04d Elapsed Time = %s", count, U.getElapsedTime(mainStart)));
	}
	
	private Hashtable<String, String> getDataPartitionInfo(String schema)
	{
		Hashtable<String, String> ht = new Hashtable<String, String>();
		long mainStart = System.currentTimeMillis();
		int count = 0;
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		
		String key, value;
		String sql = "Select d.tabschema, d.tabname, d.datapartitionid, d.datapartitionname "
				+ "From syscat.datapartitions d "
				+ "where d.tabschema = ? "
				//+ "and d.tabname = 'EDW_ATUL_CONLD_CLM_SUM_HSTRY' "
				+ "group by d.tabschema, d.tabname, d.datapartitionid, d.datapartitionname "
				+ "order by d.tabschema, d.tabname, d.datapartitionid, d.datapartitionname "
				+ "with ur";
		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				key   = rs.getString(1).trim() + "," + rs.getString(2).trim() + "," + rs.getString(3).trim();
				value = rs.getString(4);
				ht.put(key, value);
				++count;
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(String.format(procName + " Count = %04d Elapsed Time = %s", count, U.getElapsedTime(mainStart)));
		return ht;
	}
	
	private void getTablePartitionSize(String schema, Hashtable<String, String> dtpHashtable)
	{
		long mainStart = System.currentTimeMillis();
		int count = 0;
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		ArrayList<TablePartitionData> list = null;
		
		String key, prevKey = "", sql = "Select t.tabschema, t.tabname, t.DATA_PARTITION_ID, "
				+ "avg(t.DATA_OBJECT_P_SIZE) dsize "
				+ "From table (sysproc.admin_get_tab_info(?,'')) t "
				//+ "From table (sysproc.admin_get_tab_info(?,'EDW_ATUL_CONLD_CLM_SUM_HSTRY')) t "
				+ "group by t.tabschema, t.tabname, t.DATA_PARTITION_ID "
				+ "order by t.tabschema, t.tabname, t.DATA_PARTITION_ID "
				+ "with ur";
		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				key = rs.getString(1).trim() + "," + rs.getString(2).trim();
				TablePartitionData data = new TablePartitionData();
				data.dataPartitionID = rs.getInt(3);
				data.dataPartition   = dtpHashtable.get(key+","+data.dataPartitionID);
				data.size            = rs.getLong(4);
				if (count == 0)
				{
					list = new ArrayList<TablePartitionData>();
					prevKey = key;
					list.add(data);
				} else
				{
					if (prevKey.equals(key))
					{
						list.add(data);
					} else
					{
						TableMapping map = hashTables.get(prevKey);
						if (map != null)
						{
							map.dataPartSizeList = list;
							hashTables.put(prevKey, map);
						}
						list = new ArrayList<TablePartitionData>();
						prevKey = key;
						list.add(data);
					}
				}
				++count;
			}
			TableMapping map = hashTables.get(prevKey);
			if (map != null)
			{
				map.dataPartSizeList = list;
				hashTables.put(prevKey, map);
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(String.format(procName + " Count = %04d Elapsed Time = %s", count, U.getElapsedTime(mainStart)));
	}
	
	private void getIndexSizes(String schema, Hashtable<String, String> dtpHashtable)
	{
		long mainStart = System.currentTimeMillis();
		int count = 0, subCount = 1;
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		ArrayList<IndexData> list = null;
		
		String key, prevKey = "";
		String sql = "Select t.tabschema, t.tabname, t.indschema, t.indname, t.index_partitioning, "
				+ "t.datapartitionid, avg(t.INDEX_OBJECT_P_SIZE) isize "
				+ "From table (sysproc.admin_get_index_info('T',?,'')) t "
				+ "Group by t.tabschema, t.tabname, t.indschema, t.indname, t.index_partitioning, t.datapartitionid "
				+ "Order by t.tabschema, t.tabname, t.indschema, t.indname, t.index_partitioning, t.datapartitionid "
				+ "with ur";

		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				key = rs.getString(1).trim() + "," + rs.getString(2).trim();
				IndexData data = new IndexData();
				data.indschema = rs.getString(3).trim();
				data.indname = rs.getString(4).trim();
				data.indexPartioning = rs.getString(5);
				data.datapartitionid = rs.getInt(6);
				data.indexID = subCount;
				data.dataPartition = dtpHashtable.get(key+","+data.datapartitionid);
				data.size = rs.getLong(7);
				
				if (count == 0)
				{
					list = new ArrayList<IndexData>();
					prevKey = key;
					list.add(data);
				} else
				{
					if (prevKey.equals(key))
					{
						list.add(data);
					} else
					{
						TableMapping map = hashTables.get(prevKey);
						if (map != null)
						{
							debug("map is not null and prevkey="+prevKey);
							map.indexPartSizeList = list;
							hashTables.put(prevKey, map);
						}
						list = new ArrayList<IndexData>();
						prevKey = key;
						list.add(data);
						subCount = 1;
					}
				}
				++count;
				++subCount;
				debug("count="+count+" key="+key);
			}
			TableMapping map = hashTables.get(prevKey);
			if (map != null)
			{
				debug("last map is not null and prevkey="+prevKey);
				map.indexPartSizeList = list;
				hashTables.put(prevKey, map);
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(String.format(procName +" Count = %04d Elapsed Time = %s", count, U.getElapsedTime(mainStart)));
	}
	
	private void getRowsAffectedInfo(String schema, Hashtable<String, String> dtpHashtable)
	{
		long mainStart = System.currentTimeMillis();
		int count = 0;
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		ArrayList<TablePartitionData> list = null;
		
		String key, prevKey = "";
		String sql = "Select t.tabschema, t.tabname, t.DATA_PARTITION_ID, "
				+ "sum(m.rows_inserted), sum(m.rows_deleted), sum(m.rows_updated) "
				+ "From table (sysproc.admin_get_tab_info(?,'')) t "
				+ "group by t.tabschema, t.tabname, t.DATA_PARTITION_ID "
				+ "order by t.tabschema, t.tabname, t.DATA_PARTITION_ID";

		try
		{
			PreparedStatement pstmt = conn.PrepareNewQuery(sql);
			pstmt.setString(1, schema);
			pstmt.setString(2, schema);
			ResultSet rs = conn.ExecuteQuery(pstmt);
			while (rs.next())
			{
				key = rs.getString(1).trim() + "," + rs.getString(2).trim();
				TablePartitionData data = new TablePartitionData();
				data.dataPartitionID = rs.getInt(3);
				data.dataPartition   = dtpHashtable.get(key+","+data.dataPartitionID);				
				data.rowsInserted    = rs.getLong(4);
				data.rowsDeleted     = rs.getLong(5);
				data.rowsUpdated     = rs.getLong(6);
				if (count == 0)
				{
					list = new ArrayList<TablePartitionData>();
					prevKey = key;
					list.add(data);
				} else
				{
					if (prevKey.equals(key))
					{
						list.add(data);
					} else
					{
						TableMapping map = hashTables.get(prevKey);
						if (map != null)
						{
							map.dataPartSizeList = list;
							hashTables.put(prevKey, map);
						}
						list = new ArrayList<TablePartitionData>();
						prevKey = key;
						list.add(data);
					}
				}
				if (count % 500 == 0)
				{
					info(String.format(procName + " Count = %04d Elapsed Time = %s", count, U.getElapsedTime(mainStart)));
				}
				++count;
			}
			TableMapping map = hashTables.get(prevKey);
			if (map != null)
			{
				map.dataPartSizeList = list;
				hashTables.put(prevKey, map);
			}
		} catch (SQLException e)
		{
			error("SQL Error", e);
		}
		info(procName + " Elapsed Time = " + U.getElapsedTime(mainStart));
	}
	
	public ArrayList<TableMapping> getTableInfo()
	{
		return getTableInfo(false);
	}
	
	public ArrayList<TableMapping> getTableInfo(boolean rowsAffected)
	{
		long mainStart = System.currentTimeMillis();
		String procName = new Throwable().getStackTrace()[0].getMethodName();
		String schemaList = conn.getMigrationSchemaInList();
		String schemaIDList = conn.getSelectedSchemaInListID();		
		schemaList = schemaList.substring(2, schemaList.length() - 2);
		String[] sList = schemaList.split(",");
		String[] aList = schemaIDList.split(",");
		int tableID, mlnCount = countMLN();
		Hashtable<String, String> dataPartionHashtable = null;
		
		info(procName + " Started Working ");
		String val = U.getProperty("TableID");
		if (val == null || val.length() == 0)
		{
			tableID = 1;
		} else
		{
			tableID = Integer.valueOf(val);
		}
		for (int i = 0; i < sList.length; ++i)
		{
			dataPartionHashtable = getDataPartitionInfo(sList[i]);
			tableID = getTableInfo(sList[i], tableID, mlnCount, aList[i]);
			getPartInfo(sList[i]);
			if (rowsAffected)
			{
				getRowsAffectedInfo(sList[i], dataPartionHashtable);
			} else
			{
				getTablePartitionSize(sList[i], dataPartionHashtable);
				getIndexSizes(sList[i], dataPartionHashtable);				
			}
		}
		U.putProperty("TableID", String.valueOf(tableID));
		listTables = new ArrayList<TableMapping>(hashTables.values());
		info(procName + " Elapsed Time = " + U.getElapsedTime(mainStart));
		return listTables;
	}

	public String genDBPartGroups(ArrayList<DBGroupData> list)
	{
		StringBuffer sql = new StringBuffer();
		String genStr;
		for (DBGroupData pdata : list)
		{
			genStr = pdata.getdstSQL();
			debug(genStr);
			sql.append(genStr + U.linesep);
		}
		debug("=========================================================");
		return sql.toString();
	}

	public ArrayList<BufferpoolData> validateBufferpools(ArrayList<BufferpoolData> source)
	{
		for (BufferpoolData data : source)
		{
			data.exists = conn.checkBufferPools(data.bufferpoolName);
		}
		return source;
	}

	public String genBufferpools(ArrayList<BufferpoolData> list)
	{
		StringBuffer sql = new StringBuffer();
		String genStr;

		for (BufferpoolData data : list)
		{
			genStr = data.getSQL();
			debug(genStr);
			sql.append(genStr + U.linesep);
		}

		debug("=========================================================");
		return sql.toString();
	}

	public ArrayList<Db2lookInfoData> db2lookValidation(
			ArrayList<Db2lookInfoData> source)
	{
		conn.openObject();
		for (Db2lookInfoData data : source)
		{
			data.exists = conn.checkObject(data.objType, data.objSchema,
					data.objName, data.objAttribute);
		}
		conn.closeObject();
		return source;
	}

	private void initFiles(ArrayList<Db2lookInfoData> list, String boilerPlate)
	{
		Set<String> files = new HashSet<String>();
		for (Db2lookInfoData data : list)
		{
			if (data.objType.equalsIgnoreCase("SEQUENCE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE"))
			{
				data.setName("04sequences", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("SEQUENCE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				data.setName("05altersequences", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE")
					&& !data.tabType.equalsIgnoreCase("S"))
			{
				data.setName("06tables", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE")
					&& data.tabType.equalsIgnoreCase("S"))
			{
				data.setName("07mqts", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				data.setName("08altertables", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("COMMENT"))
			{
				data.setName("09tablecomments", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("TRIGGER"))
			{
				data.setName("11triggers", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("PKEY")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				data.setName("12pkeys", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("INDEX")
					&& data.sqlOperation.equalsIgnoreCase("CREATE"))
			{
				data.setName("13indexes", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("INDEX")
					&& data.sqlOperation.equalsIgnoreCase("COMMENT"))
			{
				data.setName("14indexcomments", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("CHECK"))
			{
				data.setName("15checks", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("UNIQUE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				data.setName("16unique", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("ALIAS"))
			{
				data.setName("17alias", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("FUNCTION"))
			{
				data.setName("18functions", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("PROCEDURE"))
			{
				data.setName("19procedures", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("VIEW"))
			{
				data.setName("10views", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("MASK"))
			{
				data.setName("20masks", boilerPlate);
			} else if (data.objType.equalsIgnoreCase("FKEY")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				data.setName("21fkeys", boilerPlate);
			}
			if (data.getName() != null)
				files.add(data.getName());
		}
		ArrayList<String> sortedfiles = new ArrayList<String>(files);
		Collections.sort(sortedfiles);
		listFileNames.add(sortedfiles);
	}

	private void genDDLs(ArrayList<Db2lookInfoData> list, int token,
			SchemaData schemaData)
	{
		StringBuffer buffer = new StringBuffer();
		Regex r = new Regex();
		String sql;
		String procTerminator;

		if (list == null)
		{
			list = getdb2lookInfo(token, schemaData);
		}

		buffer.append("SET CURRENT SCHEMA = \"" + schemaData.schema + "\""
				+ U.sqlTerminator + U.linesep);
		buffer.append("SET CURRENT PATH = \"SYSIBM\",\"SYSFUN\",\"SYSPROC\",\""
				+ schemaData.schema + "\"" + U.sqlTerminator + U.linesep);
		buffer.append("SET SYSIBM.NLS_STRING_UNITS = 'SYSTEM'" + U.sqlTerminator
				+ U.linesep);
		procTerminator = "--#SET TERMINATOR @" + U.linesep;

		initFiles(list, buffer.toString());
		for (Db2lookInfoData data : list)
		{
			if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE")
					&& !data.tabType.equalsIgnoreCase("S"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				debug("Modified SQL Begins for Tables ===============");
				sql = r.modifyTableDDL(data.table, sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE")
					&& data.tabType.equalsIgnoreCase("S"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				debug("Modified SQL Begins for MQTs ===============");
				sql = r.modifyMQTDDL(data.table, sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("TABLE")
					&& data.sqlOperation.equalsIgnoreCase("COMMENT"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("PKEY")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("INDEX")
					&& data.sqlOperation.equalsIgnoreCase("CREATE"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				debug("Modified SQL Begins for Indexes ===============");
				sql = r.modifyIndexDDL(data.table, schemaData.schema, sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("INDEX")
					&& data.sqlOperation.equalsIgnoreCase("COMMENT"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				sql = r.modifyIndexDDL(data.table, schemaData.schema, sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("CHECK"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("UNIQUE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("FKEY")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("ALIAS"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("FUNCTION"))
			{
				sql = data.getSQLStatement("@");
				debug(sql);
				U.WriteFile(data.getName(), procTerminator + sql);
			} else if (data.objType.equalsIgnoreCase("MASK"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("PROCEDURE"))
			{
				sql = data.getSQLStatement("@");
				debug(sql);
				U.WriteFile(data.getName(), procTerminator + sql);
			} else if (data.objType.equalsIgnoreCase("SEQUENCE")
					&& data.sqlOperation.equalsIgnoreCase("CREATE"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("SEQUENCE")
					&& data.sqlOperation.equalsIgnoreCase("ALTER"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			} else if (data.objType.equalsIgnoreCase("TRIGGER"))
			{
				sql = data.getSQLStatement("@");
				debug(sql);
				U.WriteFile(data.getName(), procTerminator + sql);
			} else if (data.objType.equalsIgnoreCase("VIEW"))
			{
				sql = data.getSQLStatement();
				debug(sql);
				U.WriteFile(data.getName(), sql);
			}
		}
		info("--=========================================================");
		info("--Total tablespaces generated through DDL modification = "
				+ r.getCountTablespaces());
		info("--=========================================================");
	}

	public void cleanLookTable()
	{
		try
		{
			conn.CallPrepareOnly("CALL SYSPROC.DB2LK_CLEAN_TABLE(?)");
			for (Integer data : tokenList)
			{
				conn.cstmt.setLong(1, data);
				conn.cstmt.executeUpdate();
			}
			conn.cstmt.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}

	public void genRunstats(String fileName)
	{
		StringBuffer list = new StringBuffer();
		for (TableMapping data : listTables)
		{
			String sql = data.getStatsProfile();
			debug(sql);
			if (sql.length() > 0)
				list.append(sql);
		}
		U.WriteFile(fileName, list.toString());
	}

	public void genObjectsDDLs(int opToken, SchemaData schemaData, ArrayList<Db2lookInfoData> target)
	{
		genDDLs(target, opToken, schemaData);
		cleanLookTable();
	}

	public int genLook(String schema)
	{
		long starttime = 0L;
		int opToken = -1;
		try
		{
			conn.CallPrepareOnly("CALL SYSPROC.DB2LK_GENERATE_DDL(?, ?)");
			info("=========================================");
			info("DDL generation started for " + schema);
			info("=========================================");
			conn.cstmt.setString(1, "-e -z " + schema);
			//conn.cstmt.setString(1, "-e -z " + schema + " -tw EDW_ATUL_CONLD_CLM_SUM_HSTRY");
			conn.cstmt.registerOutParameter(2, Types.BIGINT);
			conn.cstmt.executeUpdate();
			opToken = conn.cstmt.getInt(2);
			conn.cstmt.close();
			tokenList.add(opToken);
			info("db2look took " + U.getElapsedTime(starttime));
		} catch (SQLException e)
		{
			error("Error in genlook", e);
		}
		return opToken;
	}

	public int getTotalTablespacesCount()
	{
		int count = 0;
		for (TableMapping data : listTables)
		{
			count += (data.getCountDataTS() + data.getCountIndexTS());
		}
		return count;
	}
	
	private void createDriverShellScript()
	{
		String jarName;		
		String path = SQLQueries.class.getResource(SQLQueries.class.getSimpleName() + ".class").getFile();
	    if(path.startsWith("/")) 
	    {
	       jarName = System.getProperty("user.dir") + U.filesep + "migrkit.jar";
	    } else
	    {
	       debug("path = " + path);
	       jarName = path.substring(0, path.lastIndexOf('!'));
	       if (jarName.startsWith("file:"))
	    	   jarName = jarName.substring(5);
	       debug("jar name = " + jarName);
	    }
		String fileName = U.getOutputDirName() + U.filesep + U.driverShellScriptName;
		String scriptName = U.getOutputDirName() + U.filesep + U.deployObjectsFileName;
		String logFileName = U.getOutputDirName() + U.filesep + U.ddlLogFileName;
		BufferedWriter writer = null;
		StringBuffer buffer = new StringBuffer();
		
		buffer.append("#!" + U.getShell() + U.linesep + U.linesep);
		buffer.append("# Copyright(r) IBM Corporation" + U.linesep);
		buffer.append("#" + U.linesep + U.linesep);
		buffer.append("# Run this script on the target Db2 server to deploy objects" + U.linesep);
		buffer.append("#" + U.linesep + U.linesep);
		buffer.append(scriptName + " | tee " + logFileName + U.linesep);
		buffer.append(U.linesep);
		buffer.append("JAVA=$(db2jdkp)/bin/java" + U.linesep);
		buffer.append(U.linesep);
		buffer.append("if [[ ! -f $JAVA ]] ; then" + U.linesep);
		buffer.append("   echo Java not found. Exiting ..." + U.linesep);
		buffer.append("   exit 1" + U.linesep);
		buffer.append("fi" + U.linesep);
		buffer.append(U.linesep);
		buffer.append("PROG=com.ibm.migr.data.AnalyzeLog" + U.linesep);
		buffer.append("CLPATH=\"-cp "+jarName+"\"" + U.linesep);
		buffer.append(U.linesep);
		buffer.append("$JAVA $CLPATH $PROG "+logFileName+" | tee "+logFileName+".result" + U.linesep);
		buffer.append(U.linesep);
		
		try
		{
			writer = new BufferedWriter(new FileWriter(fileName, false));
			debug(buffer.toString());
			writer.write(buffer.toString());
			writer.close();
			if (!U.win())
			{
				new RunCommand("/bin/chmod +x " + fileName, "").run();
			}
		} catch (IOException e)
		{
			error("Error writing to file", e);
		}
	}

	public void createShellScript()
	{
		createDriverShellScript();
		
		String storage;
		String[] storages = { U.stogroupdata, U.stogroupidx, U.stogrouptemp };
		String[] stogroups;
		BufferedWriter writer = null;
		StringBuffer buffer = new StringBuffer();

		buffer.append("#!" + U.getShell() + U.linesep + U.linesep);
		buffer.append("# Copyright(r) IBM Corporation" + U.linesep);
		buffer.append("#" + U.linesep + U.linesep);

		buffer.append("echo -------------------------------------------------------------------" + U.linesep);
		buffer.append("echo Deploying Objects ..." + U.linesep);
		buffer.append("echo -------------------------------------------------------------------" + U.linesep);
		for (int j = 0; j < storages.length; ++j)
		{
			if (storages[j] != null)
			{
				storage = storages[j].split("\\|")[0];
				stogroups = storages[j].split("\\|")[1].split(",");
				for (int i = 0; i < stogroups.length; ++i)
				{
					buffer.append("echo Checking directory " + stogroups[i] + " for the storage group " + storage + U.linesep);
					buffer.append("echo -------------------------------------------------------------------" + U.linesep);
					buffer.append("if [[ -d " + stogroups[i] + " ]] ; then" + U.linesep);
					buffer.append("   echo \"directory " + stogroups[i] + " found\"" + U.linesep);
					buffer.append("else" + U.linesep);
					buffer.append("   echo \"directory " + stogroups[i] + " not found required for creating storage group " + storage + ". Exiting ...\"" + U.linesep);
					buffer.append("   exit 1" + U.linesep);
					buffer.append("fi" + U.linesep);
					buffer.append("echo -------------------------------------------------------------------" + U.linesep);
				}
			}
		}
		buffer.append("db2 -v CONNECT TO BLUDB" + U.linesep);
		buffer.append("db2 -tvf " + U.dbpgbptsFileName + U.linesep);
		buffer.append("db2 -tvf " + U.tablespacesFileName + U.linesep);
		buffer.append("db2 -tvf " + U.schemasFileName + U.linesep);
		buffer.append("db2 -tvf " + U.globalVariableFileName + U.linesep);
		for (ArrayList<String> list : listFileNames)
		{
			buffer.append("echo -------------------------------------------------------------------" + U.linesep);
			for (String fileName : list)
			{
				if (fileName != null && fileName.length() > 0)
				{
					debug("Create script " + fileName);
					buffer.append("db2 -tvf " + fileName + U.linesep);
				}
			}
			buffer.append("echo -------------------------------------------------------------------" + U.linesep);
		}
		buffer.append("db2 -tvf " + U.runstatsFileName + U.linesep);
		buffer.append("db2 -tvf " + U.authsFileName + U.linesep);
		buffer.append("db2 -v CONNECT RESET" + U.linesep);

		try
		{
			String file = U.getOutputDirName() + U.filesep + U.deployObjectsFileName;
			writer = new BufferedWriter(new FileWriter(file, false));
			debug(buffer.toString());
			writer.write(buffer.toString());
			writer.close();
			if (!U.win())
			{
				new RunCommand("/bin/chmod +x " + file, "").run();
			}
		} catch (IOException e)
		{
			error("Error writing to file", e);
		}
	}

	public static void main(String[] args)
	{
		SQLQueries p = new SQLQueries(null);
		set(LEVEL_DEBUG);
		p.createDriverShellScript();
	}
}
