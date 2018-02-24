package com.ibm.migr.db2;

import static com.ibm.migr.utils.Log.*;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Properties;
import java.util.Set;

import com.ibm.migr.utils.Crypt;
import com.ibm.migr.utils.U;


/**
 * @author Vikram. Main db2 class to open, close connection and do
 * basic stuff
 *
 */
public class db2
{
	public ResultSet rs = null;
	public Statement statement = null;
	public PreparedStatement queryStatement = null;
	public CallableStatement cstmt = null;
	private ServerData sd = new ServerData();
	private Hashtable<String, PreparedData> hTable = new Hashtable<String, PreparedData>();
	private ArrayList<SchemaData> schemaData = new ArrayList<SchemaData>();
	private Connection connection = null;
	private Properties connProp = new Properties();
	private Properties progProp = null;
	private int runid = 0;
	
	/**
	 * To open a connection and write results into a file with thread name
	 * @param fp
	 * @param clientUser
	 * @param tname
	 */
	public db2(BufferedWriter fp, String clientUser, int runID, String tname,
			boolean connectionRoundRobin)
	{
		this.runid = runID;
		getConnection(null, clientUser, connectionRoundRobin);
	}
	
	public db2(String source, BufferedWriter fp, String clientUser, int runID, String tname,
			boolean connectionRoundRobin)
	{
		this.runid = runID;
		getConnection(source, clientUser, connectionRoundRobin);
	}
	
	/**
	 * Open a db2 connection
	 */
	public db2()
	{
		getConnection(null, null, false);
	}
	
	/**
	 * Open a db2 connection
	 */
	public db2(String source)
	{
		getConnection(source, null, false);
	}
	
	/**
	 * return database name to be used in scripting
	 * @return
	 */
	public String getDBName()
	{
		return sd.dbname.toUpperCase();
	}
	
	/**
	 * Return client user name for scripting
	 * @return
	 */
	public String getClientUser()
	{
		return sd.clientUser;
	}

	/**
	 * return client program name for scripting 
	 * @return
	 */
	public String getApplicationName()
	{
		return sd.applicationName;
	}

	/**
	 * Return client accounting information
	 * @return
	 */
	public String getClientAccountingInformation()
	{
		return sd.clientAccountingInformation;
	}

	/**
	 * Return work station name
	 * @return
	 */
	public String getClientHostName()
	{
		return sd.clientHostname;
	}
	
	public String getServerName()
	{
		return sd.server;
	}
	
	/**
	 * set client user name for WLM purposes
	 * @param propertyName
	 */
	public void setClientUser(String propertyName)
	{
		if (connection != null)
		{
			try
			{
				connection.setClientInfo("ClientUser", propertyName);
				sd.clientUser = propertyName;
			} catch (SQLException e)
			{
				error("Error", e);
			}
		}
	}

	/**
	 * Set client accounting information for WLM purposes
	 * @param propertyName
	 */
	public void setClientAccountingInformation(String propertyName)
	{
		if (connection != null)
		{
			try
			{
				connection.setClientInfo("ClientAccountingInformation", propertyName);
				sd.clientAccountingInformation = propertyName;
			} catch (SQLException e)
			{
				error("Error", e);
			}
		}
	}
	
	/**
	 * Set client work station name for WLM purposes
	 * @param propertyName
	 */
	public void setClientHostName(String propertyName)
	{
		if (connection != null)
		{
			try
			{
				connection.setClientInfo("ClientHostname", propertyName);
				sd.clientHostname = propertyName;
			} catch (SQLException e)
			{
				error("Error", e);
			}
		}
	}
	
	public void CallPrepareOnly(String sql) throws SQLException
	{
		String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	cstmt = connection.prepareCall(sql);
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
	}
	
	public void PrepareOnly(String sql) throws SQLException
	{
		String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
	}

	/**
	 * Prepare a prepared statement
	 * @param procName - Proc name for debugging and elapsed time purposes
	 * @param sql - SQL for the prepared statement
	 * @return
	 * @throws SQLException
	 */
    public ResultSet CallPrepare(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	cstmt = connection.prepareCall(sql);
    	cstmt.execute();
    	rs = cstmt.getResultSet();
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }    
    
    /**
     * get column value for the index from result set
     * @param colIndex
     * @return
     * @throws Exception
     */
	public String GetColumnValue(int colIndex) throws Exception
    {
        String tmp = "";        
        try
        {
            tmp = rs.getString(colIndex);                         
        } catch (SQLException e)
        {
           debug("Col[" + colIndex + "] Error:" + e.getMessage());
           return "SkipThisRow";               
        }
        return tmp;
    }
    
	/**
	 * Prepare statement for SQL
	 * @param procName - Used for debugging elapsed time
	 * @param sql
	 * @return
	 * @throws SQLException
	 */
    public PreparedStatement Prepare(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	queryStatement.setFetchSize(sd.fetchSize);
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    	return queryStatement;
    }    
    
    /**
     * General purpose routine to do INSERT/UPDATE/DELETE and
     * get affected rows
     * @param sql
     * @return
     */
    public int executeUpdate(String sql)
    {
    	int sqlCount = 0;

        PreparedStatement statment = null;

        try
        {
        	queryStatement = connection.prepareStatement(sql);
        	sqlCount = queryStatement.executeUpdate();
	    }
        catch (SQLException ex)
        {
        	if (ex.getErrorCode() == -3603)
        	{
        		return -3603;
        	} else if (ex.getErrorCode() == -3600)
        	{
        		return -3600;
        	} else if (ex.getErrorCode() == -601)
        	{
        		return 0;
        	} else if (ex.getErrorCode() == -3608)
        	{
        		return -3608;
        	}
        	else
        	{
    	    	error("Error in executeUpdate sql="+sql, ex);
        	}
        }
        catch (Exception e)
	    {
	    	error("Error is executing sql="+sql, e);
	    } finally
	    {
		    if (statment != null)
				try {statment.close(); } catch (SQLException e) { e.printStackTrace();	}	    	
	    }
        return sqlCount;
    }

    /**
     * Execute and record elapsed time for debugging
     * @param procName
     * @param sql
     * @throws SQLException
     */
    public void PrepareExecute(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	queryStatement.executeQuery();
    	queryStatement.close();
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    }
    
    public ResultSet ExecuteQuery(PreparedStatement pstmt) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	rs = pstmt.executeQuery();
    	debug(procName, " ("+U.getElapsedTime(start)+")");
    	return rs;
    }

    public ResultSet ExecuteQuery(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	rs = queryStatement.executeQuery();
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }
    
    /**
     * Insert / update and delete and get elapsed time
     * @param procName
     * @param sql
     * @return
     */
    public boolean ExecuteUpdate(String sql)
    {
    	if (sql == null || sql.length() == 0) return false;
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	boolean ret = false;
    	long start = System.currentTimeMillis();
    	try
		{
        	queryStatement = connection.prepareStatement(sql);
			queryStatement.executeUpdate();
			queryStatement.close();
			ret = true;
	    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
		} catch (SQLException e)
		{
			if (e.getErrorCode() == -601 || e.getErrorCode() == -204)
			{
				if (e.getErrorCode() == -601)
					info("Object exists sql=" + sql);
				if (e.getErrorCode() == -204)
					info("Object does not exist sql=" + sql);
			   return false;
			} else if (e.getErrorCode() == -290)
			{
				error("Critical table space error. SQL Code = " + e.getErrorCode() + " Consider circular logging. Exiting... : ");
				System.exit(-1);
			}
			else
			   e.printStackTrace();
		}
    	return ret;
    }
    
    /**
     * Get first column value from the result set
     * @param procName
     * @param sql
     * @return
     */
    public String ExecuteQueryFirstColumn(String sql)
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	if (sql == null || sql.length() == 0) return "";
    	String str = "";
    	long start = System.currentTimeMillis();
    	try
		{
        	queryStatement = connection.prepareStatement(sql);
			rs = queryStatement.executeQuery();
	    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
	    	if (rs.next())
	    	{
	    		str = rs.getString(1);
	    	}
	    	queryStatement.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
    	return str.trim();
    }
    
    /**
     * Execute a SELECT statement and return the result set
     * @param procName
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet ExecuteStatementQuery(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	rs = statement.executeQuery(sql);
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);   
    	return rs;
    }
    
    public PreparedStatement PrepareNewQuery(String sql) throws SQLException
    {
    	PreparedStatement pstmt;
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	pstmt = connection.prepareStatement(sql);
    	pstmt.setFetchSize(sd.fetchSize);
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    	return pstmt;
    }
    
    /**
     * Execute a result set for a SQL
     * @param procName
     * @param sql
     * @return
     * @throws SQLException
     */
    public ResultSet PrepareExecuteQuery(String sql) throws SQLException
    {
    	String procName = new Throwable().getStackTrace()[1].getMethodName();
    	long start = System.currentTimeMillis();
    	queryStatement = connection.prepareStatement(sql);
    	queryStatement.setFetchSize(sd.fetchSize);
    	rs = queryStatement.executeQuery();
    	debug(procName, " ("+U.getElapsedTime(start)+") SQL=" + sql);
    	return rs;
    }
    
    /**
     * Get next result set for scroll purposes
     * @return
     */
    public boolean next()
    {
    	boolean n = false;
    	try
		{
			n = rs.next();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return n;
    }
    
    public int getDataBuffer()
    {
    	return sd.dataBuffer;
    }
    
    public int getNumCPU()
    {
    	return sd.numCPU;
    }
    
    public String getInstanceName()
    {
    	String instance = ExecuteQueryFirstColumn("select inst_name From table(sysproc.env_get_inst_info()) as t");
    	return instance;
    }
     
    /**
     * Get current schema name for scripting purposes
     * @return
     */
    public String getCurrentSchema()
    {
    	String schema = ExecuteQueryFirstColumn("values current schema");
    	return schema;
    }
    
    /**
     * Close the result set
     * @param methodName
     */
	public void closeReader()
	{
		String methodName = new Throwable().getStackTrace()[1].getMethodName();
		try
		{
    		if (rs != null)
    		  rs.close();
		} catch (Exception e)
		{
		   error("Error in closing Reader in method " + methodName);
		}
		try
		{
			if (queryStatement != null)
			  queryStatement.close();
		} catch (Exception e)
		{
		   error("Error in closing PreparedStatement in method  " + methodName);
		}
		try
		{
			if (statement != null)
			   statement.close();
		} catch (Exception e)
		{
		   error("Error in closing Statement in method  " + methodName);
		}
	}
	
	/**
	 * Read connection and other program specific properties.
	 * The name of the file can be overridden by using -D switch
	 * The file can be in jar file also 
	 * @param dToken
	 */
    private void loadConnectionPropertyFile(String dToken)
    {
    	if (progProp == null)
    	{
    		progProp = new Properties();
        	String propFile = System.getProperty(dToken);
        	if (propFile == null || propFile.equals(""))
        	{
        	    propFile = U.CONNECTION_PROPERTY_FILE;
        	}
            try
            {
    	        if (U.FileExists(propFile))
    	        {
    	        	progProp.load(new FileInputStream(propFile));
    	            info("Local Configuration file loaded: '" + propFile + "'" + "("+progProp.size()+")");            	
    	        } else
    	        {
    	        	InputStream istream = ClassLoader.getSystemResourceAsStream(propFile);
    	            if (istream == null)
    	            {
    	            	progProp.load(new FileInputStream(propFile));
    	                info("Configuration file loaded: '" + propFile + "'" + "("+progProp.size()+")");
    	            } else
    	            {
    	            	progProp.load(istream);
    	                info("Configuration file loaded from jar: '" + propFile + "'" + "("+progProp.size()+")");
    	            }            	
    	        }
            } catch (Exception e)
            {
            	info("Error loading " + dToken + " property file " + propFile);
            	progProp = null;
            	e.printStackTrace();
            }
    	}
    }
    
    /**
     * Return the connection
     * @return
     */
    public Connection getConnection()
    {
    	return getConnection(null, null, false);
    }
    
    public String getSelectedSchemaInListID()
    {
    	String str = "";
    	boolean once = true;
    	for (SchemaData data : schemaData)
    	{
    		if (data.selected)
    		{
	    		if (once)
	    		{
	    			str = data.getSchemaID();
	    			once = false;
	    		} else
	    		{
	    			str = str + "," + data.getSchemaID();
	    		}
    		}
    	}
    	return str;
    }
    
    public ArrayList<SchemaData> getSelectedSchemaList()
    {
    	return schemaData;
    }
	
    /**
     * Return the connection and set the client attributes for WLM purposes
     * Also set program specific values in addition to connection properties
     * If connection is already established, then do not repeat
     * @param clientUser
     * @return
     */
	public Connection getConnection(String source, String clientUser, boolean connectionRoundRobin)
	{
		if (connection != null)
			return connection;
		
		loadConnectionPropertyFile(source);
		for (Enumeration<?> e = progProp.propertyNames(); e.hasMoreElements();)
		{
			String key = (String) e.nextElement();
			String val = progProp.getProperty(key);
			if (key != null && key.length() > 0)
				key = key.trim();
			if (val != null && val.length() > 0)
				val = val.trim();
			if (!key.equalsIgnoreCase("password"))
				debug(key + "=" +  val);
			if (key.equalsIgnoreCase("userid"))
			{
				sd.userid = val;					
			} else if (key.equalsIgnoreCase("password"))
			{				
				if (val.startsWith("zxc_"))
				{
					sd.password = new Crypt().decrypt(val.substring(4));
				} else
				    sd.password = val;
				debug(key + "=*********");
			} else if (key.equalsIgnoreCase("dbname"))
			{
				sd.dbname = val;
			} else if (key.equalsIgnoreCase("server"))
			{
				String[] servers = val.split(",");
				String assignedServer = (connectionRoundRobin) ?
						servers[runid%servers.length] : servers[0];
				sd.server = assignedServer;
				debug(key + "=" +  assignedServer + " runid=" + runid + " servers.length=" + servers.length);
			} else if (key.equalsIgnoreCase("port"))
			{
				sd.port = Integer.valueOf(val);
			} else if (key.equalsIgnoreCase("autoCommit"))
			{
				if (!(val == null || val.length() == 0))
				   sd.autoCommit = Boolean.valueOf(val);
			} else if (key.equalsIgnoreCase("fetchSize"))
			{
				if (!(val == null || val.length() == 0))
				{
					try
					{
					   int f = Integer.parseInt(val);
					   sd.fetchSize = f;
					   U.fetchSize = f;
					} catch (Exception ex)
					{
						ex.printStackTrace();
					}
					
				}
			} else if (key.equalsIgnoreCase("migrationSchemaList"))
			{
				if (!(val == null || val.length() == 0))
				   sd.migrationSchemaList = val;
			} else if (key.equalsIgnoreCase("dataBuffer"))
			{
				if (!(val == null || val.length() == 0))
				   sd.dataBuffer = Integer.valueOf(val);
			} else if (key.equalsIgnoreCase("numCPU"))
			{
				if (!(val == null || val.length() == 0))
				   sd.numCPU = Integer.valueOf(val);
			} else if (key.equalsIgnoreCase("targetMLNCount"))
			{
				if (!(val == null || val.length() == 0))
				   U.targetMLNCount = Integer.valueOf(val);
			} else if (key.equalsIgnoreCase("dataBufferpool"))
			{
				if (!(val == null || val.length() == 0))
				   U.dataBufferpool = val;
			} else if (key.equalsIgnoreCase("idxBufferpool"))
			{
				if (!(val == null || val.length() == 0))
				   U.idxBufferpool = val;
			} else if (key.equalsIgnoreCase("tempBufferpool"))
			{
				if (!(val == null || val.length() == 0))
				   U.tempBufferpool = val;
			} else if (key.equalsIgnoreCase("extentSize"))
			{
				if (!(val == null || val.length() == 0))
				   U.extentSize = Integer.valueOf(val);
			} else if (key.equalsIgnoreCase("mapdbpg"))
			{
				if (!(val == null || val.length() == 0))
				   U.mapdbpg = val;
			} else if (key.equalsIgnoreCase("stogroupdata"))
			{
				if (!(val == null || val.length() == 0))
				   U.stogroupdata = val;
			} else if (key.equalsIgnoreCase("stogroupidx"))
			{
				if (!(val == null || val.length() == 0))
				   U.stogroupidx = val;
			} else if (key.equalsIgnoreCase("stogrouptemp"))
			{
				if (!(val == null || val.length() == 0))
				   U.stogrouptemp = val;
			} else if (key.equalsIgnoreCase("initialSizeIncrement"))
			{
				if (!(val == null || val.length() == 0))
				   U.initialSizeIncrement = Double.valueOf(val);
			} else
			{
				if (key.equals("currentSchema"))
				   sd.currentSchema = val;
				if (key.equals("clientUser"))
				   sd.clientUser = val;
				if (key.equals("clientHostname"))
				   sd.clientHostname = val;
				if (key.equals("clientAccountingInformation"))
				   sd.clientAccountingInformation = val;
				if (key.equals("applicationName"))
				   sd.applicationName = val;
				connProp.setProperty(key, val);
			}			
		}
		if (sd.userid != null)
		{
			String pass = (sd.password == null) ? "" : sd.password;
	        pass = (pass.matches("^\\s*$")) ? "" : pass;
			connProp.setProperty("user", sd.userid);
			connProp.setProperty("password", pass);
		}
		
		if (clientUser != null)
		{
			connProp.setProperty("clientUser", clientUser.toUpperCase());
		}
		
		try
		{
			String url = "jdbc:db2://" + sd.server + ":" + sd.port + "/" + sd.dbname;			
			if (!U.driverLoaded)
			{
				String driverName = "com.ibm.db2.jcc.DB2Driver";
			    Class.forName(driverName).newInstance();
			    U.driverLoaded = true;
	            info("Passenger Loaded :" + driverName);
			}
			connection = DriverManager.getConnection(url, connProp);
			connection.setClientInfo("ClientUser", sd.clientUser);
			connection.setClientInfo("ClientAccountingInformation", sd.clientAccountingInformation);
			connection.setClientInfo("ApplicationName", sd.applicationName);
			connection.setClientInfo("ClientHostname", sd.clientHostname);
			connection.setAutoCommit(sd.autoCommit);
			
			
			if (sd.migrationSchemaList == null || (sd.migrationSchemaList.length() > 0
					&& sd.migrationSchemaList.equalsIgnoreCase("ALL")))
			{
				schemaData = getAllSchema(true);
				sd.migrationSchemaList = "";
				boolean once = true;
				for (SchemaData str : schemaData)
				{
					if (!once)
						sd.migrationSchemaList += ",";
					sd.migrationSchemaList += str.schema;
					once = false;
				}
			} else
			{
				String[] schema = sd.migrationSchemaList.split(",");
				schemaData = getAllSchema(false);
				for (SchemaData data : schemaData)
				{
					for (int i = 0; i < schema.length; ++i)
					{
						if (data.schema.equals(schema[i]))
						{
							data.selected = true;
						}
					}
				}
			}
						
			DatabaseMetaData md = connection.getMetaData();
            info("Database Product Name :" + md.getDatabaseProductName());
            info("Database Product Version :" + md.getDatabaseProductVersion());
            info("JDBC driver " + md.getDriverName() + " Version = " + md.getDriverVersion());
            U.majorSourceDBVersion = md.getDatabaseMajorVersion();
            U.minorSourceDBVersion = md.getDatabaseMinorVersion();
            info("Database Major Version :" + U.majorSourceDBVersion);
            info("Database Minor Version :" + U.minorSourceDBVersion);
            info("ClientUser=" + connection.getClientInfo("ClientUser"));
            info("ClientAccountingInformation=" + connection.getClientInfo("ClientAccountingInformation"));
            info("ApplicationName=" + connection.getClientInfo("ApplicationName"));
            info("ClientHostname=" + connection.getClientInfo("ClientHostname"));
            
		} catch (InstantiationException ex)
		{
			ex.printStackTrace();
		} catch (IllegalAccessException ex)
		{
			ex.printStackTrace();
		} catch (ClassNotFoundException ex)
		{
			ex.printStackTrace();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		if (connection == null)
			System.exit(-1);
		return connection;
	}

	/**
	 * Commit a transaction
	 */
	public void commit()
	{
		if (connection != null)
		{
			try
			{
				if (!sd.autoCommit)
				   connection.commit();
			} catch (SQLException e)
			{
				e.printStackTrace();
			}			
		}
	}
	
	/**
	 * Close connection and commit before it
	 */
	public void close()
	{
		if (connection != null)
		{
			try
			{
				if (!connection.isClosed())
				   connection.commit();
			} catch (SQLException ex)
			{
				ex.printStackTrace();
			}
			try
			{
				connection.close();
				sd.isOpen = false;
			} catch (SQLException ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	/** 
	 * Overridde auto commit
	 * @param value
	 */
	public void setAutoCommit(Boolean value)
	{
		sd.autoCommit = value;
	}
	
	public ArrayList<SchemaData> getAllSchema(boolean isSelected)
	{
		int id = 1;
		ArrayList<SchemaData> list = new ArrayList<SchemaData>();
		String sql = "select schemaname, owner, remarks from syscat.schemata "
				+ "where definertype = 'U' order by schemaname";
		try
		{
			ResultSet rs = PrepareExecuteQuery(sql);
			while (rs.next())
			{
				SchemaData data = new SchemaData();
				data.schema = rs.getString(1).trim();
				data.owner  = rs.getString(2);
				data.remarks = rs.getString(3);
				data.id = id;
				data.selected = isSelected;
				list.add(data);
				++id;
			}					
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return list;
	}
	
	/*public ArrayList<SchemaData> getMigrationSchemaList()
	{
		return sd.getMigrationSchemaList();
	}*/
	
	public String getMigrationSchemaInList()
	{
		return sd.getMigrationSchemaInList();
	}
	
	public void openObject()
	{
		hTable.put("ALIAS", new PreparedData(connection, "select 'X' from syscat.tables t full outer join syscat.modules m on m.moduleschema = t.tabschema and m.modulename = t.tabname and type = 'A' full outer join syscat.sequences s on s.seqschema = t.tabschema and s.seqname = t.tabname and moduletype = 'A' where t.tabschema = ? and t.tabname = ? and t.type = 'A'"));
		hTable.put("CHECK", new PreparedData(connection, "select 'X' from syscat.checks where tabschema = ? and constname = ?")); // constname = obj_attribute
		hTable.put("FKEY", new PreparedData(connection, "select 'X' from syscat.references where tabschema = ? and constname = ?")); // constname = obj_attribute
		hTable.put("FUNCTION", new PreparedData(connection, "select 'X' from syscat.routines where routineschema = ? and specificname = ?")); // specificname = obj_name
		hTable.put("INDEX", new PreparedData(connection, "select 'X' from syscat.indexes where indschema = ? and indname = ?")); // indname = obj_name
		hTable.put("MASK", new PreparedData(connection, "select 'X' from syscat.controls where tabschema = ? and controlname = ?")); // controlname = obj_name
		hTable.put("PKEY", new PreparedData(connection, "select 'X' from syscat.tabconst where tabschema = ? and constname = ?")); // constname = obj_attribute
		hTable.put("PROCEDURE", new PreparedData(connection, "select 'X' from syscat.routines where routineschema = ? and specificname = ?")); // specificname = obj_name
		hTable.put("SCHEMA", new PreparedData(connection, "select 'X' from syscat.schemata where schemaname = ? OR schemaname = ?")); // schemaname = obj_name
		hTable.put("SEQUENCE", new PreparedData(connection, "select 'X' from syscat.sequences where seqschema = ? and seqname = ?")); // seqname = obj_name
		hTable.put("TABLE", new PreparedData(connection, "select 'X' from syscat.tables where tabschema = ? and tabname = ?")); //tabname = obj_name
		hTable.put("TRIGGER", new PreparedData(connection, "select 'X' from syscat.triggers where trigschema = ? and trigname = ?")); // trigname = obj_name
		hTable.put("UNIQUE", new PreparedData(connection, "select 'X' from syscat.tabconst where tabschema = ? and constname = ?")); // constname = obj_attribute
		hTable.put("VIEW", new PreparedData(connection, "select 'X' from syscat.views where viewschema = ? and viewname = ?")); //viewname = obj_name
	}	
	
	public boolean checkObject(String type, String schema, String name, String attribute)
	{
		PreparedData data = hTable.get(type);
		switch (type)
		{
		    case "CHECK" : case "FKEY" : case "PKEY" : case "UNIQUE" :
		       name = attribute;
		       break;
		}
		return data.doesExist(schema, name);
	}
	
	public void closeObject()
	{
		Set<String> keys = hTable.keySet();
		for (String key : keys)
		{
			PreparedData data = hTable.get(key);
			data.close();
		}
	}
	
	public boolean checkDBPartGroups(String dbPartitionGroupName)
	{
		boolean retValue;
		PreparedData data = new PreparedData(connection, "select 'X' from SYSCAT.DBPARTITIONGROUPDEF where DBPGNAME = ? OR DBPGNAME = ?");		
		retValue = data.doesExist(dbPartitionGroupName, dbPartitionGroupName);
		data.close();
		return retValue;
	}

	public boolean checkBufferPools(String bpname)
	{
		boolean retValue;
		PreparedData data = new PreparedData(connection, "select 'X' from SYSCAT.DBPARTITIONGROUPDEF where DBPGNAME = ? OR DBPGNAME = ?");		
		retValue = data.doesExist(bpname, bpname);
		data.close();
		return retValue;
	}
}
