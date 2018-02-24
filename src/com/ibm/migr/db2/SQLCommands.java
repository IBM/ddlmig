package com.ibm.migr.db2;

import static com.ibm.migr.utils.Log.*;
import java.io.BufferedWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

import com.ibm.migr.utils.U;

public class SQLCommands
{
	private HashMap<Integer, UOWData> uowMap;
	private boolean retry = U.retry;
	private String linesep = U.linesep, colsep = U.dataDelimitor;
	private Connection connection = null;
	private BufferedWriter fp = null;
    
	private int size;
	private int[] numVars, offsetVars;
	private String[] varArgs;
	private String tname = "";
	
	private List<PreparedStatement[]> pstmt;
	private List<CallableStatement[]> cstmt;
	
	public SQLCommands(HashMap<Integer, UOWData> uowMap, 
			BufferedWriter fp, int runid, Connection connection)
	{
		this.uowMap = uowMap;
		this.uowMap = U.getUOWMap();
		this.fp = fp;
		this.tname = "thread_" + U.runName(runid);
		this.connection = connection;
		size = uowMap.size();
		pstmt = new ArrayList<PreparedStatement[]>(size);
		cstmt = new ArrayList<CallableStatement[]>(size);
		for (int i = 0; i < size; ++i)
		{
			pstmt.add(i, new PreparedStatement[uowMap.get(i).size()]); 
			cstmt.add(i, new CallableStatement[uowMap.get(i).size()]); 
		}
	}
	
	private void setSQLType(int id, int jd, String type)
	{
		UOWData uow = uowMap.get(id);
		uow.setSQLType(jd, type);
	}
		
	private String getSQLType(int id, int jd)
	{
		return (uowMap == null) ? "" : ((UOWData) uowMap.get(id)).getSQLType(jd);
	}
		
	public void commit()
	{
		if (connection != null)
		{
			try
			{
				if (!connection.isClosed())
				  connection.commit();
			} catch (SQLException e)
			{
				e.printStackTrace();
			}			
		}
	}		
		
	public String getParams(int idx)
	{
		String value = "";
		for (int j = 1; j <= numVars[idx]; ++j)
		{
			value += "," + varArgs[offsetVars[idx]+j];
		}
        return value;
	}	

	public void prepare()
	{
		PreparedStatement[] ps;
		CallableStatement[] cs;
		UOWData uow;

		String sql;
		if (size == 0) return;
		if (connection == null) return;
		for (int i = 0; i < size; ++i)
		{
			uow = uowMap.get(i);
			ps = pstmt.get(i);
			cs = cstmt.get(i);
			for (int j= 0; j < uow.size(); ++j)
			{
				sql = uow.getSQL(j);
				if (Pattern.matches("^\\s*$", sql)) 
			    {
			       continue;
			    }
				try
				{
					if (Pattern.matches("^\\s*values.*", sql.toLowerCase()))
			        {
					   ps[j] = connection.prepareStatement(sql);
					   ps[j].setFetchSize(U.fetchSize);
					   setSQLType(i, j, "VALUES");
			        } else if (Pattern.matches("^\\s*select.*", sql.toLowerCase()))
			        {		        	
			        	ps[j] = connection.prepareStatement(sql);
						ps[j].setFetchSize(U.fetchSize);
						setSQLType(i, j, "SELECT");		        	
			        }
					else if(Pattern.matches("^\\s*(insert|update|delete).*", sql.toLowerCase()))                     
			        {
						setSQLType(i, j, "IUD");
						ps[j] = connection.prepareStatement(sql);
			        } else if(Pattern.matches("^\\s*(call).*", sql.toLowerCase())) 
			        {
			        	setSQLType(i, j, "CALL");
			        	cs[j] = connection.prepareCall(sql);
			        } else
			        {
			        	setSQLType(i, j, "OTHER");
			        	ps[j] = connection.prepareStatement(sql);
			        }
				} catch (SQLException e)
				{				
					e.printStackTrace();
				}
			}
			pstmt.set(i, ps);
			cstmt.set(i, cs);
		}
	}
	
	public String toString(int index, int jd)
	{
		UOWData uow;
		uow = uowMap.get(index);
		return uow.getSQL(jd);
	}

	
	/**
	 * Not tested - Needs to be done
	 * @param index
	 * @param jd
	 * @param params
	 */
	public void bind(int index, int jd, String params)
	{
		PreparedStatement ps;
		CallableStatement cs;
		UOWData uow = uowMap.get(index);
		String value, sqlType;
		String[] varArgs = params.split(",",-1);
		for (int j = 0; j < uow.size(); ++j)
		{
			ps = pstmt.get(index)[j];
    		cs = cstmt.get(index)[j];
    		for (int k = 1; k <= varArgs.length; ++k)
    		{
    			value = varArgs[k-1];
        		sqlType = getSQLType(index, j);
    			debug("Parameter marker Slot = " + k + " value = " + value);
        		try
    			{
    	        	if (sqlType.equals("CALL"))
    	        	{
    					cs.setString(k, value);
    	        	}
    	        	else 
    	        	{
    					ps.setString(k, value);
    	        	}
    			} catch (SQLException e)
    			{
    				e.printStackTrace();
    			}        				
    		}
		}
	}
	
	/**
	 * Not tested
	 */
	public void bind()
	{
		UOWData uow;
		PreparedStatement ps;
		CallableStatement cs;
		String value, sqlType;
		int offset = 0;
        for (int i = 0; i < size; ++i)
        {
        	uow = uowMap.get(i);
        	for (int j = 0; j < uow.size(); ++j)
        	{
    			ps = pstmt.get(i)[j];
        		cs = cstmt.get(i)[j];
        		sqlType = getSQLType(i, j);
        		if (numVars[i] > 0)
        		{
        			for (int k = 1; k <= numVars[i]; ++k)
        			{
        				value = varArgs[k-1+offset];
        				debug("Parameter marker Slot = " + k + " value = " + value);
                		try
    					{
            	        	if (sqlType.equals("CALL"))
            	        	{
    							cs.setString(k, value);
            	        	}
            	        	else 
            	        	{
    							ps.setString(k, value);
            	        	}
    					} catch (SQLException e)
    					{
    						e.printStackTrace();
    					}        				
        			}
        			offset += numVars[i];
    	        }
        	}
        }
	}	

	public void close()
	{
		UOWData uow;
		PreparedStatement ps;
		CallableStatement cs;

		for (int i = 0; i < size; ++i)
		{
			uow = uowMap.get(i);
			for (int j= 0; j < uow.size(); ++j)
			{
    			ps = pstmt.get(i)[j];
        		cs = cstmt.get(i)[j];
	    		try
	    		{
	    		if (ps != null)
	    			ps.close();
	    		if (cs != null)
	    			cs.close();
	    		} catch (Exception ex)
	    		{
	    			ex.printStackTrace();
	    		}				
			}
		}
	}
	
    private String getColumnValue(ResultSet rs, ResultSetMetaData metaData, int colIndex) throws Exception
    {
        String tmp = "";
        try
        {
            tmp = rs.getString(colIndex);                         
        } catch (SQLException e)
        {
           error("Col[" + colIndex + "] Error:" + e.getMessage());
           return "SkipThisRow";               
        }
        return tmp;
    }
    
    private long getResultSet(PreparedStatement queryStatement, ResultSet Reader) throws Exception
    {
       ResultSetMetaData rsmtadta = null;
       StringBuffer buffer = new StringBuffer();  
       int colCount;
       boolean skip = false;
       String colValue = "";
       long numRows = 0L;
       
       if (Reader == null) return 0L;
       rsmtadta = queryStatement.getMetaData(); 
       if (rsmtadta == null)
          rsmtadta = Reader.getMetaData();
       colCount = rsmtadta.getColumnCount();
       while (Reader.next()) 
       {
    	   if (U.saveQueryResults)
    	   {
	          buffer.setLength(0);
	          skip = false;
	          for( int j = 1; j <= colCount; j++ ) 
	          {
	             colValue = getColumnValue(Reader, rsmtadta, j);
	             buffer.append(colValue == null ? "" : colValue);
	             if (colValue != null && colValue.equals("SkipThisRow"))
	             {
	                 skip = true;
	                 continue;
	             }
	             if (j != colCount)
	                 buffer.append(colsep);                           
	          }
	          buffer.append(linesep);
	          if (!skip)
	             debug(buffer.toString());
    	   }
           numRows++;
       }
       if (U.saveQueryResults)
       {
    	   fp.write(buffer.toString());
       }
       return numRows;
    }
    
    public void db2batch(int index)
    {
		PreparedStatement ps;
		CallableStatement cs;

    	UOWData uow = uowMap.get(index);
    	SQLData sqlData;
    	String type;
    	
    	for (int j = 0; j < uow.size(); ++j)
    	{
    		sqlData = uow.getSQLData(j);
    		type = uow.getSQLType(j);
    		ps = pstmt.get(index)[j];
    		cs = cstmt.get(index)[j];
    		db2batch(sqlData, type, ps, cs);
    	}
    }
    
    private void db2batch(SQLData data, String type, PreparedStatement ps, CallableStatement cs)
    {
       String sqlerrmc, sql = data.getSQL();
       boolean doRetry;
       int retryCount = 0;

       long now, first;
       
       ResultSet Reader = null;
        
       if (!U.appDelay.equalsIgnoreCase("0"))
	   {
		   debug("Delaying app by " + U.appDelay);
		   try { Thread.sleep(Long.valueOf(U.appDelay)); } 
		   catch (NumberFormatException e) { } 
		   catch (InterruptedException e)  { }
	   }
       if (Pattern.matches("^\\s*$", sql)) 
       {
          return;
       }
       
       first = System.currentTimeMillis();
       
       try
       {
          //if (debug) log("Starting to execute: \""+sql+"\""+U.linesep); 
          if (type.equals("SELECT") || type.equals("VALUES"))
          {
              Reader = ps.executeQuery();
              long rows = getResultSet(ps, Reader);
              data.setRows(rows);
          } else if(type.equals("IUD"))                     
          {
        	  do
        	  {
            	  doRetry = false;
            	  try
            	  {
                      int rows = ps.executeUpdate();
                      data.setRows(rows);
                      //if (debug) log(rows + " rows affected \"" + sql + "\"" + U.linesep);
            	  } catch (SQLException ex)
            	  {
            		  if (ex.getErrorCode() == -911)
            		  {
            			  data.deadlock();
            			  if (ex instanceof com.ibm.db2.jcc.DB2Diagnosable)
            			  {
        					  com.ibm.db2.jcc.DB2Sqlca sqlca = ((com.ibm.db2.jcc.DB2Diagnosable)ex).getSqlca();
        					  if (sqlca != null)
        					  {
        						 sqlerrmc = sqlca.getSqlErrmc();	
        						 if (sqlerrmc != null && sqlerrmc.trim().equals("2") && retry)
        						 {
        					 		info("Retrying ("+retryCount+") -911 RC=2 for " + sql);
        					 		++retryCount;
        					 		doRetry = (retryCount > 10) ? false : true;
        					 	 } else
        					 	 {
         							 doRetry = false;
         							 info("Deadlock for " + sql);
        					 		 //ex.printStackTrace();
        					 	 }
        					  }
            			  }
            		  } else if (ex.getErrorCode() == -955)
            		  {
            			  data.sortheap();
            			  if (ex instanceof com.ibm.db2.jcc.DB2Diagnosable)
            			  {
        					  com.ibm.db2.jcc.DB2Sqlca sqlca = ((com.ibm.db2.jcc.DB2Diagnosable)ex).getSqlca();
        					  if (sqlca != null)
        					  {
        						 sqlerrmc = sqlca.getSqlErrmc();	
        						 if (sqlerrmc != null && sqlerrmc.trim().equals("3") && retry)
        						 {
        					 		info("Retrying ("+retryCount+") -955 RC=3 for " + sql);
        					 		++retryCount;
        							doRetry = (retryCount > 10) ? false : true;
        					 	 } else
        					 	 {
         							 doRetry = false;
         							 info("Sortheap Error -955 RC=3 for " + sql);
        					 		 //ex.printStackTrace();
        					 	 }
        					  }
            			  }
            		  }
            		  else if (ex.getErrorCode() == -30108)
            		  {
            			  ;
            		  }
            		  else if (ex.getErrorCode() == -803)
            		  {
            			  data.uniqueViolations();
            		  }
            		  else
            		  {
						 doRetry = false;
				 		 ex.printStackTrace();	                			  
            		  }
            	  }
        	  } while (doRetry);
          } else if(type.equals("CALL")) 
          {
        	  do
        	  {
            	  doRetry = false;
            	  try
            	  {
                      cs.execute();
                      Reader = cs.getResultSet();
                      getResultSet(cs, Reader);
                      while (cs.getMoreResults())
                      {
                         Reader = cs.getResultSet();
                         getResultSet(cs, Reader);
                      }
            	  } catch (SQLException ex)
            	  {
            		  if (ex.getErrorCode() == -2310)
            		  {
            			  if (ex instanceof com.ibm.db2.jcc.DB2Diagnosable)
            			  {
        					  com.ibm.db2.jcc.DB2Sqlca sqlca = ((com.ibm.db2.jcc.DB2Diagnosable)ex).getSqlca();
        					  if (sqlca != null)
        					  {
        						 sqlerrmc = sqlca.getSqlErrmc();	
        						 if (sqlerrmc != null && sqlerrmc.trim().equals("-911") && retry)
        						 {
        							data.deadlock();
        					 		info("Retrying -2310 and -911 error for " + sql);
        							doRetry = true;
        					 	 } else
        					 	 {
         							 doRetry = false;
        					 		 ex.printStackTrace();
        					 	 }
        					  }
            			  }
            		  } else if (ex.getErrorCode() == -30108)
            		  {
            			  
            		  } else
            		  {
						 doRetry = false;
				 		 ex.printStackTrace();	                			  
            		  }
            	  }
            	  Thread.sleep(50);
        	  } while (doRetry);
          } else
          {
             ps.executeUpdate();                     
          }
          now = System.currentTimeMillis();    
          String logValue = U.getElapsedTime(first) + " for \"" + U.truncStr(sql,60,"...") + "\"";
          info(logValue);
          data.setElapsedTime((now - first)/1000.0F);
          first = now;
          if (U.autoCommit)
             connection.commit();
       } 
       catch (SQLException qex)
       {
    	   if (qex.getErrorCode() == -30108)
    	   {
    		   ;
    	   } else if (qex.getErrorCode() == -911)
    	   {
    		   data.deadlock();
    	   }
    	   else
    	   {
	           info("exception executing = " + sql + " Error Code= " + qex.getErrorCode() + " Message=" + qex.getMessage());
	           qex.printStackTrace();
	           try
	           {
	        	   connection.rollback();
	           } catch (SQLException e)
	           {
	               e.printStackTrace();
	           }
    	   }
       }
       catch (Exception ex)
       {
           info("SQL=" + sql);
           error("exception executing SQL = " + ex.getMessage());
           ex.printStackTrace();
           try
           {
        	   connection.rollback();
           } catch (SQLException e)
           {
               error("rollback", e);
           }
       }
       finally
       {
          if (Reader != null)
          {    
              try
              {
                  Reader.close();
              } catch (SQLException e)
              {
                  error("Reader.close() error " + e.getMessage());
              }
          }
       }
	}
}