/**
 * Author Vikram Khatri
 */
package com.ibm.migr.data;

import static com.ibm.migr.utils.Log.*;
import java.util.Collections;
import java.util.List;
import com.ibm.migr.db2.TableMapping;
import com.ibm.migr.db2.db2;
import com.ibm.migr.utils.U;

public class GenHPUScripts
{
	private long mainStart = System.currentTimeMillis();
	private db2 srcConn;
	private db2 dstConn;
	private List<TableMapping> listSrc, listDst;
	private SQLQueries p, q;
	private String srcdbName, dstdbName, instanceName, serverName, linesep = U.linesep;
	private int dataBuffer, numCPU;
	
	public GenHPUScripts()
	{
		open();
	}
	
	private void open()
	{
		init();
	}
	
	private String getMessageFileName(String schema, String table)
	{
		String sep, dir = U.messageDirectory;
		sep = (U.messageDirectory.contains("/")) ? "/" : U.filesep;
		return dir + sep + schema.toLowerCase() + "_" + table.toLowerCase() + ".msg";
	}
	
	private String getColdHPUCtl(String schema, String table, String srcPartitions,
			String dstPartitions, boolean dstStatsProfile,
			int dataBuffer, int numCPU)
	{
		String tab = schema + "." + table;
		StringBuffer ctl = new StringBuffer();
		ctl.append("# Control file for HPU generated at " + U.getTime()+linesep);
		ctl.append("# Control file for " + tab + linesep);
		ctl.append("GLOBAL CONNECT TO " + srcdbName+linesep);
		ctl.append("UMASK \"022\""+linesep);
		ctl.append("MIGRATE TABLESPACE"+linesep);
		ctl.append("PART ("+srcPartitions+")"+linesep);
		ctl.append("DB2 NO"+linesep);
		ctl.append("LOCK NO"+linesep);
		ctl.append("QUIESCE NO"+linesep);
		ctl.append("UNICODE"+linesep);
		ctl.append("TARGET ENVIRONMENT(INSTANCE \""+instanceName+"\" ON \""+serverName+"\" IN "+dstdbName+" REPART PMAP_32K)"+linesep);
		ctl.append("WORKING IN (\""+U.workingDirectory+"\")"+linesep);
		ctl.append("SELECT * FROM "+ tab + linesep);
		ctl.append(";"+linesep);
		ctl.append("LOADMODE REPLACE"+linesep);
		ctl.append("LOADOPT (MESSAGES \""+getMessageFileName(schema, table)+"\","+linesep);
		if (dstStatsProfile)
		ctl.append("         STATISTICS USE PROFILE,"+linesep);
		ctl.append("         LOCK WITH FORCE,"+linesep);
		ctl.append("         DATA BUFFER "+dataBuffer+","+linesep);
		ctl.append("         CPU_PARALLELISM "+numCPU+")"+linesep);
		ctl.append("TARGET KEYS (CURRENT PARTS("+dstPartitions+"))"+linesep);
		ctl.append("FORMAT DELIMITED"+linesep);
		ctl.append("INTO "+tab+linesep);
		ctl.append(";"+linesep);		
		return ctl.toString();
	}
	
	private boolean doWeHaveStatsProfile(String schema, String table)
	{
		for (TableMapping data : listDst)
		{
			if (data.tabschema.equals(schema) && data.tabname.equals(table))
			{
				if (data.statisticalProfile == null || 
						data.statisticalProfile.length() == 0)
					return false;
				else 
				{
					if (data.statisticalProfile.trim().length() == 0)
						return false;
					else
						return true;
				}
			}
		}
		return false;
	}
	
	private void genHPUCtlFiles()
	{
		int counter = 1;
		String ctlStr, fileName;
		boolean dstStatsProfile; 
		for (TableMapping data : listSrc)
		{
			dstStatsProfile = doWeHaveStatsProfile(data.tabschema, data.tabname);
			ctlStr = getColdHPUCtl(data.tabschema, data.tabname, 
					data.dbpgData.srcPartitions,
					data.dbpgData.dstPartitions, dstStatsProfile,
					dataBuffer, numCPU);
			fileName = String.format("%05d_%s_%s.ctl", counter, data.tabschema, data.tabname);
			debug(ctlStr);
			U.truncateFile(fileName);
			U.WriteFile(fileName, ctlStr);
			++counter;
		}
		info("=====================================================");		
	}
	
	public void init()
	{
		srcConn = new db2("SRCDB");
		dstConn = new db2("DSTDB");		
		p = new SQLQueries(srcConn);
		q = new SQLQueries(dstConn);
		srcdbName = srcConn.getDBName();
		dstdbName = dstConn.getDBName();
		instanceName = dstConn.getInstanceName();
		serverName = dstConn.getServerName();
		dataBuffer = dstConn.getDataBuffer();
		numCPU = dstConn.getNumCPU();
		listSrc = p.getTableInfo();
		listDst = q.getTableInfo();
		Collections.sort(listSrc);
		Collections.sort(listDst);
	}
	
	public void run()
	{
		genHPUCtlFiles();
		close();
		info("End to end Elapsed Time   = " + U.getElapsedTime(mainStart));
	}
	
	private void close()
	{
		srcConn.close();
		dstConn.close();
	}
	
	public static void main(String[] args)
	{
		String myClass = new Throwable().getStackTrace()[0].getClassName();
		if (args.length < 3)
		{
			info("***********************************************************************************************");
			info("usage: java -Xmx4096m "
					+ myClass
					+ " -DSRCDB=./srcConn.properties -DDSTDB=./dstConn.properties "
					+ "<NameofHPUOutputDir> <PipeWorkingDir> <MessageDir>");

			info("***********************************************************************************************");
			info("IBM HPU Control File Generation Program");
			info("Use switch -Xmx4096m - Amount of the maximum memory for the program");
			info("Name of the program = " + myClass);
			info("Use switch -DSRCDB to specify name of the source db properties file");
			info("Use switch -DSRCDB to specify name of the destination db properties file");
			info("Param 1 - Specify name of the output directory to store HPU control files");
			info("Param 2 - Specify name of the working directory on target server to create HPU pipes for migration");
			info("Param 3 - Specify name of the message directory on target server for LOAD to create messages");
			info("working and message directory must be a shared file system for all hosts on target");
			System.exit(-1);
		}

		U.outputDataDirectory = args[0];
		U.workingDirectory = args[1];
		U.messageDirectory = args[2];
		String srcDB = System.getProperty("SRCDB"), dstDB = System.getProperty("DSTDB");
		
		if (srcDB == null || srcDB.length() == 0)
		{
			error("-DSRCDB switch is not specified for the source db properties file");
			System.exit(-1);
		}
		if (dstDB == null || dstDB.length() == 0)
		{
			error("-DDSTDB switch is not specified for the destination db properties file");
			System.exit(-1);
		}

		info("*****************************************************************");
		info("*****       IBM HPU Control File Generation Program         *****");
		info("*****************************************************************");
		info("Runtime initiated with the following parameters:");
		info("HPU Scripts output directory : " + U.outputDataDirectory);
		info("Working directory for pipes  : " + U.workingDirectory);
		info("Load Message directory       : " + U.messageDirectory);
		info("*****************************************************************");
		info("HPU Control File Generation Program Started....");
		
		GenHPUScripts drv = new GenHPUScripts();
		drv.run();
	}
}
