package com.ibm.migr.data;

import static com.ibm.migr.utils.Log.*;
import java.util.ArrayList;
import com.ibm.migr.db2.BufferpoolData;
import com.ibm.migr.db2.DBGroupData;
import com.ibm.migr.db2.Db2lookInfoData;
import com.ibm.migr.db2.SchemaData;
import com.ibm.migr.db2.db2;
import com.ibm.migr.utils.Args;
import com.ibm.migr.utils.U;

public class GenInitialObjects
{
	private long mainStart = System.currentTimeMillis();
	private db2 srcConn, dstConn = null;
	private SQLQueries p, q = null;
	
	public GenInitialObjects()
	{
		srcConn = new db2("SRCDB");
		p = new SQLQueries(srcConn);
		if (U.checkDestinationObjects)
		{
			dstConn = new db2("DSTDB");
			q = new SQLQueries(dstConn);
		}
	}
	
	public void t()
	{
		p.getTableInfo();
		U.WriteFile(false, U.tableInfoFileName, p.printAllTableInfo());
	}
	
	public void run()
	{
		String sql;
		// Get all partition Groups
		ArrayList<DBGroupData> partGroups = p.getDBPartGroups();
		// Validate with source
		if (U.checkDestinationObjects)
		   partGroups = q.validateDBPartGroups(partGroups);
		// Generate DDL
		sql = p.genDBPartGroups(partGroups);
		U.WriteFile(false, "01dbpgbpts.sql", sql);
		// Generate DDL for storage groups
		sql = p.genStorageGroups();
		U.WriteFile(true, U.dbpgbptsFileName, sql);
		// Get all bufferpools
		ArrayList<BufferpoolData> bufPools = p.getBufferpools();
		// Validate with source
		if (U.checkDestinationObjects)
		   bufPools = q.validateBufferpools(bufPools);
		// Generate DDL
		sql = p.genBufferpools(bufPools);
		U.WriteFile(true, U.dbpgbptsFileName, sql);
		// Generate DDL for temporary tablespace
		sql = p.genTempTableSpace();
		U.WriteFile(true, U.dbpgbptsFileName, sql);
		// Populate Table size Data from source
		p.getTableInfo();
		// Print all table info for reference
		U.WriteFile(false, U.tableInfoFileName, p.printAllTableInfo());
		// Get All generated table spaces
		U.WriteFile(false, U.tablespacesFileName, p.getAllGeneratedTablespacesDefn());		
		// Generate global variables
		U.WriteFile(false, U.globalVariableFileName, p.genGlobalVariables());
		// Get all schema		
		U.WriteFile(false, U.schemasFileName, p.genSchema()); 
		// Get all source schema list
		for (SchemaData schemaData : p.getSchemas())
		{
			if (schemaData.selected)
			{
				// Populate systools.db2look_info and get the token
				int token = p.genLook(schemaData.schema);
				// get Db2lookInfoData
				ArrayList<Db2lookInfoData> lookData = p.getdb2lookInfo(token, schemaData);
				// get Db2lookInfoData validated against target
				if (U.checkDestinationObjects)
					lookData = q.db2lookValidation(lookData);
				// Generate DDL for all objects in schema
				p.genObjectsDDLs(token, schemaData, lookData);
			}
		}
		// Runstats based upon profile which was set in the source
		p.genRunstats(U.runstatsFileName);
		// Generate auths
		U.WriteFile(false, U.authsFileName, p.genAuths());
		// Generate the script to run all generated DDLs
		p.createShellScript();
		info("End to end Elapsed Time   = " + U.getElapsedTime(mainStart));
		close();
	}
	
	private void close()
	{
		srcConn.close();
	}
	
	public static void main(String[] args)
	{
		String myClass = new Throwable().getStackTrace()[0].getClassName();
		if (args.length < 2)
		{
			info("***********************************************************************************************");
			info("Usage: java -Xmx4096m " + myClass
					+ " -DSRCDB=./srcConn.properties <NameofHPUOutputDir> false");
			info("Usage: java -Xmx4096m " + myClass
					+ " -DSRCDB=./srcConn.properties -DDSTDB=./dstConn.properties <NameofHPUOutputDir> true");

			info("***********************************************************************************************");
			info("Get All Database Partitions Groups");
			info("Use switch -Xmx4096m - Amount of the maximum memory for the program");
			info("Name of the program = " + myClass);
			info("Use switch -DSRCDB to specify name of the source db properties file");
			info("Param 1 - Specify name of the output directory to store generated scripts");
			info("Param 2 - True/False - Check if object exists on target or not");
			info("If Param 2 is true - You must specify -DDSTDB for destination db properties file");
			info("Manually Modify the scripts for the target database to fit to the new MLN topology");
			System.exit(-1);
		}
		Args a = new Args(args);
		U.outputDataDirectory = a.targets()[0];
		U.checkDestinationObjects = Boolean.valueOf(a.targets()[1]);
		if (a.switchPresent("-d"))
			set(LEVEL_DEBUG);
		String dbProp = System.getProperty("SRCDB");
		
		if (dbProp == null || dbProp.length() == 0)
		{
			error("-DSRCDB switch is not specified for the source db properties file");
			System.exit(-1);
		}
		
		if (U.checkDestinationObjects)
		{
			dbProp = System.getProperty("DSTDB");
			if (dbProp == null || dbProp.length() == 0)
			{
				error("-DDSTDB switch is not specified for the destination db properties file");
				System.exit(-1);
			}
		}
		
		info("*****************************************************************");
		info("*****            Get Database Scripts from source           *****");
		info("*****************************************************************");
		info("Runtime initiated with the following parameters:");
		info("Data output directory: " + U.outputDataDirectory);
		info("*****************************************************************");
		info("Extracting DDL Program Started....");
		
		GenInitialObjects drv = new GenInitialObjects();
		drv.run();
		//drv.t();
	}
}
