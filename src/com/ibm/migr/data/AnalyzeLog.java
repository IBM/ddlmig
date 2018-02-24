package com.ibm.migr.data;

import static com.ibm.migr.utils.Log.*;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.migr.utils.Args;
import com.ibm.migr.utils.U;

public class AnalyzeLog
{
	private Map<String, Integer> sqlTokenMap = new HashMap<String, Integer>();
	public AnalyzeLog()
	{
		
	}
	
	private void log(String s)
	{
		System.out.println(s);
	}
	
	public void run()
	{		
		String buffer = U.ReadFile(U.ddlLogFileName);
		Pattern p = Pattern.compile("(?mi)^(?!SQLSTATE)(SQL\\w+)\\s+.*$");
        Matcher  m = p.matcher(buffer);
        while (m.find())
        {
        	String key = m.group(1);
        	if (sqlTokenMap.containsKey(key))
        	{
        		sqlTokenMap.put(key, sqlTokenMap.get(key)+1);
        	} else
        	{
        		sqlTokenMap.put(key, 1);
        	}
        }
        log("==================================================================");
        for (Map.Entry<String, Integer> entry : sqlTokenMap.entrySet()) 
        {
        	log(String.format("Token = %10s Count = %4d", entry.getKey(), entry.getValue()));
        }   
        log("==================================================================");
        for (String key :  sqlTokenMap.keySet())
        {
        	p = Pattern.compile("(?mi)^"+key+".*$");
        	m = p.matcher(buffer);
        	log("==================================================================");
        	while (m.find())
        		log(m.group());
        }
	}
	
	public static void main(String[] args)
	{
		String myClass = new Throwable().getStackTrace()[0].getClassName();
		if (args.length < 1)
		{
			info("***********************************************************************************************");
			info("Usage: java " + myClass + " <db2ddllogfiletoanalyze>");
			info("***********************************************************************************************");
			System.exit(-1);
		}
		Args a = new Args(args);
		U.ddlLogFileName = a.targets()[0];
		if (a.switchPresent("-d"))
			set(LEVEL_DEBUG);
		AnalyzeLog al = new AnalyzeLog();
		al.run();
	}
}
