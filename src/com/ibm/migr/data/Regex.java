package com.ibm.migr.data;

import static com.ibm.migr.utils.Log.*;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ibm.migr.db2.TSData;
import com.ibm.migr.db2.TablePartitionData;
import com.ibm.migr.db2.TableMapping;
import com.ibm.migr.utils.U;

public class Regex
{
	public static String fileName;
	public String buffer;
	private int countTablespaces = 0, countNonPartIndexTablespaces = 0;
	
	public Regex()
	{
		;
	}
	
	private void init()
	{
		try
		{
			StringBuffer buf = new StringBuffer();
			String s;
			BufferedReader fin = new BufferedReader(new InputStreamReader(new FileInputStream(fileName),"UTF-8"));
			while ((s=fin.readLine())!=null) 
			{
				buf.append(s).append(U.linesep);
            }
			fin.close();
			buffer = buf.toString();
		} catch (Exception e)
		{
			e.printStackTrace();
			System.exit(-1);
		}
	}
	
	String modifyIndexDDL(TableMapping data, String schemaUnderMigration, String buffer)
	{
		String regex = "(?mi)(\\bIN\\b\\s+\"\\w+\")|(\\bCOMPRESS\\b\\s+\\bNO\\b)|(\\bCREATE\\b\\s+(\\bUNIQUE\\b\\s+)?\\bINDEX\\b.*)|(\\bCOMMENT\\b\\s+(\\bON\\b\\s+)?\\bINDEX\\b.*)";
		String regexCrTab = "(?mi)(?<=\")?(\\w+)(?=\")?";
		 
		StringBuffer newBuffer = new StringBuffer();
		Pattern p = Pattern.compile(regex);
		Pattern p2 = Pattern.compile(regexCrTab);
		String ts = "\"TS ERROR IN SETTING\"";
		String schema, index, matchedStr;
		ArrayList<String> array = new ArrayList<String>();

		Matcher m2, m = p.matcher(buffer);
		m = p.matcher(buffer);
		while (m.find())
		{
			matchedStr = m.group();
			for (int i = 0; i <= m.groupCount(); ++i)
			{
				debug(String.format("Group [%d] : %s", i, m.group(i)));
			}
			if (matchedStr.matches("(?mis)^CREATE.*"))
			{
				array.clear();
				m2 = p2.matcher(matchedStr);
				while (m2.find())
				{
					if (m2.group().equalsIgnoreCase("create")) continue;
					if (m2.group().equalsIgnoreCase("unique")) continue;
					if (m2.group().equalsIgnoreCase("index")) continue;					
					if (m2.group().equalsIgnoreCase("on")) continue;					
				    array.add(m2.group(0).replaceAll("\"", "").trim().toUpperCase());
				}
				schema = array.get(0);
				if (!schema.equalsIgnoreCase(schemaUnderMigration))
				{
					m2 = p2.matcher(matchedStr);
					while (m2.find())
					{
						if (m2.group().equalsIgnoreCase("create")) continue;
						if (m2.group().equalsIgnoreCase("unique")) continue;
						if (m2.group().equalsIgnoreCase("index")) continue;					
						StringBuffer sb = new StringBuffer();
						m2.appendReplacement(sb, Matcher.quoteReplacement(schemaUnderMigration));
						m2.appendTail(sb);
						m.appendReplacement(newBuffer, sb.toString());
						break;
					}
					schema = schemaUnderMigration;
				}
				index = array.get(1);
				ts = " IN \"" + data.getNonPartitionedIndexTableSpacesName(index) + "\"";
				debug("schema = " + schema + " ts = " + ts);				
			} else if (matchedStr.matches("(?mis)^COMMENT.*"))
			{
				array.clear();
				m2 = p2.matcher(matchedStr);
				while (m2.find())
				{
					if (m2.group().equalsIgnoreCase("comment")) continue;
					if (m2.group().equalsIgnoreCase("on")) continue;
					if (m2.group().equalsIgnoreCase("index")) continue;					
				    array.add(m2.group(0).replaceAll("\"", "").trim().toUpperCase());
				}
				schema = array.get(0);
				if (!schema.equalsIgnoreCase(schemaUnderMigration))
				{
					m2 = p2.matcher(matchedStr);
					while (m2.find())
					{
						if (m2.group().equalsIgnoreCase("comment")) continue;
						if (m2.group().equalsIgnoreCase("on")) continue;
						if (m2.group().equalsIgnoreCase("index")) continue;					
						StringBuffer sb = new StringBuffer();
						m2.appendReplacement(sb, Matcher.quoteReplacement(schemaUnderMigration));
						m2.appendTail(sb);
						m.appendReplacement(newBuffer, sb.toString());
						break;
					}
					schema = schemaUnderMigration;
				}				
			} else if (matchedStr.matches("(?i)^\\bIN\\b.*"))
			{
				countNonPartIndexTablespaces++;
				m.appendReplacement(newBuffer, Matcher.quoteReplacement(ts));
				ts = "\"TS ERROR IN SETTING\"";
			} else if (matchedStr.matches("(?i)\\bCOMPRESS\\b\\s+\\bNO\\b"))
			{
				m.appendReplacement(newBuffer, "COMPRESS YES");
			}
		}
		m.appendTail(newBuffer);		
		debug("Modified SQL Begins ===============");
		debug(newBuffer.toString());
		debug("Modified SQL Ends   ===============");
		return newBuffer.toString();
	}
	
	String modifyMQTDDL(TableMapping data, String buffer)
	{
		String regex = "(?mis)(\\bIN\\b\\s+\"\\w+\")|(\\bINDEX\\b\\s+\\bIN\\b\\s+\"\\w+\")|(\\bCREATE\\b\\s+(\\bSUMMARY\\b\\s+)?\\bTABLE\\b.*?)(?=\\bAS\\b\\s*)?(?=\\()";
		String regexCrTab = "(?mis)(?<=\")?(\\w+)(?=\")?";
		String refresh = "(?mi)(\\bREFRESH\\b\\s+\\bIMMEDIATE\\b)";
		buffer = buffer.replaceFirst(refresh, "REFRESH DEFERRED");		 
		StringBuffer newBuffer = new StringBuffer();
		Pattern p = Pattern.compile(regex);
		Pattern p2 = Pattern.compile(regexCrTab);
		String ts = "\"TS ERROR IN SETTING\"";
		String schema, table, matchedStr, integrityStr = null;
		ArrayList<String> array = new ArrayList<String>();

		Matcher m2, m = p.matcher(buffer);
		m = p.matcher(buffer);
		while (m.find())
		{
			matchedStr = m.group();
			for (int i = 0; i <= m.groupCount(); ++i)
			{
				debug(String.format("Group [%d] : %s", i, m.group(i)));
			}
			if (matchedStr.matches("(?mis)^CREATE.*"))
			{
				array.clear();
				m2 = p2.matcher(matchedStr);
				while (m2.find())
				{
					if (m2.group().equalsIgnoreCase("create")) continue;
					if (m2.group().equalsIgnoreCase("summary")) continue;					
					if (m2.group().equalsIgnoreCase("table")) continue;					
				    array.add(m2.group(0).replaceAll("\"", "").trim().toUpperCase());
				}
				schema = array.get(0);
				table = array.get(1);
				ts = " IN \"" + data.getDataTableSpaceName("") + "\"" + " INDEX IN " 
					     +"\"" + data.getIndexTableSpaceName("") + "\"";
				integrityStr = "SET INTEGRITY FOR \""+schema+"\".\""+table+"\" IMMEDIATE CHECKED" + U.sqlTerminator + U.linesep;
				debug("tablespace clause = " + ts);				
			} else if (matchedStr.matches("(?i)^\\bIN\\b.*"))
			{
				countTablespaces++;
				m.appendReplacement(newBuffer, Matcher.quoteReplacement(ts));
				ts = "\"TS ERROR IN SETTING\"";
			} else if (matchedStr.matches("(?i)^\\bINDEX\\b\\s+\\bIN\\b.*"))
			{
				m.appendReplacement(newBuffer, "");
			}
		}		
		m.appendTail(newBuffer);
		if (integrityStr != null) newBuffer.append(integrityStr);
		debug("Modified SQL Begins ===============");
		debug(newBuffer.toString());
		debug("Modified SQL Ends   ===============");
		return newBuffer.toString();
	}
	
	String modifyTableDDL(TableMapping data, String buffer)
	{
		String regex = "(?mi)(\\bIN\\b\\s+\"\\w+\"|\\bINDEX\\b\\s+\\bIN\\b\\s+\"\\w+\"|\\bPART\\b\\s+\"\\w+\".*(?=[,|\"\\)])|\\bCREATE\\b\\s+\\bTABLE\\b.*(?=\\())";
		String regexPart = "(?mi)(PART\\s+)\"(\\w+)\"(.*?)(?=\\bIN\\b)";
		 
		StringBuffer newBuffer = new StringBuffer();
		Pattern p = Pattern.compile(regex);
		Pattern p2 = Pattern.compile(regexPart);
		String ts = "TS ERROR IN SETTING", part = "PART ERROR IN SETTING";
		String matchedStr;
		//ArrayList<String> array = new ArrayList<String>();

		Matcher m2, m = p.matcher(buffer);
		m = p.matcher(buffer);
		while (m.find())
		{
			matchedStr = m.group();
			for (int i = 0; i < m.groupCount(); ++i)
			{
				debug(String.format("Group [%d] : %s", i, m.group(i)));
			}
			if (matchedStr.matches("(?i)^CREATE.*"))
			{
				ts = " IN \"" + data.getDataTableSpaceName("") + "\"" + " INDEX IN "
						+"\"" + data.getIndexTableSpaceName("") + "\"";				
			} else if (matchedStr.matches("(?i)^\\bIN\\b.*"))
			{
				countTablespaces++;
				m.appendReplacement(newBuffer, Matcher.quoteReplacement(ts));
				ts = "TS ERROR IN SETTING";
			} else if (matchedStr.matches("(?i)^\\bINDEX\\b\\s+\\bIN\\b.*"))
			{
				m.appendReplacement(newBuffer, "");
			} else if (matchedStr.matches("(?i)^PART.*"))
			{
				m2 = p2.matcher(matchedStr);
				while (m2.find())
				{
					for (int i = 0; i < m2.groupCount(); ++i)
					{
						debug(String.format("PART Group [%d] : %s", i, m2.group(i)));
					}
					part = m2.group(0) + "IN \"" + data.getDataTableSpaceName(m2.group(2)) + "\"" 
					         + " INDEX IN \"" + data.getIndexTableSpaceName(m2.group(2)) + "\"";
					countTablespaces++;
					debug("PART CLAUSE = " + part);
					m.appendReplacement(newBuffer, Matcher.quoteReplacement(part));
					part = "PART ERROR IN SETTING";
				}
			}
		}
		m.appendTail(newBuffer);
		debug("Modified SQL Begins ===============");
		debug(newBuffer.toString());
		debug("Modified SQL Ends   ===============");
		return newBuffer.toString();
	}
	
	private TSData initialSize (Matcher m)
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
	
	void testregex()
	{
		String regex = "(?mi)(_X\\b|_D\\b).*?\\bINITIALSIZE\\b\\s+(\\d+).*";
		Pattern p = Pattern.compile(regex);
		TSData ts = initialSize(p.matcher(buffer));
		info("========================================================="+ U.linesep);
		info(String.format("-- Total Data  Initial size = %14.2f KB", ts.getData()));
		info(String.format("-- Total Index Initial size = %14.2f KB", ts.getIndex()));
		info(String.format("-- Grand Total Initial size = %14.2f KB", (ts.getData()+ts.getIndex())));
		info(String.format("-- Grand Total Initial size = %14.2f MB", (ts.getData()+ts.getIndex())/(1024.0)));
		info(String.format("-- Grand Total Initial size = %14.2f GB", (ts.getData()+ts.getIndex())/(1024.0*1024.0)));
		info(String.format("-- Grand Total Initial size = %14.2f TB", (ts.getData()+ts.getIndex())/(1024.0*1024.0*1024.0)));
		info("========================================================="+ U.linesep);

	}
	
	public long getCountTablespaces()
	{
		return countTablespaces * 2 + countNonPartIndexTablespaces;
	}
	
	void run()
	{
		init();
		testregex();
		//modifyTableDDL(buffer);
		//modifyMQTDDL(buffer);
		//modifyIndexDDL(buffer, "EDW5P1");
	}
	
	public static void main(String[] args)
	{
		String myClass = new Throwable().getStackTrace()[0].getClassName();
		
		if (args.length < 1)
		{
			info("***********************************************************************************************");
			info("Usage: java -Xmx4096m " + myClass + " NameofFile");
			System.exit(-1);
		}

		fileName = args[0];
		Regex drv = new Regex();
		drv.run();
	}
}
