package com.ibm.migr.utils;

import static com.ibm.migr.utils.Log.debug;
import static com.ibm.migr.utils.Log.error;
import static com.ibm.migr.utils.Log.info;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.nio.channels.FileChannel;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import com.ibm.migr.db2.UOWData;
import com.ibm.migr.db2.db2;

/**
 * @author Vikram. A general purpose utility class to do specic and 
 * repeatative things. Most of the stuff in this class is static 
 * that does not require an instantiation of the class 
 *
 */
public class U
{
	private static HashMap<Integer, UOWData> uowMap = new HashMap<Integer, UOWData>();
	private static HashMap<String, Long> memberMap = new HashMap<String, Long>();
	private static HashMap<String, Long> elapsedHostMap = new HashMap<String, Long>();
	private static Properties progProp = new Properties();	
	
	public static long uowGapTime = 0L;
	public static final long start = System.currentTimeMillis();
	public static boolean alternate = true, useIngest = false;
	public static int dataBufferSize = 8388608, counter = 1;
	public static String dbName = "", schema = "";
	private final static String osType = (System.getProperty("os.name").toUpperCase().startsWith("WIN")) ? "WIN" : 
    	(System.getProperty("os.name").startsWith("z/OS")) ? "z/OS" : "OTHER";
	public final static String filesep = System.getProperty("file.separator");
	public final static String linesep = System.getProperty("line.separator");
	public static boolean useDB2 = true, refreshed = false, norowwarnings = false;
	public final static String CONNECTION_PROPERTY_FILE = "db.properties";
	public static String dataDelimitor = "|"; // Override this using -DDIMILITER=|
	public static String sqlTerminator = ";";
	public static String ext, outputDataDirectory = "", workingDirectory = "", messageDirectory = ""; 
	public static String inputDataDirectory = "";
	public static boolean retry = false;
	public static boolean autoCommit = false, saveQueryResults = false;
	public static boolean driverLoaded = false, checkDestinationObjects = false;

	public final static String fileEncoding = "UTF-8";
	public static String tableSpaceData = "", tableSpaceIndex = "";
	private static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MMddyyyy_hhmmssSSS");
	
	public static String appDelay = "0";
	public static int numThreads = 1, fetchSize = 1000;
	public static int targetMLNCount;
    public static String dataBufferpool, idxBufferpool, tempBufferpool;
    public static String mapdbpg, stogroupdata, stogroupidx, stogrouptemp;
    public static double initialSizeIncrement;
    public static int extentSize, majorSourceDBVersion = -1, minorSourceDBVersion = -1;
    
    public static String driverShellScriptName  = "00deployobjects";
    public static String dbpgbptsFileName       = "01dbpgbpts.sql";
    public static String tablespacesFileName    = "02tablespaces.sql";
    public static String schemasFileName        = "03schema.sql";
    public static String globalVariableFileName = "04globalvars.sql";
    public static String runstatsFileName       = "49runstats.sql";
    public static String authsFileName          = "50auths.sql";
    public static String tableInfoFileName      = "98tableinfo.log";
    public static String deployObjectsFileName  = "99deployobjects";
    public static String ddlLogFileName         = "00deployobjects.log";


    public static String getTime()
    {
    	return  new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
    }
    
    public static String trim(String name)
    {
    	if (name != null)
    		name = name.trim();
    	return name;
    }
    
    private static String getProgPropFileName()
    {
    	return System.getProperty("user.dir") + U.filesep + "ts.properties";
    }
    
    private static void loadProperties()
    {
    	String fileName = getProgPropFileName();
    	FileInputStream input = null;
		try
		{
			input = new FileInputStream(fileName);
			progProp.load(input);
            debug("Configuration file loaded: '" + fileName + "'" + "("+progProp.size()+")");	            
		} catch (IOException e)
		{
			info(fileName + " not found. Continuing ...");
		} finally
		{
			if (input != null)
			{
				try
				{
					input.close();
				} catch (IOException e)
				{
					error("loadProperties", e);
				}
			}
		}
    }
    
    public static String getProperty(String key)
    {
    	loadProperties();
    	return progProp.getProperty(key);
    }
    
    public static void putProperty(String key, String value)
    {    	
    	OutputStream output = null;
		try
		{
			debug("Propertry file name = " + getProgPropFileName());
			output = new FileOutputStream(getProgPropFileName());
			progProp.put(key, value);
	    	progProp.store(output, null);
		} catch (IOException e)
		{
			error("putProperty", e);
		} finally
		{
			if (output != null)
			{
				try
				{
					output.close();
				} catch (Exception e)
				{
					error("putProperty", e);
				}
			}
		}
    }
	
	/**
	 * Determine OS type for the purpose of writing scripts
	 * @return
	 */
	public static boolean win()
	{
		return osType.equalsIgnoreCase("Win");
	}
	
	public static HashMap<Integer, UOWData> getUOWMap()
	{
		return uowMap;
	}
	
	public static HashMap<String, Long> getMemberMap()
	{
		return memberMap;
	}
	
	public static HashMap<String, Long> getElapsedHostMap()
	{
		return elapsedHostMap;
	}
	
	public static String truncStr(Object obj, int maxLength, String suffix) 
	{
		if (obj == null || maxLength == 0) 
		{
		   return "";
		}
		String str = obj.toString();
		if (str.length() <= maxLength)
			return str;
		if (suffix == null)
			suffix = "...";
		int maxNumChars = maxLength - suffix.length();
		if (maxNumChars < 0)
		{
			str = suffix.substring(0, maxLength);
		} else if (str.length() > maxNumChars)
		{
			str = str.substring(0, maxNumChars) + suffix;
		}
		return str;
	}
	
	/**
	 * chmod file. But this did not work on Linux
	 * @param fileName
	 */
	public static void chmod(String fileName)
	{
		if (!U.win())
		{
	        try
			{
	    		String cmd = "/bin/chmod 755 "+ U.putQuote(fileName);
	        	//U.log(cmd);
	    		Runtime.getRuntime().exec(cmd);
			} catch (IOException e)
			{
				e.printStackTrace();
			}			
		}
	}
	
	/**
	 * Get value from -D switch
	 * @param name
	 * @param defaultValue
	 * @return
	 */
	static public int setVariable(String name, int defaultValue)
	{
		String val = System.getProperty(name);
		int variable = defaultValue;
		if (val == null)
		{
			 variable = defaultValue;			
		} else
		{
			try
			{
				variable = Integer.valueOf(val);
			} catch (Exception e)
			{
				e.printStackTrace();
				System.exit(-1);
			}
		}
		return variable;
	}
	
	/**
	 * Count params
	 * @param haystack
	 * @param needle
	 * @return
	 */
	public static int countParams(CharSequence haystack, char needle)
	{
		int count = 0;
		for (int i = 0; i < haystack.length(); i++)
		{
			if (haystack.charAt(i) == needle)
			{
				count++;
			}
		}
		return count;
	}
	
	/**
	 * Set Thread id for logging purposes
	 * @param runid
	 * @return
	 */
	public static String runName(int runid)
	{
		String str;
		if (runid < 99)
    	{
    		str = String.format("%02d ", runid);
    	} else
    	{
    		str = String.format("%03d ", runid);
    	}
		return str;
	}
	
	/**
	 * Put quote around name for scripts
	 * @param name
	 * @return
	 */
    public static String putQuote(String name)
    {
        if (name == null || name.length() == 0) return name;
        if (name.charAt(0) == '"') return name;
        return "\""+name+"\"";
    }

    /**
     * Truncate file
     * @param file
     */
	public static void truncateFile(String file)
	{
		FileChannel outChan;
		try
		{
			outChan = new FileOutputStream(file, true).getChannel();
			outChan.truncate(0);
			outChan.close();
		} catch (FileNotFoundException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}

	/**
	 * Check if file exists
	 * @param fileName
	 * @return
	 */
	public static boolean FileExists(String fileName)
	{
	    	if (fileName == null) return false;
	    	File f = new File(removeQuote(fileName));
	    	//String path = f.getAbsolutePath();
	    	return f.exists();
	}
	
	/**
	 * Remove double quote from the string beginning and end
	 * @param name
	 * @return
	 */
	public static String removeQuote(String name)
    {
    	if (name == null || name.length() == 0) return name;
    	int len = name.length();
    	if (name.charAt(0) == '"' && len > 2)
    		return name.substring(1, len-1);
    	else
    		return name;
    }
	
	/**
	 * Set processing time after finish of the batch so that file names
	 * and the staging table names can be set
	 */
	public static String getBatchProcessingTime()
	{
		return simpleDateFormat.format(System.currentTimeMillis());
	}

	/**
	 * Get elapsed time. 
	 * put long start = System.currentTimeMillis(); at the beginning 
	 * from where the elapsed time is required
	 * @param start
	 * @return
	 */
	public static String getElapsedTime(long start)
	{
		long now = System.currentTimeMillis();
		String elapsedTimeStr = "";
		long elapsed = (now - start) / 1000;
		int days, hours, min, sec, ms;
		ms = (int) ((now - start) - (elapsed * 1000));
		String milli = String.format("%.3f", (1.0 * ms / 1000.0));
		if (elapsed >= 86400)
		{
			days = (int) elapsed / 86400;
			if (days > 1)
				elapsedTimeStr = days + " days ";
			else
				elapsedTimeStr = days + " day ";
		}
		elapsed = elapsed % 86400;
		if (elapsed >= 3600 && elapsed < 86400)
		{
			hours = (int) elapsed / 3600;
			if (hours > 1)
				elapsedTimeStr += hours + " hours ";
			else
				elapsedTimeStr += hours + " hour ";
		}
		elapsed = elapsed % 3600;
		if (elapsed >= 60 && elapsed < 3600)
		{
			min = (int) elapsed / 60;
			if (min > 1)
				elapsedTimeStr += min + " mins ";
			else
				elapsedTimeStr += min + " min ";
		}
		sec = (int) elapsed % 60;
		elapsedTimeStr += String.format("%02d%s sec", sec, milli.substring(1));
		return elapsedTimeStr;
	}
	
	public static void createOutputDir()
	{
		File tmpfile = new File(outputDataDirectory);
	    tmpfile.mkdirs();
	    try
	    {
	       debug("OUTPUT_DIR is : " + tmpfile.getCanonicalPath());
	    } catch (IOException e1)
	    {
	       debug(e1.getMessage());
	    }
	}
	
	/**
	 * Output dir name and create it if necessary
	 * Get the name either from start program or from -D switch
	 * @return
	 */
	public static String getOutputDirName()
	{
		if (outputDataDirectory == "")
			outputDataDirectory = "." + filesep + "output";
		try
		{
	        File tmpfile = new File(outputDataDirectory);
	        tmpfile.mkdirs();
	        outputDataDirectory = tmpfile.getCanonicalPath();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return outputDataDirectory;		
	}
	
	/** 
	 * Create directory if needed
	 * @param dir
	 */
	public static void createDirectory(String dir)
	{
		File tmpfile = new File(dir);
		if (!tmpfile.isDirectory())
           tmpfile.mkdirs();
	}
	
	/**
	 * Get input dir name and get it either from -D switch 
	 * or from command line
	 * @return
	 */
	private static String getInputDirName()
	{
		String inputDir = System.getProperty("INPUT_DIR");
		
		if (inputDir == null)
			inputDir = "." + filesep;
		try
		{
			inputDir = new File(inputDir).getCanonicalPath();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		return inputDir;		
	}

	/**
	 * Get current schema name for scripting purpose
	 * @return
	 */
	public static String getCurrentSchema()
	{
		if (useDB2 && schema.length() == 0)
		{
			db2 db2Conn = new db2();
			schema = db2Conn.getCurrentSchema();
			db2Conn.close();
		}
		return schema;
	}
	
	public static String ReadFile(String fileName)
	{
		try
		{
			BufferedReader bufferedReader;
			bufferedReader = new BufferedReader(new FileReader(fileName));
			StringBuffer stringBuffer = new StringBuffer();
			String line = null;
			while((line =bufferedReader.readLine())!=null)
			{
			   stringBuffer.append(line).append("\n");
			}
			bufferedReader.close();
			return stringBuffer.toString();
		} catch (Exception e)
		{
			error("ReadFile Error", e);
		}
		return null;
	}
	
	public static void WriteFile(String name, String data)
	{
		WriteFile(true, name, data);
	}
	
	public static void WriteFile(boolean append, String name, String data)
	{
		BufferedWriter buffer;
		String scriptName = "";
		
		try
		{
			scriptName = U.getOutputDirName() + U.filesep + name;
			buffer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(scriptName, append), U.fileEncoding));			
			buffer.write(data);
			buffer.close();
		} catch (UnsupportedEncodingException e)
		{
			error("Error creating encoding for ", e);
		} catch (FileNotFoundException e)
		{
			error("Error creating file ", e);
		} catch (IOException e)
		{
			error("Error file ", scriptName, e);
		}
	}
	
    public static String getShell()
    {
    	String shell = null;
    	if (win())
    		shell = "cmd";
    	else
    	{
    		String line;
    		Process p;
			try
			{
				p = Runtime.getRuntime().exec("env");
	    		BufferedReader stdInput = new BufferedReader(new InputStreamReader(p.getInputStream()));
				while ((line = stdInput.readLine()) != null)
				{
					if (line.startsWith("SHELL"))
					{
						shell = line.substring(line.indexOf("=")+1);
						break;
					}
				}				
				p.destroy();
				if (stdInput != null)
					stdInput.close();
			} catch (IOException e)
			{
				e.printStackTrace();
			}
			if (shell.equalsIgnoreCase("cmd"))
				shell = "/bin/sh";
    	}
    	return shell;
    }
    
    private static int BUFFER_SIZE = 512;
	public static void copy(InputStream in, OutputStream out)
			throws IOException
	{
		copy(in, out, BUFFER_SIZE);
	}

	public static void copy(InputStream in, OutputStream out,
			int aBufferSize) throws IOException
	{
		byte[] buffer = new byte[aBufferSize];
		int length;

		while ((length = in.read(buffer)) > -1)
		{
			out.write(buffer, 0, length);
		}
		out.flush();
	}
}
