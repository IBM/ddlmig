package com.ibm.migr.utils;

import static com.ibm.migr.utils.Log.*;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

public class RunCommand implements Runnable
{
	private String shellCommand;
	private String arguments;
	
	public RunCommand()
	{
	}
	
	public RunCommand(String shellCommand, String arguments)
	{
		this.shellCommand = shellCommand;
		this.arguments = arguments;
	}
	
	@Override
	public void run()
	{
	      Process p = null;
	      String cmd[] = null;
	      
	      // Use exec(String[]) to handle spaces in directory
	      try
	      {
	         cmd = new String[]{U.getShell(),"-c", shellCommand, arguments};             
	         debug("CMD values " + Arrays.toString(cmd));
	         p = Runtime.getRuntime().exec(cmd);
	         final InputStream stdInput = p.getInputStream();
	         final InputStream stdError = p.getErrorStream();
			 // Handle stdout...
			 new Thread() 
			 {
				 public void run() 
				 {
					 try
					{
						U.copy(stdInput, System.out);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				 }
			 }.start();
			// Handle stderr...
			 new Thread() 
			 {
				 public void run() 
				 {
					 try
					{
						U.copy(stdError, System.out);
					} catch (IOException e)
					{
						e.printStackTrace();
					}
				 }
			 }.start();
			 p.waitFor();
			 if (stdInput != null) stdInput.close();
			 if (stdError != null) stdError.close();    
			 p.destroy();
	      } catch (Exception e) 
	      {	
	         e.printStackTrace();
	      }
	}
}
