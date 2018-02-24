/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

/**
 * Author: Vikram Khatri vikram.khatri@us.ibm.com
 * Copyright: IBM Corporation
 */

public class SQLData
{
	private String SQL;
	private int numExecutions = 0;
	private float sumElapsedTime;
	private double sumGeometricMean;
	private long previousSumReturned = -1L;
	private float minTime = Float.MAX_VALUE, maxTime = Float.MIN_VALUE;
	private int deadlocks = 0;
	private int uniqueViolations = 0;
	private int rowsReturnedCounter = 0;
	
	public SQLData(String SQL)
	{
		if (SQL.length() > 0) 
			SQL = SQL.trim().replaceAll(";$", "");
		this.SQL = SQL;
	}
	
	public String getSQL()
	{		
		return SQL;
	}
	
	public void setElapsedTime(float elapsed)
	{
		if (elapsed < minTime)
			minTime = elapsed;
		if (elapsed > maxTime)
			maxTime = elapsed;
		if (numExecutions == 0)
		{
			sumElapsedTime = elapsed;
			sumGeometricMean = Math.log10(elapsed);
		} else
		{
			sumElapsedTime += elapsed;
			sumGeometricMean += Math.log10(elapsed);
		}
		++numExecutions;
	}
	
	public float getTotalElapsedTime()
	{
		return sumElapsedTime;
	}
	
	public float getAvgTime()
	{
		return (numExecutions == 0) ? 0.0F : sumElapsedTime / numExecutions;
	}
	
	public double getGeometricMean()
	{
		return (numExecutions == 0) ? 0.0F : Math.pow(10.0, (sumGeometricMean / numExecutions));
	}
	
	public void setRows(long num)
	{
		++rowsReturnedCounter;
		if (previousSumReturned == -1L)
			previousSumReturned = num;
		else
			previousSumReturned += num;
	}
	
	public long getAvgRowsReturned()
	{
		return (rowsReturnedCounter == 0) ? 0 : previousSumReturned / rowsReturnedCounter;
	}
	
	public float getMinTime()
	{
		return (minTime == Float.MAX_VALUE) ? 0.0F : minTime;
	}
	
	public float getMaxTime()
	{
		return (maxTime == Float.MIN_VALUE) ? 0.0F : maxTime;
	}
	
	public int getNumExecutions()
	{
		return numExecutions;
	}
	
	public int getDeadlocks()
	{
		return deadlocks;
	}
	
	public void sortheap()
	{
	}
	
	public void deadlock()
	{
		++deadlocks;
	}

	public int getUniqueViolations()
	{
		return uniqueViolations;
	}
	
	public void uniqueViolations()
	{
		++uniqueViolations;
	}
	
	public boolean errorExist()
	{
		return (deadlocks > 0 && uniqueViolations > 0);
	}
}