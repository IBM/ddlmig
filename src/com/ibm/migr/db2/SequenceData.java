/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

import com.ibm.migr.utils.U;

public class SequenceData
{
	public String schema, name;
	public long restartValue;
	
	public String getRestartSQL()
	{
		return "ALTER SEQUENCE " + U.putQuote(schema) + "." + U.putQuote(name) + " RESTART WITH " + restartValue + U.linesep;
	}
}
