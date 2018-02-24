package com.ibm.migr.db2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PreparedData
{
	public String sql, schema, name;
	private PreparedStatement pstmt;
	
	public PreparedData(Connection conn, String sql)
	{
		this.sql = sql;
		try
		{
			pstmt = conn.prepareStatement(sql);
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
	
	public boolean doesExist(String schema, String name)
	{
		boolean exists = false;
		try
		{
			pstmt.setString(1, schema);
			pstmt.setString(2, name);
			ResultSet rs = pstmt.executeQuery();
			if (rs.next())
			{
				exists = true;
			}
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
		return exists;
	}
	
	public void close()
	{
		try
		{
			pstmt.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		}
	}
}


