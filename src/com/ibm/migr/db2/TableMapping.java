/**
 * Author Vikram Khatri
 */
package com.ibm.migr.db2;

import static com.ibm.migr.utils.Log.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.ibm.migr.utils.U;

/**
 * @author Vikram
 * Simple bean for holding table data
 */
public class TableMapping implements Comparable<TableMapping> 
{
	private double increment = U.initialSizeIncrement;
	public String sequence;
	public String tabschema, tabname, compression, tableorg;
	public Date lastused;
	public String rowcompmode, pctpagessaved;
	public String partitionMode, pmapID, appendMode, type;
	public String statisticalProfile;
	public short pctfree;
	public Long card;
	public boolean isVolatile;
	public DBGroupData dbpgData;
	public int sourceMLNCount, tableID;
	public ArrayList<TablePartitionData> dataPartSizeList;
	public ArrayList<IndexData> indexPartSizeList;
	public String schemaID;
	private int countDataTS = 0, countIndexTS = 0;
 
	public TableMapping() 
	{
		;
	}
		
	public void savePropFile()
	{
		
	}
	
	public String getStatsProfile()
	{		
		String t = " SET PROFILE ONLY" + U.sqlTerminator + U.linesep;
		
		return (statisticalProfile == null || statisticalProfile.length() == 0) ? ""
				: statisticalProfile + t;
	}

	@Override
	public int compareTo(TableMapping o)
	{
		return lastused.compareTo(o.lastused);
	}
	
	public boolean existsIndex(String indexName)
	{
		for (IndexData data : indexPartSizeList)
		{
			if (data.indname.equals(indexName))
				return true;
		}
		return false;
	}
	
	private String getTableID()
	{
		return String.format("T%05d", tableID);
	}
	
	private String getPartitionID(String partitionName)
	{
		int partID = 0;
		for (TablePartitionData data : dataPartSizeList)
		{
			if (data.dataPartition.equals(partitionName))
			{
				partID = data.dataPartitionID;
				break;
			}
		}
		return String.format("D%03d", partID);
	}
	
	public String toString()
	{
		StringBuffer buffer = new StringBuffer();
		buffer.append("Schema             = " + tabschema + U.linesep);
		buffer.append("Table              = " + tabname + U.linesep);
		buffer.append("srcdbpgname        = " + dbpgData.srcDBPartitionGroupName + U.linesep);
		buffer.append("dstdbpgname        = " + dbpgData.dstDBPartitionGroupName + U.linesep);
		buffer.append("Last Used          = " + lastused + U.linesep);
		buffer.append("Card               = " + card + U.linesep);
		buffer.append("Compression        = " + compression + U.linesep);
		buffer.append("tableorg           = " + tableorg + U.linesep);
		buffer.append("rowcompmode        = " + rowcompmode + U.linesep);
		buffer.append("pctpagessaved      = " + pctpagessaved + U.linesep);
		buffer.append("partition_mode     = " + partitionMode + U.linesep);
		buffer.append("pmap_id            = " + pmapID + U.linesep);
		buffer.append("Append Mode        = " + appendMode + U.linesep);
		buffer.append("Volatile           = " + isVolatile + U.linesep);
		buffer.append("pctfree            = " + pctfree + U.linesep);
		buffer.append("Type               = " + type + U.linesep);
		buffer.append("stats profile      = " + statisticalProfile + U.linesep);
		buffer.append("src partition list = " + dbpgData.srcPartitions + U.linesep);
		buffer.append("dst partition list = " + dbpgData.dstPartitions + U.linesep);
		buffer.append("Partition Ratio    = " + getDBPartRatio() + U.linesep);
		buffer.append("Data "+ U.linesep);
		buffer.append("  Range partitions       = " + getPartitionDataCount() + U.linesep);
		buffer.append("  Avg range part size    = " + getAverageRangePartitonSize() + U.linesep);
		buffer.append("  Cold Partitions        = " + getColdPartitions() + U.linesep);
		buffer.append("  Hot Partitions         = " + getHotPartitions() + U.linesep);
		buffer.append("  # of rows affected     = " + getNumberofRowsAffected() + U.linesep);
		buffer.append("Indexes "+ U.linesep);
		buffer.append("  Total Indexes          = " + getTotalIndexCount() + U.linesep);
		buffer.append("  Partitioned Indexes    = " + getPartitionIndexCount() + U.linesep);
		buffer.append("  Non-Part indexes       = " + getNonPartitionIndexCount() + U.linesep);
		buffer.append("  Part Index Avg. size   = " + getPartitionedIndexAverageSize() + U.linesep);
		buffer.append("  Non-Part Index Avg.Size= " + getNonPartitionedIndexAverageSize() + U.linesep);
		buffer.append("=========================================================" + U.linesep);
		return buffer.toString();
	}

	public static Comparator<TableMapping> CardCompare = new Comparator<TableMapping>() {
		public int compare(TableMapping t1, TableMapping t2)
		{
			return t1.card.compareTo(t2.card);
		}
	};
	
	public String getNumberofRowsAffected()
	{
		long i = 0, u = 0,d = 0;
		for (TablePartitionData data : dataPartSizeList) 
		{
			i += data.rowsInserted;
			u += data.rowsDeleted;
			d += data.rowsUpdated;
		}
		return "Inserted="+i+":Deleted="+d+":updated"+u;
	}
	
	public String getColdPartitions()
	{
		boolean once = true;
		StringBuffer buf = new StringBuffer();
		for (TablePartitionData data : dataPartSizeList) 
		{
			if (dataPartSizeList.size() > 0)
			{
				if (data.rowsInserted == 0 && data.rowsDeleted == 0 && data.rowsUpdated == 0)
				{
					if (once)
					{
						once = false;
						buf.append(data.dataPartition);
					} else
					{
						buf.append(","+data.dataPartition);
					}
				}
			} else
			{
				buf.append("");
			}
		}
		return buf.toString();
	}
	
	public String getHotPartitions()
	{
		boolean once = true;
		StringBuffer buf = new StringBuffer();
		for (TablePartitionData data : dataPartSizeList) 
		{
			if (dataPartSizeList.size() > 0)
			{
				if (data.rowsInserted > 0 && data.rowsDeleted > 0 && data.rowsUpdated > 0)
				{
					if (once)
					{
						once = false;
						buf.append(data.dataPartition);
					} else
					{
						buf.append(","+data.dataPartition);
					}
				}
			} else
			{
				buf.append("");
			}
		}
		return buf.toString();
	}
	
	public long getAverageRangePartitonSize()
	{
		int count = 0;
		long size = 0L;			
		
		for (TablePartitionData data : dataPartSizeList) 
		{
			size += data.size;
			++count;
		}
		return (count == 0) ? size : (long) (1.0 * size / count);
	}
	
	public float getDBPartRatio()
	{
		int srcCount = dbpgData.srcPartitions.split(",").length;
		int dstCount = dbpgData.dstPartitions.split(",").length;
		
		return (float) (srcCount * 1.0 / dstCount);
	}
	
	public long getAverageRangePartitionInitialSize()
	{
		long size = (long) (increment * getDBPartRatio() * getAverageRangePartitonSize());
		return (size < 512) ? 512 : size;
	}

	public int getTotalIndexCount()
	{
		String[] array = new String[indexPartSizeList.size()];
		int index = 0;
		for (IndexData data : indexPartSizeList) 
		{
		  array[index] = data.indschema + "." + data.indname;
		  index++;
		}
		Set<String> temp = new HashSet<String>(Arrays.asList(array));		
		return temp.size();
	}
	
	public int getPartitionIndexCount()
	{
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int partitionedIndexcount = 0;
		for (IndexData data : indexPartSizeList) 
		{
			String key = data.indschema + "." + data.indname;
			if (map.containsKey(key))
			{
				map.put(key, map.get(key)+1);
			} else
			{
				map.put(key, 1);
			}
		}
		for (Map.Entry<String, Integer> key : map.entrySet())
		{
			if (key.getValue() > 1)
				++partitionedIndexcount;
		}
		return partitionedIndexcount;
	}
	
	public int getNonPartitionIndexCount()
	{
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		int nonPartitionedIndexcount = 0;
		for (IndexData data : indexPartSizeList) 
		{
			String key = data.indschema + "." + data.indname;
			if (map.containsKey(key))
			{
				map.put(key, map.get(key)+1);
			} else
			{
				map.put(key, 1);
			}
		}
		for (Map.Entry<String, Integer> key : map.entrySet())
		{
			if (key.getValue() == 1)
				++nonPartitionedIndexcount;
		}
		return nonPartitionedIndexcount;		
	}

	public int getPartitionDataCount()
	{
		return dataPartSizeList.size();
	}
	
	public int getDstDBPartitionCount()
	{
		return dbpgData.dstPartitions.split(",").length;		
	}

	public int getSrcDBPartitionCount()
	{
		return dbpgData.srcPartitions.split(",").length;		
	}

	public long getIndexAverageSize()
	{
		long size = 0L;			
		
		for (IndexData data : indexPartSizeList) 
		{
			if (data.indexPartioning == null || (data.indexPartioning != null && data.indexPartioning.trim().length() == 0))
			{
				size += data.size;
			}
		}
		return (indexPartSizeList.size() == 0) ? size :  (long) (1.0 * size / indexPartSizeList.size());
	}
	
	public long getPartitionedIndexAverageSize()
	{
		long size = 0L, totalSize = 0L;
		HashMap<String, DataSize> map = new HashMap<String, DataSize>();			
		
		for (IndexData data : indexPartSizeList) 
		{
			if (data.indexPartioning != null && data.indexPartioning.equals("P"))
			{
				String key = data.indschema + "." + data.indname;
				if (map.containsKey(key))
				{
					DataSize d = map.get(key);
					d.count += 1;
					d.size += data.size;
					map.put(key, d);				
				} else
				{
					map.put(key, new DataSize(1, data.size));
				}				
			}
		}
		for (Map.Entry<String, DataSize> key : map.entrySet())
		{
			DataSize d = key.getValue();
			size = (long) (1.0 * d.size / d.count);
			totalSize += size;
		}		
		return totalSize;
	}
	
	public long getNonPartitionedIndexAverageSize()
	{
		long size = 0L;			
		
		for (IndexData data : indexPartSizeList) 
		{
			if (data.indexPartioning != null && data.indexPartioning.equals("N"))
			{
				size += data.size;
			}
		}
		return (indexPartSizeList.size() == 0) ? size :  (long) (1.0 * size / indexPartSizeList.size());
	}
	
	public long getIndexInitialSize()
	{
		long size = (long) (increment * getDBPartRatio() * getIndexAverageSize());
		return (size < 512) ? 512 : size;
	}
	
	public long getPartitionedIndexInitialSize()
	{
		long size = (long) (increment * getDBPartRatio() * getPartitionedIndexAverageSize());
		return (size < 512) ? 512 : size;
	}
	
	public long getNonPartitionedIndexInitialSize()
	{
		long size = (long) (increment * getDBPartRatio() * getNonPartitionedIndexAverageSize());
		return (size < 512) ? 512 : size;
	}
	
	private String getTableSpaceName(String token, String partitionName)
	{
		String tableSpaceName;
		int partSize = dataPartSizeList.size();
		if (partSize == 1)
		{
		    tableSpaceName = schemaID + "_" + getTableID() + "_" + token;
		} else {
		    tableSpaceName = schemaID + "_" + getTableID() + "_" + getPartitionID(partitionName) +  "_" + token;				
		}
		debug("tableSpaceName = " + tableSpaceName);
		return tableSpaceName;
	}
	
	public String getDataTableSpaceName(String partitionName)
	{
		return getTableSpaceName("D", partitionName);
	}
	
	public String getIndexTableSpaceName(String partitionName)
	{
		return getTableSpaceName("X", partitionName);
	}

	
	public ArrayList<String> getPartitionandNonPartitionTableSpacesDefinition()
	{
		ArrayList<String> list = new ArrayList<String>();
		String tableSpaceName, genStr;
		String dataBufferpool = U.dataBufferpool.split(",")[0];
		String idxBufferpool  = U.idxBufferpool.split(",")[0];
		String stogroupdata = U.stogroupdata.split("\\|")[0];
		String stogroupidx  = U.stogroupidx.split("\\|")[0];
		int dataBufferpoolPageSize = Integer.valueOf(U.dataBufferpool.split(",")[1]);
		int idxBufferpoolPageSize  = Integer.valueOf(U.idxBufferpool.split(",")[1]); 
		int extentSize = U.extentSize;
		int partSize = dataPartSizeList.size();
		long size = 0L;

		for (TablePartitionData data : dataPartSizeList)
		{
			tableSpaceName = getDataTableSpaceName(data.dataPartition);
			size = getAverageRangePartitionInitialSize();			
			genStr = "CREATE LARGE TABLESPACE "+ tableSpaceName 
					+ " IN DATABASE PARTITION GROUP " + dbpgData.dstDBPartitionGroupName
					+ " PAGESIZE " + dataBufferpoolPageSize 
					+ " MANAGED BY AUTOMATIC STORAGE "
					+ " USING STOGROUP " + stogroupdata
					+ " INITIALSIZE " + size + " K" 
					+ " INCREASESIZE " + getIncreaseSize(size) + " K" 
					+ " EXTENTSIZE " + extentSize 
					+ " PREFETCHSIZE AUTOMATIC " 
					+ " BUFFERPOOL " 
					+ dataBufferpool;
			list.add(genStr);
			++countDataTS;
			tableSpaceName = getIndexTableSpaceName(data.dataPartition);
			if (partSize == 1)
			{
			    size = getIndexInitialSize();
			} else {
			    size = getPartitionedIndexInitialSize();
			}
			genStr = "CREATE LARGE TABLESPACE "+ tableSpaceName
					+ " IN DATABASE PARTITION GROUP " + dbpgData.dstDBPartitionGroupName
					+ " PAGESIZE " + idxBufferpoolPageSize 
					+ " MANAGED BY AUTOMATIC STORAGE "
					+ " USING STOGROUP " + stogroupidx
					+ " INITIALSIZE " + size + " K" 
					+ " INCREASESIZE " + getIncreaseSize(size) + " K" 
					+ " EXTENTSIZE " + extentSize
					+ " PREFETCHSIZE AUTOMATIC "
					+ " BUFFERPOOL " 
					+ idxBufferpool;
			//list.add("-- Initial Index size in K = " + size);
			list.add(genStr);
			++countIndexTS;
		}
		return list;
	}
	
	private String getNonPartitionedIndexTableSpacesName(int indexID)
	{
		return schemaID + "_" + getTableID() + "_N" + String.format("%03d", indexID) + "_X";
	}
	
	private int getIndexID(String indexName)
	{
		int partID = 0;
		for (IndexData data : indexPartSizeList)
		{
			if (data.indname.equals(indexName)) 
			{
				partID = data.indexID;
				break;
			}
		}
		return partID;
	}
	
	public String getNonPartitionedIndexTableSpacesName(String indexName)
	{
		int indID = getIndexID(indexName);
		return getNonPartitionedIndexTableSpacesName(indID);
	}

	public ArrayList<String> getNonPartitionedIndexTableSpacesDefinition()
	{
		ArrayList<String> list = new ArrayList<String>();
		String tableSpaceName, genStr;
		String idxBufferpool  = U.idxBufferpool.split(",")[0];
		String stogroupidx  = U.stogroupidx.split("\\|")[0];
		int idxBufferpoolPageSize  = Integer.valueOf(U.idxBufferpool.split(",")[1]); 
		int extentSize = U.extentSize;
		
		HashMap<String, Integer> map = new HashMap<String, Integer>();			
		
		for (IndexData data : indexPartSizeList) 
		{
			if (data.indexPartioning != null && data.indexPartioning.equals("N"))
			{
				String key = getNonPartitionedIndexTableSpacesName(data.indexID);
				if (map.containsKey(key))
				{
					map.put(key, map.get(key)+1);				
				} else
				{
					map.put(key, 1);
				}	
			}
		}
		for (Map.Entry<String, Integer> key : map.entrySet())
		{
			if (key.getValue() == 1)
			{
				long size = getNonPartitionedIndexInitialSize();
			    tableSpaceName = key.getKey();
			    genStr = "CREATE LARGE TABLESPACE "+ tableSpaceName
					+ " IN DATABASE PARTITION GROUP " + dbpgData.dstDBPartitionGroupName
					+ " PAGESIZE " + idxBufferpoolPageSize 
					+ " MANAGED BY AUTOMATIC STORAGE "
					+ " USING STOGROUP " + stogroupidx
					+ " INITIALSIZE " + size + " K" 
					+ " INCREASESIZE " + getIncreaseSize(size) + " K" 
					+ " EXTENTSIZE " + extentSize
					+ " PREFETCHSIZE AUTOMATIC "
					+ " BUFFERPOOL " 
					+ idxBufferpool;
				//list.add("-- Initial Non-Part Index size in K = " + size);
			    list.add(genStr);
			    ++countIndexTS;
			}
		}		
		return list;
	}
	
	public int getCountDataTS()
	{
		return countDataTS;
	}
	
	public int getCountIndexTS()
	{
		return countIndexTS;
	}
	
	private long getIncreaseSize(long size)
	{
		if (size < 1024*10)
			return size;
		else
		{
			return (long) (0.25 * size);
		}
	}
}
