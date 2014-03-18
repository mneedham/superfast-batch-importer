package org.neo4j.batchimport.newimport.structs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DiskRecordsBuffer {
	int type, arrayIndex, qIndex;
	private int recordCount, maxEntries, curEntries;
	private List<Object>[] records = null;
	protected boolean isLastBuf = false, inUse = false;
	public DiskRecordsBuffer(int type, int entries, int arrayIndex){
		this.maxEntries = entries;
		this.type = type;
		this.arrayIndex = arrayIndex;
		List<Object>[] recs = new ArrayList[this.maxEntries];
		for (int i = 0; i < this.maxEntries; i++)
			recs[i] = new ArrayList<>();
			records = recs;	
	}
	public int getMaxEntries(){
		return maxEntries;
	}
	public int getRecordCount(){
		return recordCount;
	}
	public int getCurrentEntries(){
		return curEntries;
	}
	public boolean isLastBuffer(){
		return isLastBuf;
	}
	public void setLastBuffer(boolean lastBuffer){
		isLastBuf = lastBuffer;
	}

	public int extend(int entries) {
		if (entries <= maxEntries)
			return 0;

		List<Object>[] newRecords = new ArrayList[entries];
		System.arraycopy(this.records, 0, newRecords, 0, this.records.length);
		for (int i =  records.length; i < entries; i++)
			newRecords[i] = new ArrayList<>();
			records = newRecords;
			maxEntries = entries;
			return maxEntries;
	}
	public int addRecord(Object rec, int index){
		if (curEntries < index+1)
			curEntries = index +1;
		recordCount++;
		records[index].add(rec);
		return records[index].size()-1;
	}
	public Object getRecord(int index, int offset){
		return records[index].get(offset);
	}
	public void clearRecord(int index){
		records[index].clear();
	}
	public boolean isEmpty(int index){
		return records[index].isEmpty();
	}
	public Iterator<Object> iterator(int index){
		return records[index].iterator();
	}
	public int getSize(int index){
		return records[index].size();
	}
	public void cleanup(){  
		inUse = false;
		recordCount = 0;
		for (int i = 0; i < this.maxEntries; i++)
			records[i].clear();
		if ((Runtime.getRuntime().freeMemory()/Constants.mb) < 100)
			System.gc();
	}	
}
