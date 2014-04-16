package org.neo4j.batchimport.importer.structs;

import java.util.concurrent.ArrayBlockingQueue;


public class DiskRecordsCache {
	private int maxEntries;
	private DiskRecordsBuffer[][] diskRecords;
	private ArrayBlockingQueue<Integer>[] available;
	public DiskRecordsCache(int size, int maxEntries){
		this.maxEntries = maxEntries;
		diskRecords = new DiskRecordsBuffer[Constants.RECORDS_TYPE_MAX][size];
		available = new ArrayBlockingQueue[Constants.RECORDS_TYPE_MAX];
		for (int j = 0; j < Constants.RECORDS_TYPE_MAX; j++)
			available[j] = new ArrayBlockingQueue<Integer>(size);
		for (int j = 0; j < Constants.RECORDS_TYPE_MAX; j++){
			for (int i = 0; i < size; i++){
				diskRecords[j][i] = new DiskRecordsBuffer(this.maxEntries, i); 
				try {
					available[j].put(i);
				}catch (InterruptedException ie){
					
				};
			}
		}
	}
	public DiskRecordsBuffer getDiskRecords(int type){
		try {
			DiskRecordsBuffer recs = diskRecords[type][available[type].take()];
			recs.inUse = true;
			return recs;
		}catch (Exception ie){
			System.out.println(ie.getMessage()+" DiskRecordsCache.getDiskRecords");
			return null;
		}
	}
	public void putDiskRecords(DiskRecordsBuffer recs, int type){
		try {
			recs.cleanup();
			available[type].put(recs.arrayIndex);
		}catch (Exception ie){
			System.out.println(ie.getMessage()+" DiskRecordsCache.putDiskRecords");
		};
	}

}
