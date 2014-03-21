package org.neo4j.batchimport.newimport.structs;

import java.util.Arrays;
import org.neo4j.batchimport.newimport.utils.Utils;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;

public abstract class AbstractDataBuffer {
	private int stageIndex;
	private StringBuilder rawStrBuf = null;
	private int[][][] records = null;
	private int maxEntries = 0, curEntries = 0, numColumns = 0;
	private boolean moreData = true;
	protected int bufSequenceId = -1;
	protected long[] id = null;
	private DiskRecordsBuffer[] diskRecords = new DiskRecordsBuffer[4];
	private DiskRecordsCache diskRecordsCache;
	
	public AbstractDataBuffer(int maxEntries, int bufSize, int index, DiskRecordsCache diskCache) {
		this.maxEntries = maxEntries;
		id = new long[this.maxEntries];
		rawStrBuf = new StringBuilder();
		rawStrBuf.ensureCapacity(bufSize);
		diskRecordsCache = diskCache;
		for (int i = 0; i < diskRecords.length; i++)
			diskRecords[i] = diskRecordsCache.getDiskRecords(i);
	}
	public StringBuilder getStrBuf(){
		return rawStrBuf;
	}
	public int getNumColumns(){
		return numColumns;
	}
	public int getBufSequenceId(){
		return bufSequenceId;
	}
	public void setBufSequenceId(int sequenceId){
		bufSequenceId = sequenceId;
	}
	public boolean moreData(){
		return this.moreData;
	}

	public int getCurEntries(){
		return curEntries;
	}
	public int getMaxEntries(){
		return maxEntries;
	}
	public int getStageIndex(){
		return stageIndex;
	}
	public void setStageIndex(int index){
		stageIndex = index;
	}
	public long getId(int index){
		return id[index];
	}
	public void setId(int index, long id){
		this.id[index] = id;
	}
	public void initRecords(int numColumns){
		if (this.numColumns != numColumns){
			this.numColumns = numColumns;
			records = new int[this.maxEntries][numColumns][2];
		}
	}
	public int addRecord(int[][] record, int type) {
		if (curEntries == maxEntries){
			//expand
			this.maxEntries += Constants.DATA_INCREMENT;
			int[][][] newRecords = new int[this.maxEntries][numColumns][2];
			for (int i = 0 ; i < records.length; i++)
				Utils.arrayCopy(records[i], newRecords[i]);
			records = newRecords;
			long[] newId = new long[this.maxEntries];
			System.arraycopy(id, 0, newId, 0, id.length);
			id = newId;
			for (int i = 0; i < diskRecords.length; i++)
				if (diskRecords[i] != null && diskRecords[i].getMaxEntries() < this.maxEntries)
					diskRecords[i].extend(this.maxEntries);
		}   		
		Utils.arrayCopy(record, records[curEntries]);
		return curEntries++;
	}
	public int[][][] getRecords() {
		return records;
	}
	public boolean isMoreData() {
		return moreData;
	}
	public void setMoreData(boolean moreData) {
		this.moreData = moreData;
	}
	
	public void cleanup(){
		curEntries = 0;
		rawStrBuf.setLength(0);
		for (int i = 0; i < records.length; i++)
			for (int j = 0; j < records[i].length; j++)
				Arrays.fill(records[i][j], 0);
		this.moreData = true;
	}
	private long getLong(int[][] record, int colIndex){
		int[] valIndex = record[colIndex];
		String val = rawStrBuf.substring(valIndex[0], valIndex[1]);
		return Long.parseLong(val.trim());
	}
	public long getLong(int rowIndex, int colIndex){
		int[] valIndex = records[rowIndex][colIndex];
		String val = rawStrBuf.substring(valIndex[0], valIndex[1]);
		return Long.parseLong(val.trim());
	}
	public String getString(int rowIndex, int colIndex){
		int[] valIndex = records[rowIndex][colIndex];
		String val = rawStrBuf.substring(valIndex[0], valIndex[1]).trim();
		return val;
	}
	public DiskRecordsBuffer getDiskRecords(int type){
		return diskRecords[type];
	}
	public DiskRecordsBuffer removeDiskRecords(int type){
		DiskRecordsBuffer recs =  diskRecords[type];
		 diskRecords[type] = diskRecordsCache.getDiskRecords(type);
		 if (diskRecords[type].getMaxEntries() < this.maxEntries)
			 diskRecords[type].extend(this.maxEntries);
		return recs;
	}
	public PropertyRecord getPropertyRecord(long id, int index, int offset){
		PropertyRecord propRec = new PropertyRecord(id);
		propRec.setInUse( true );
		propRec.setCreated();
		PropertyBlock block = (PropertyBlock)diskRecords[Constants.PROPERTY].getRecord(index, offset );
		propRec.addPropertyBlock( block );
		return propRec;
	}
}
