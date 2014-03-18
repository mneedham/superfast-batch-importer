package org.neo4j.batchimport.newimport.stages;
import java.lang.reflect.Method;

import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.batchimport.newimport.structs.DiskBlockingQ;
import org.neo4j.batchimport.newimport.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.newimport.structs.DiskRecordsCache;
import org.neo4j.batchimport.newimport.structs.RunData;
import org.neo4j.batchimport.newimport.utils.Utils;

public class WriterWorker extends java.lang.Thread {
	int workerType;
	String threadName ;
	Method[] writerMethods;
	DiskBlockingQ diskRecordsQ;
	DiskRecordsCache diskRecCache;
	Stages stages;
	WriterStage writerStage;
	WriterWorker(int type, Method[] writerMethods, DiskRecordsCache diskRecCache,
			DiskBlockingQ diskRecordsQ, Stages stages, WriterStage writerStage){
		this.workerType = type;
		this.writerMethods = writerMethods;
		this.diskRecordsQ = diskRecordsQ;
		this.diskRecCache = diskRecCache;
		this.stages = stages;
		this.writerStage = writerStage;
	}
	public void run() {
		threadName = "Writer_"+Constants.RECORD_TYPE_NAME[workerType];
		Thread.currentThread().setName(threadName);
		this.setPriority(NORM_PRIORITY+2);
		while (!isDone()){
			try {
				DiskRecordsBuffer buf = diskRecordsQ.getBuffer(workerType);
				if (buf == null)
					continue;
				Object[] parameters = new Object[1];
				parameters[0] = buf;
				writerStage.getRunData(workerType).linesProcessed += buf.getRecordCount();
				try{
					writerMethods[workerType].invoke(stages.getStageMethods().writerStage, parameters);
				} catch (Exception e) {
					Utils.SystemOutPrintln("Writer Method Failed :" + e.getMessage());
				}			
				diskRecCache.putDiskRecords(buf, workerType);
			} catch (Exception e){
				Utils.SystemOutPrintln("Writer died:"+ threadName);
				break;
			}
		}
		//System.out.println("Exiting writer "+ threadName);
	}
	public boolean isDone(){
		if (stages.moreWork())	
			return false;
		return true;
	}
}
