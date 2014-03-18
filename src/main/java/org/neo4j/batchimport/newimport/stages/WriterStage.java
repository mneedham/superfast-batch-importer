package org.neo4j.batchimport.newimport.stages;

import java.lang.reflect.Method;

import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.batchimport.newimport.structs.DiskBlockingQ;
import org.neo4j.batchimport.newimport.structs.DiskRecordsCache;
import org.neo4j.batchimport.newimport.structs.RunData;

public class WriterStage {
	int numWriters = 0;
	DiskRecordsCache diskRecCache;
	DiskBlockingQ diskBlockingQ;
	WriterWorker[] writerWorker = null;
	Method[] writerMethods = null;
	RunData[] writerRunData;
	Stages stages;
	public WriterStage(Stages stages){
		this.stages = stages;
    	diskRecCache = new DiskRecordsCache(Constants.BUFFERQ_SIZE*2, Constants.BUFFER_ENTRIES);
    	diskBlockingQ = new DiskBlockingQ(3, Constants.BUFFERQ_SIZE);
    	stages.registerWriterStage(this);
	}
	public void init(Method... methods){
		this.numWriters = methods.length;
		 writerWorker = new WriterWorker[numWriters];
		 writerMethods = new Method[numWriters];
		 writerRunData = new RunData[numWriters];
		 for (int i = 0; i < methods.length; i++)
			 writerMethods[i] = methods[i];
		 for (int i = 0; i < numWriters; i++){
			 writerWorker[i] = new WriterWorker(i, writerMethods, diskRecCache, diskBlockingQ, stages, this);
			 writerRunData[i] = new RunData("Writer"+i);
		 }
	}
	public void start(){
		for (int i = 0; i < numWriters; i++)
			 writerWorker[i].start();
	}
	public void stop(){
		for (int i = 0; i < numWriters; i++)
		 writerWorker[i].stop();
	}
	public DiskRecordsCache getDiskRecordsCache(){
		return diskRecCache;
	}
	public DiskBlockingQ getDiskBlockingQ(){
		return diskBlockingQ;
	}
	public RunData getRunData(int index){
		return writerRunData[index];
	}

}
