package org.neo4j.batchimport.newimport.stages;

import java.lang.reflect.Method;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.neo4j.batchimport.newimport.structs.CSVDataBuffer;
import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.batchimport.newimport.structs.Constants.ImportStageState;
import org.neo4j.batchimport.newimport.structs.Constants.ThreadState;
import org.neo4j.batchimport.newimport.structs.Constants.ThreadStateTransition;
import org.neo4j.batchimport.newimport.structs.DataBufferBlockingQ;
import org.neo4j.batchimport.newimport.structs.DiskRecordsCache;
import org.neo4j.batchimport.newimport.structs.RunData;
import org.neo4j.batchimport.newimport.utils.Utils;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

public class Stages {
	private DataBufferBlockingQ<CSVDataBuffer> bufferQ;
	private int threadCount;
	private int activeThreadCount = 0;
	private ThreadState[] threadState;
	private RunData[][] stageRunData;
	private boolean[] stageComplete;
	private int numStages;
	private ImportWorker[] importWorkers;
	private Method[] importStageMethods;
	private CSVDataBuffer[] bufArray;
	private boolean moreWork = true;
	private ReadFileData currentInput = null;
	private long startImport = System.currentTimeMillis();
	protected CountDownLatch startStagesSyncPoint = new CountDownLatch(1);
	private StageMethods stageMethods;
	private int currentMode = -1;
	private WriterStage writerStage;
	
	public Stages(StageMethods stageMethods){
		threadCount = Runtime.getRuntime().availableProcessors();		
		importWorkers = new ImportWorker[threadCount];
		threadState = new ThreadState[threadCount];
		this.stageMethods = stageMethods;	
	}

	public void init(int mode, Method... methods){
		currentMode = mode;
		this.numStages = methods.length;
		stageRunData = new RunData[numStages][threadCount];
		for (int i = 0; i < numStages; i++)
			for (int j = 0; j < threadCount; j++)
				stageRunData[i][j] = new RunData(i+"_"+j);
		stageComplete = new boolean[numStages];
		bufferQ = new DataBufferBlockingQ<CSVDataBuffer>(numStages, Constants.BUFFERQ_SIZE, threadCount);
		initInputQ(bufferQ.getQ(0), bufArray);
		importStageMethods = new Method[methods.length];
		for (int i = 0; i <methods.length; i++)
			importStageMethods[i] = methods[i];	
		for (int i = 0; i < threadCount; i++)
			if (importWorkers[i] != null)
				importWorkers[i].setImportWorkers(importStageMethods);
	}
	public void setSingleThreaded(boolean... type){
		for (int i = 0; i < type.length; i++)
			bufferQ.setSingleThreaded(i, type[i]);
	}
	public ThreadState getThreadState(int threadIndex){
		return threadState[threadIndex];
	}
	public void setThreadState(int threadIndex, ThreadState state){
		threadState[threadIndex] = state;
	}
	public void setDataBuffers(DiskRecordsCache diskCache){
		bufArray = createInputBufferArray(Constants.BUFFERQ_SIZE, diskCache);
	}
	public boolean moreWork(){
		return moreWork;
	}
	public void registerWriterStage(WriterStage writerStage){
		this.writerStage = writerStage;
	}
	public void start(ReadFileData input){
		for (int i = 0; i < stageComplete.length; i++)
			stageComplete[i] = false;
		currentInput = input;
		setDataInput(currentInput, bufArray);
		startWorkers();
		bufferQ.reset();
		assignThreadsToStages();
		startStagesSyncPoint.countDown();
		startStagesSyncPoint = new CountDownLatch(1);
			
	}
	private void startWorkers(){
		for (int i = 0; i < threadCount; i++){
			if (importWorkers[i] == null){
				importWorkers[i] = new ImportWorker(currentInput, i, this);
				importWorkers[i].setImportWorkers(importStageMethods);
				importWorkers[i].start();
				try {
					Thread.sleep(200);
				} catch (Exception e){};
			} else {
				importWorkers[i].setInput(currentInput);
				importWorkers[i].setImportWorkers(importStageMethods);
			}
		}
	}

	private void assignThreadsToStages(){
		int threadIndex = 0, stageIndex = 0;
		//one thread to each stage
		for (int i = 0; i < numStages; i++){
			importWorkers[threadIndex].setStageIndex(i, true);
			threadState[threadIndex] = ThreadState.Pinned;
			threadIndex++;
		}
		//remaining up to maxThreads/2, pin to multithreaded stages
		for (; threadIndex < this.threadCount/2;stageIndex++)
			if (!bufferQ.isSingleThreaded(stageIndex % numStages)){
				importWorkers[threadIndex].setStageIndex(stageIndex % numStages, true);
				threadState[threadIndex] = ThreadState.Pinned;
				threadIndex++;
			}
	}
	public ImportStageState stop(){
		moreWork = false;
		startStagesSyncPoint.countDown();
		return ImportStageState.Exited;
	}
	public DataBufferBlockingQ<CSVDataBuffer> getBufferQ(){
		return bufferQ;
	}
	public synchronized int upThreadCount(){
		return(activeThreadCount++);
	}
	public synchronized int downThreadCount(){
		return(--activeThreadCount);
	}
	public int getThreadCount(){
		return threadCount;
	}
	public RunData getStageRunData(int index1, int index2){
		return stageRunData[index1][index2];
	}
	public void setStageComplete(int stageIndex, boolean value){
		stageComplete[stageIndex] = value;
	}
	public boolean isStageComplete(int stageIndex){
		return stageComplete[stageIndex];
	}
	public int numStages(){
		return numStages;
	}
	public StageMethods getStageMethods(){
		return stageMethods;
	}
	public void threadStateTransition(int threadIndex, ThreadStateTransition transition){
		if (transition ==  ThreadStateTransition.StartProcessBuffer){
			if (threadState[threadIndex] == ThreadState.PinnedWaitInputQ ||
					threadState[threadIndex] == ThreadState.Pinned)
				threadState[threadIndex] = ThreadState.PinnedProceessing;
			else
				threadState[threadIndex] = ThreadState.FloatProcessing;
		} else if (transition ==  ThreadStateTransition.EndProcessBuffer){
			if (threadState[threadIndex] == ThreadState.PinnedProceessing ||
					threadState[threadIndex] == ThreadState.Pinned)
				threadState[threadIndex] = ThreadState.PinnedWaitInputQ;
			else
				threadState[threadIndex] = ThreadState.FloatWaitInputQ;
		}
				
	}
	public Object getMethods(){
		if (currentMode == Constants.NODE)
			return stageMethods.importNode;
		if (currentMode == Constants.RELATIONSHIP)
			return stageMethods.importRelationship;
		return null;
	}
	private void setDataInput(ReadFileData input, CSVDataBuffer[] bufferArray){
    	for (int i = 0; i < bufferArray.length; i++)
    		bufferArray[i].initRecords(input.getHeaderLength());
    }
	private  CSVDataBuffer[] createInputBufferArray(int size, DiskRecordsCache diskCache){
    	CSVDataBuffer[] bufferArray = new CSVDataBuffer[size];
    	for (int i = 0; i < size; i++){
    		CSVDataBuffer buf = new CSVDataBuffer(Constants.BUFFER_ENTRIES, 
    							Constants.BUFFER_SIZE_BYTES, i, diskCache);
    		bufferArray[i] = buf;
    	};
    	return bufferArray;
    }
	 private void initInputQ(ArrayBlockingQueue<CSVDataBuffer> inputQ, CSVDataBuffer[] bufArray){
	    	int size = inputQ.remainingCapacity();
	    	try {
	        	for (int i = 0; i < size; i++)
	        		inputQ.put( bufArray[i]);
	        	//System.out.println("EmptyQ Size:"+ inputQ.size());
	        } catch (InterruptedException ie){
	        	Utils.SystemOutPrintln("Interruped empty queue creation");
	        } 
	    }
	 public boolean isComplete(){
		 for (boolean status: stageComplete)
			 if (!status)
				 return false;
		 return true;
	 }
	 public void pollResults(BatchInserterImpl batchInserter){
		 long startTime = System.currentTimeMillis();
		 long waitCount = 0, billion = 1;
		 while (true){
			 try {
				 Thread.sleep(500);
				 waitCount += 500;
				 if (Constants.printPollInterval != -1){
					 if (waitCount % Constants.printPollInterval == 0)
						 printRunData( Constants.detailedData, this.numStages, startTime, batchInserter);
				 } 
				 if (waitCount % Constants.progressPollInterval == 0)
						 System.out.print("In progress: ["+waitCount/1000+"] "+Utils.getMaxIds(batchInserter.getNeoStore())+'\r');

				 if (Utils.getTotalIds(batchInserter.getNeoStore()) > billion*1000000000){
					 // for strange reason, process abort for lack of memory. 
					 // This is a forced gc every billion elements to avoid out-of-memory failures.
					 billion++;
					 System.gc();
				 }
				 if (isComplete())
					 return;
			 } catch (Exception e){
				 Utils.SystemOutPrintln(e.getMessage()+" Writer exception");
			 }
		 }
	 }
	 public void printRunData(boolean detailed, int stages, long startTime,BatchInserterImpl batchInserter){
		 long curTime = System.currentTimeMillis();		
		 long timeTaken = (curTime-startTime);
		 System.out.println("At time ["+(curTime-startImport)+" ms]["+timeTaken+" ms]");
		 System.out.println("\t"+Utils.memoryStats());
		 StringBuilder str = new StringBuilder();
		 str.append("Q Stats");
		 for (int i = 0; i < bufferQ.getLength();i++)
			 str.append("["+bufferQ.getQ(i).size()+":"+bufferQ.getThreadCount(i)+"]");
		 str.append("][");	
		 int[] threadAssigment = bufferQ.getThreadAssignment();
		 for (int i =0; i < threadCount; i++)
			 str.append("("+threadAssigment[i]+":"+threadState[i]+")");
		 str.append("]");		
		 System.out.println("\t"+str);
		 str.setLength(0);
		 str.append("StageStats["+timeTaken+" ms]");
		 for (int j = 0; j < stages; j++){
			 long linesProcessed = 0; 
			 long snapRate = 1, rate = 1;;
			 if (stageComplete[j])
				 continue;		
			 for (int i = 0; i < stageRunData[j].length && stageRunData[j][i] != null; i++){
				 linesProcessed += stageRunData[j][i].linesProcessed;
				 rate += stageRunData[j][i].totalRate();
				 snapRate += stageRunData[j][i].snapshotRate();
			 }
			 String type = "M";
			 if (bufferQ.isSingleThreaded(j))
				 type = "S";
			 str.append("["+type+":"+linesProcessed+":"+rate+":"+snapRate+"]");
			 if (detailed)
				 for (int i = 0; i < stageRunData[j].length && stageRunData[j][i] != null; i++)
					 stageRunData[j][i].printData(0);
		 };
		 System.out.println("\t"+str);
		 str.setLength(0);
		 str.append("Writers:");
		 for (int i = 0; i < writerStage.numWriters; i++){
			 str.append("["+Constants.RECORD_TYPE_NAME[i]+":"+writerStage.diskBlockingQ.getLength(i)+":"+
					 		writerStage.getRunData(i).linesProcessed+"]");
		 }
		 System.out.println("\t"+str);
		 str.setLength(0);
		 System.out.println("\t"+"MaxIds:"+Utils.getMaxIds(batchInserter.getNeoStore()));
	 }
}
