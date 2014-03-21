package org.neo4j.batchimport.newimport.structs;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.batchimport.newimport.stages.ImportWorker;

public class DataBufferBlockingQ<DataBufferType>{
		private ArrayBlockingQueue<DataBufferType>[] blockingQ;
		private boolean[] isSingleThreaded;
		private int[] threadCount;
		private int[] threadAssignment;
		private int maxThreads;
		private boolean[] lastBuffer;
		private int[] bufOrder = null;
		private int curSize;
		public DataBufferBlockingQ(int size, int capacity, int maxThreads) {
			blockingQ = new ArrayBlockingQueue[size];
			isSingleThreaded = new boolean[size];
			lastBuffer = new boolean[size];
			threadCount = new int[size];
			bufOrder = new int[size];
			curSize = size;
			for (int i = 0; i < size; i++){
				blockingQ[i] = new ArrayBlockingQueue<DataBufferType>(capacity);
				isSingleThreaded[i] = false;
				threadCount[i] = 0;
				bufOrder[i] = -1;
				lastBuffer[i] = false;
			}
			this.maxThreads = maxThreads;
			threadAssignment = new int[maxThreads];
			for (int i = 0; i < maxThreads; i++)
				threadAssignment[i] = -1;
		}
		public int[] getThreadAssignment(){
			return threadAssignment;
		}
		public int getThreadCount(int stage){
			return threadCount[stage];
		}
		public boolean isSingleThreaded(int index){
			return isSingleThreaded[index];
		}
		public void setSingleThreaded(int index, boolean type){
			isSingleThreaded[index] = type;
		}
		public void reset(){
			for (int i = 0; i < curSize; i++){
				threadCount[i] = 0;
				bufOrder[i] = -1;
				lastBuffer[i] = false;
			}
			for (int i = 0; i < maxThreads; i++)
				threadAssignment[i] = -1;
		}
		public ArrayBlockingQueue<DataBufferType> getQ(int index){
			return blockingQ[index];
		}
		public int getLength(){
			return blockingQ.length;
		}
		private synchronized int countDown(int qIndex){
			threadCount[qIndex]--;
			return threadCount[qIndex];
		}
		private synchronized boolean countUp(int qIndex, int max){
			if (max != -1 && this.threadCount[qIndex] >= max)
				return false;		
			this.threadCount[qIndex]++;
			return true;
		}

		public boolean sequencedBuffers(int qIndex){
			if (qIndex < 1)
				return false;
			if (isSingleThreaded(qIndex) && !isSingleThreaded(qIndex-1))
				return true;
			return false;
		}
		
		private DataBufferType getBufferSingle(int qIndex) throws InterruptedException{
			DataBufferType buffer = null;
			ArrayBlockingQueue<DataBufferType> bufferQ = blockingQ[qIndex];
		ImportWorker.threadImportWorker.get().setCurrentMethod();
			while (buffer == null && !lastBuffer[qIndex]){
				if (sequencedBuffers(qIndex)){
					if (bufferQ.peek() != null){
						if (((AbstractDataBuffer)bufferQ.peek()).getBufSequenceId() == bufOrder[qIndex]+1){
							buffer = bufferQ.take();
							if (buffer != null)
								bufOrder[qIndex] = ((AbstractDataBuffer)buffer).getBufSequenceId();
							break;
						} else {
							Iterator<DataBufferType> it = bufferQ.iterator();
							while (it.hasNext()){
								buffer = (DataBufferType)it.next();
								if (((AbstractDataBuffer)buffer).getBufSequenceId() == bufOrder[qIndex]+1){
									bufferQ.remove(buffer);
									bufOrder[qIndex] = ((AbstractDataBuffer)buffer).getBufSequenceId();
									break;
								}
								buffer = null;
							}
						}
					}
					if (buffer == null)
						Thread.sleep(100);
				} else
					buffer = blockingQ[qIndex].poll(100, TimeUnit.MILLISECONDS);								
			}
			return buffer;
		}
		private  int findQMax(){
			int max = 0;
			int index = -1;
			for (int i = blockingQ.length-1; i >= 0 ; i--){
				if (isSingleThreaded[i] || lastBuffer[i])
					continue;
				if (blockingQ[i].size() >= max){
					max = blockingQ[i].size();
					index = i;
				}
			}
			return index;
		}
		private DataBufferType count(DataBufferType buf, int qIndex, ImportWorker worker){
			if (buf == null)
				return buf;
			((AbstractDataBuffer)buf).setStageIndex(qIndex);
			threadAssignment[worker.getThreadIndex()] = qIndex;
			countUp(qIndex, -1);
			return buf;
		}
		public DataBufferType getBuffer(int curQ, ImportWorker worker) throws InterruptedException{
			int qIndex = -1;
			DataBufferType buffer = null;
			if (worker.isPinned()){
				qIndex = worker.getStageIndex();
				if (isSingleThreaded[qIndex])
					return count(getBufferSingle(qIndex), qIndex, worker);
			ImportWorker.threadImportWorker.get().setCurrentMethod();
				while (buffer == null && !lastBuffer[qIndex])
					buffer = blockingQ[qIndex].poll(100, TimeUnit.MILLISECONDS);
			if (buffer == null)
			ImportWorker.threadImportWorker.get().setCurrentMethod();
			else
				ImportWorker.threadImportWorker.get().setCurrentMethod(" "+((AbstractDataBuffer)buffer).getBufSequenceId());
				return count(buffer, qIndex, worker);
			}
			qIndex = findQMax();			
			if (qIndex == -1)
				return null;
			buffer = blockingQ[qIndex].poll(500, TimeUnit.MILLISECONDS);
			return count(buffer, qIndex, worker);
		}
		public DataBufferType putBuffer(DataBufferType buf)throws InterruptedException{		
			int curQ = ((AbstractDataBuffer)buf).getStageIndex();
			int nextQ = (((AbstractDataBuffer)buf).getStageIndex()+1) % blockingQ.length;
			if (!((AbstractDataBuffer)buf).moreData())
				lastBuffer[curQ] = true;
			if (((AbstractDataBuffer)buf).getCurEntries() > 0){
				blockingQ[nextQ].put(buf);
			}
			else
				blockingQ[curQ].put(buf);
			countDown(curQ);
			return null;
		}
}
