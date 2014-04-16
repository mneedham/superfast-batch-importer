package org.neo4j.batchimport.importer.structs;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

public class DiskBlockingQ {
		private ArrayBlockingQueue<DiskRecordsBuffer>[] blockingQ;
		public DiskBlockingQ(int size, int capacity) {
			blockingQ = new ArrayBlockingQueue[size];
			for (int i = 0; i < size; i++)
				blockingQ[i] = new ArrayBlockingQueue<DiskRecordsBuffer>(capacity);
		}	
		public ArrayBlockingQueue<DiskRecordsBuffer> getQ(int index){
			return blockingQ[index];
		}
		public int getLength(int qIndex){
			return blockingQ[qIndex].size();
		}
	
		public DiskRecordsBuffer getBuffer(int qIndex) throws InterruptedException{
			DiskRecordsBuffer buf = blockingQ[qIndex].poll(500, TimeUnit.MILLISECONDS);
			if (buf != null)				
				buf.qIndex = qIndex;
			return buf;
		}
		public void putBuffer(int qIndex, DiskRecordsBuffer buf)throws InterruptedException{
			blockingQ[qIndex].put(buf);
		}

}
