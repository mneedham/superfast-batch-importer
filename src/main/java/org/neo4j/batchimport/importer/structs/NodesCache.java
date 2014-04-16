package org.neo4j.batchimport.importer.structs;

import java.util.Arrays;

public class NodesCache {
	private long[][] nodeCache = null;
	private int numCache = 0;
	private long size = 0;
	public NodesCache(long nodeCount){
		numCache = nodeCount <= Integer.MAX_VALUE ? 1 : (int)((nodeCount/Integer.MAX_VALUE)+1);
		nodeCache = new long[numCache][];
		nodeCache[0] = new long[(int)(nodeCount % Integer.MAX_VALUE)];
		for (int i = 1; i < numCache; i++)
			nodeCache[i] = new long[Integer.MAX_VALUE];
		size = nodeCount;
		this.clean();
	}
	private int getCacheIndex(long id){
		return (int)((id/Integer.MAX_VALUE));
	}
	private int getIndex(long id){
		return (int)id % Integer.MAX_VALUE;
	}
	public long getSize(){
		return size;
	}
	public void put(long key, long value){
		nodeCache[getCacheIndex(key)][getIndex(key)] = value;
	}
	public long get(long key){
		int i = getCacheIndex(key), j = getIndex(key);
		return nodeCache[i][j];
	}
	public void extend(long newSize){
		if (size >= newSize)
			return;
		////to do
	}
	public void clean(){
		for (int i = 0; i < numCache; i++)
			Arrays.fill(nodeCache[i], -1);
	}

}
