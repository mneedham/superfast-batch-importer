package org.neo4j.batchimport.importer.structs;

import java.util.Arrays;

public class NodesCache
{
    private static final long LEFTOVER_BIT_MASK = 0xFFFFFFF8_00000000L;
    private long[][] nodeCache = null;
    private int numCache = 0;
    private long size = 0;

    public NodesCache( long nodeCount )
    {
        numCache = nodeCount <= Integer.MAX_VALUE ? 1 : (int) ((nodeCount / Integer.MAX_VALUE) + 1);
        nodeCache = new long[numCache][];
        nodeCache[0] = new long[(int) (nodeCount % Integer.MAX_VALUE)];
        for ( int i = 1; i < numCache; i++ )
        {
            nodeCache[i] = new long[Integer.MAX_VALUE];
        }
        size = nodeCount;
        this.clean();
    }

    private int getCacheIndex( long id )
    {
        return (int) ((id / Integer.MAX_VALUE));
    }

    private int getIndex( long id )
    {
        return (int) id % Integer.MAX_VALUE;
    }

    public long getSize()
    {
        return size;
    }

    public void put( long key, long value )
    {
        this.put( key, value, false );
    }

    public void put( long key, long value, boolean keepCount )
    {
        if ( keepCount )
        {
            long count = nodeCache[getCacheIndex( key )][getIndex( key )] & LEFTOVER_BIT_MASK;
            nodeCache[getCacheIndex( key )][getIndex( key )] = count & value;
        }
        else
            nodeCache[getCacheIndex( key )][getIndex( key )] = value;
    }

    public long get( long key )
    {
        int i = getCacheIndex( key ), j = getIndex( key );
        long result = nodeCache[i][j];
        return result == -1 ? -1 : result & ~LEFTOVER_BIT_MASK;
    }

    public void extend( long newSize )
    {
        if ( size >= newSize )
        {
            return;
        }
        ////to do
    }

    private int changeCount( long key, int value )
    {
        long count = nodeCache[getCacheIndex( key )][getIndex( key )] & 0xFFFFFFF800000000L;
        long id = nodeCache[getCacheIndex( key )][getIndex( key )] & 0x00000007FFFFFFFFL;
        count = count >> 35;
        count += value;
        nodeCache[getCacheIndex( key )][getIndex( key )] = count << 35 & id;
        return (int) count;
    }

    public int incrementCount( long key )
    {
        return changeCount( key, 1 );
    }

    public int decrementCount( long key )
    {
        return changeCount( key, -1 );
    }

    public int getCount( long key )
    {
        long count = nodeCache[getCacheIndex( key )][getIndex( key )] & 0xFFFFFFF800000000L;
        count = count >> 35;
        return (int) count;
    }
    public void clean()
    {
        for ( int i = 0; i < numCache; i++ )
        {
            Arrays.fill( nodeCache[i], -1 );
        }
    }

}
