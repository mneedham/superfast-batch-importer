package org.neo4j.batchimport.importer.structs;

import java.util.Arrays;

public class NodesCache
{
    public static final int COUNT_BITS = 28;
    public static final int MAX_COUNT = (1 << COUNT_BITS) - 1;
    public static final int ID_BITS = 64-(COUNT_BITS+1);
    private static final long COUNT_BIT_MASK = 0x7FFFFFF8_00000000L;
    private static final long VISITED_BIT_MASK = 0x80000000_00000000L;
    private static final long ID_BIT_MASK = ~(COUNT_BIT_MASK | VISITED_BIT_MASK);
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
        long count = nodeCache[getCacheIndex( key )][getIndex( key )] & COUNT_BIT_MASK;
        nodeCache[getCacheIndex( key )][getIndex( key )] = count | value;
    }

    public long get( long key )
    {
        int i = getCacheIndex( key ), j = getIndex( key );
        long result = nodeCache[i][j] & ID_BIT_MASK;
        return result == ID_BIT_MASK ? -1 : result;
    }

    int changeCount( long key, int value )
    {
        long count = nodeCache[getCacheIndex( key )][getIndex( key )] & COUNT_BIT_MASK;
        long otherBits = nodeCache[getCacheIndex( key )][getIndex( key )] & ~COUNT_BIT_MASK;
        count = count >>> ID_BITS;
        count += value;
        if ( count < 0 || count > MAX_COUNT )
        {
            throw new IllegalStateException( "tried to decrement counter below zero." );
        }
        nodeCache[getCacheIndex( key )][getIndex( key )] = (count << ID_BITS) | otherBits;
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
        long count = nodeCache[getCacheIndex( key )][getIndex( key )] & COUNT_BIT_MASK;
        count = count >>> ID_BITS;
        return (int) count;
    }

    public void clean()
    {
        for ( int i = 0; i < numCache; i++ )
        {
            Arrays.fill( nodeCache[i], ID_BIT_MASK );
        }
    }

    public int maxCount()
    {
        return MAX_COUNT;
    }

    public boolean checkAndSetVisited( long nodeId )
    {
        if ( (nodeCache[getCacheIndex( nodeId )][getIndex( nodeId )] & VISITED_BIT_MASK) != 0 )
        {
            return true;
        }
        nodeCache[getCacheIndex( nodeId )][getIndex( nodeId )] |= VISITED_BIT_MASK;
        return false;
    }
}
