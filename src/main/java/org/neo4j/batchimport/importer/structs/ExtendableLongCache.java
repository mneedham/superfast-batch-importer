package org.neo4j.batchimport.importer.structs;

import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.kernel.api.Exceptions.BatchImportException;

public class ExtendableLongCache
{
    protected int blockSize;
    protected ArrayList<long[]> cache;
    public static final long EMPTY = -1;

    public ExtendableLongCache( int incrementSize )
    {
        blockSize = incrementSize;
        cache = new ArrayList<long[]>();
        addBlock();
    }

    public long size()
    {
        return (cache.size() * blockSize);
    }

    protected void addBlock()
    {
        long[] block = new long[blockSize];
        Arrays.fill( block, EMPTY );
        cache.add( block );
    }

    public long get( long index ) throws BatchImportException
    {
        int blockIndex = Utils.safeCastLongToInt( index / blockSize );
        int insideIndex = Utils.safeCastLongToInt( index % blockSize );
        if ( blockIndex > (cache.size() - 1) )
            throw new BatchImportException( "Index out of bounds:" + index );
        return cache.get( blockIndex )[insideIndex];
    }

    public void put( long index, long value )
    {
        int blockIndex = Utils.safeCastLongToInt( index / blockSize );
        int insideIndex = Utils.safeCastLongToInt( index % blockSize );
        existsBlock(blockIndex); 
        cache.get( blockIndex )[insideIndex] = value;
    }

    public void fill( long value )
    {
        for ( long[] block : cache )
        {
            Arrays.fill( block, value );
        }
    }

    public void fill( long from, long to, long value )
    {
        int fromBlockIndex = Utils.safeCastLongToInt( from / blockSize );
        int insideFromIndex = Utils.safeCastLongToInt( from % blockSize );
        int toBlockIndex = Utils.safeCastLongToInt( to / blockSize );
        int insideToIndex = Utils.safeCastLongToInt( to % blockSize );
        for ( int i = fromBlockIndex; i <= toBlockIndex; i++ )
        {
            int start = 0, end = blockSize - 1;
            if ( i == fromBlockIndex )
                start = insideFromIndex;
            if ( i == toBlockIndex )
                end = insideToIndex;
            Arrays.fill( cache.get( i ), start, end, value );
        }
    }

    public void copy( long from, long to, int length )
    {
        int fromBlockIndex = Utils.safeCastLongToInt( from / blockSize );
        int insideFromIndex = Utils.safeCastLongToInt( from % blockSize );
        int toBlockIndex = Utils.safeCastLongToInt( to / blockSize );
        existsBlock(toBlockIndex);
        int insideToIndex = Utils.safeCastLongToInt( to % blockSize );
        System.arraycopy( cache.get( fromBlockIndex ), insideFromIndex, cache.get( toBlockIndex ), insideToIndex,
                length );
    }
    
    private void existsBlock(int blockIndex){
        if ( blockIndex > (cache.size() - 1) )
            addBlock();
    }
}
