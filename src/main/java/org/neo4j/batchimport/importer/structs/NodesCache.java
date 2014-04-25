package org.neo4j.batchimport.importer.structs;


public class NodesCache
{
    private long[][] nodeCache = null;
    private int numCache = 0;
    private long size = 0;
    private int denseNodeThreshold = -1;
    private long denseNodeCount = -1;

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
        this.cleanIds( true /*aöö*/ );
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

    public long put( long key, long value )
    {
        long field = getField( key );
        long previousValue = IdFieldManipulator.getId( field );
        setField( key, IdFieldManipulator.setId( field, value ) );
        return previousValue;
    }

    private void setField( long key, long field )
    {
        long[] cache = nodeCache[getCacheIndex( key )];
        int index = getIndex( key );
        cache[index] = field;
    }

    private long getField( long key )
    {
        long[] cache = nodeCache[getCacheIndex( key )];
        int index = getIndex( key );
        return cache[index];
    }

    public long get( long key )
    {
        return IdFieldManipulator.getId( getField( key ) );
    }

    int changeCount( long key, int diff )
    {
        long field = IdFieldManipulator.changeCount( getField( key ), diff );
        int count = IdFieldManipulator.getCount( field );
        setField( key, field );
        return count;
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
        return IdFieldManipulator.getCount( getField( key ) );
    }

    public void cleanIds( boolean all )
    {
        for ( int i = 0; i < numCache; i++ )
        {
            for ( int j = 0; j < nodeCache[i].length; j++ )
            {
                if ( all ||
                        // sparse node
                        IdFieldManipulator.getCount( nodeCache[i][j] ) < denseNodeThreshold )
                {
                    nodeCache[i][j] = IdFieldManipulator.cleanId( nodeCache[i][j] );
                }
            }
        }
    }

    public int maxCount()
    {
        return IdFieldManipulator.MAX_COUNT;
    }

    public boolean nodeIsDense( long key )
    {
        return getCount( key ) >= denseNodeThreshold;
    }

    public boolean checkAndSetVisited( long key )
    {
        long field = getField( key );
        if ( IdFieldManipulator.isVisited( field ) )
        {
            return true;
        }
        setField( key, IdFieldManipulator.setVisited( field ) );
        return false;
    }

    public void calculateDenseNodeThreshold( double percent )
    {
        denseNodeThreshold = 15; // obviously hard coded and bad

        denseNodeCount = 0;
        for ( long[] chard : nodeCache )
        {
            for ( long field : chard )
            {
                long count = IdFieldManipulator.getCount( field );
                if ( count >= denseNodeThreshold )
                {
                    denseNodeCount++;
                }
            }
        }
    }

    public int getDenseNodeThreshold()
    {
        if ( denseNodeThreshold == -1 )
        {
            throw new IllegalStateException( "Not calculated yet" );
        }
        return denseNodeThreshold;
    }

    public long getDenseNodeCount()
    {
        if ( denseNodeCount == -1 )
        {
            throw new IllegalStateException( "Not calculated yet" );
        }
        return denseNodeCount;
    }
}
