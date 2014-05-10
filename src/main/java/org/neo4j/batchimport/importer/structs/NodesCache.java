package org.neo4j.batchimport.importer.structs;

import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;

public class NodesCache
{
    private final int MAX_DEGREE = 200;
    private ExtendableLongCache nodeCache = null;
    private long size = 0;
    private int denseNodeThreshold = -1;
    private long denseNodeCount = -1;
    private NeoStore neoStore;
    RelationshipGroupCache relGroupCache;

    public NodesCache( long nodeCount , NeoStore neoStore)
    {
        this.neoStore = neoStore;
        int incrementSize = nodeCount <= Integer.MAX_VALUE ? (int)nodeCount : Integer.MAX_VALUE;
        nodeCache = new ExtendableLongCache( incrementSize );
        size = nodeCount;
        nodeCache.fill( 0 );
        this.cleanIds( true /*aöö*/);
    }

    public long getSize()
    {
        return size;
    }

    public long put( long key, long value ) throws BatchImportException
    {
        long field = getField( key );
        long previousValue = IdFieldManipulator.getId( field );
        setField( key, IdFieldManipulator.setId( field, value ) );
        return previousValue;
    }

    private void setField( long key, long field )
    {
        nodeCache.put( key, field );
    }

    private long getField( long key ) throws BatchImportException
    {
        return nodeCache.get( key );
    }

    public long get( long key ) throws BatchImportException
    {
        return IdFieldManipulator.getId( getField( key ) );
    }

    int changeCount( long key, int diff ) throws BatchImportException
    {
        long field = IdFieldManipulator.changeCount( getField( key ), diff );
        int count = IdFieldManipulator.getCount( field );
        setField( key, field );
        return count;
    }

    public int incrementCount( long key ) throws BatchImportException
    {
        return changeCount( key, 1 );
    }

    public int decrementCount( long key ) throws BatchImportException
    {
        return changeCount( key, -1 );
    }

    public int getCount( long key ) throws BatchImportException
    {
        return IdFieldManipulator.getCount( getField( key ) );
    }

    public void cleanIds( boolean all )
    {
        for ( int i = 0; i < nodeCache.size(); i++ )
        {
            try
            {
                int degree = IdFieldManipulator.getCount( nodeCache.get( i ) );
                if ( all ||
                // sparse node
                        degree < denseNodeThreshold )
                {
                    nodeCache.put( i, IdFieldManipulator.cleanId( nodeCache.get( i ) ) );
                }
            }
            catch ( BatchImportException be )
            {
                // do nothing.
            }
        }
    }

    public int maxCount()
    {
        return IdFieldManipulator.MAX_COUNT;
    }

    public boolean nodeIsDense( long key ) throws BatchImportException
    {
        return getCount( key ) >= denseNodeThreshold;
    }

    public boolean checkAndSetVisited( long key ) throws BatchImportException
    {
        long field = getField( key );
        if ( IdFieldManipulator.isVisited( field ) )
        {
            return true;
        }
        setField( key, IdFieldManipulator.setVisited( field ) );
        return false;
    }

    public void calculateDenseNodeThreshold() throws BatchImportException
    {
        int numRelationshipTypes = Utils.safeCastLongToInt( neoStore.getRelationshipTypeStore().getHighId() );
        denseNodeThreshold = -1;
        denseNodeCount = 0;
        long[] numNodesOfDegree = new long[MAX_DEGREE + 1];
        for ( long i = 0; i < nodeCache.size(); i++ )
        {
            long degree = IdFieldManipulator.getCount( nodeCache.get( i ) );
            if ( degree <= MAX_DEGREE )
                numNodesOfDegree[(int) degree]++;
            else
                numNodesOfDegree[MAX_DEGREE]++;
        }
        long threshold = size / (RelationshipGroupCache.ARRAY_ROW_SIZE * numRelationshipTypes);
        denseNodeThreshold = MAX_DEGREE;
        denseNodeCount = numNodesOfDegree[MAX_DEGREE];
        while ( denseNodeCount <= threshold )
        {
            denseNodeCount += numNodesOfDegree[denseNodeThreshold--];
        }
        denseNodeThreshold += 1;
        System.out.println( " denseNodeCount[" + denseNodeCount + "] denseNodeThreshold[" + denseNodeThreshold
                + "] Percentage[" + (denseNodeCount * 100) / size + "]" );
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
    
    public void createRelationshipGroupCache(){
        int relTypeCount = Utils.safeCastLongToInt( neoStore.getRelationshipTypeStore().getHighId() );
        relGroupCache = new RelationshipGroupCache( denseNodeCount * relTypeCount, neoStore.getRelationshipGroupStore() );
    }
    
    public RelationshipGroupCache getRelationshipGroupCache()
    {
        return relGroupCache;
    }
    
}
