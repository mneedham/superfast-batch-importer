package org.neo4j.batchimport.importer.structs;

import java.util.Arrays;

import org.neo4j.graphdb.Direction;
import org.neo4j.helpers.Factory;

public class RelationshipGroupCache
{
    /*
     * [0] next
     * [1] id
     * [2] type
     * [3] out
     * [4] in
     */

    private static final int INDEX_NEXT = 0;
    private static final int INDEX_ID = 1;
    private static final int INDEX_TYPE = 2;
    private static final int INDEX_OUT = 3;
    private static final int INDEX_IN = 4;

    public static final int ARRAY_ROW_SIZE = 5;
    public static final int EMPTY = -1;
    private final Factory<Long> relationshipGroupIdAssigner;
    private long[] cache;
    private int nextFreeId = 0;

    public RelationshipGroupCache( long nodeCount, Factory<Long> relationshipGroupIdAssigner )
    {
        this.relationshipGroupIdAssigner = relationshipGroupIdAssigner;
        cache = new long[(int) (nodeCount * ARRAY_ROW_SIZE)];
        Arrays.fill( cache, EMPTY );
    }

    public long allocate( int type, Direction direction, long relId )
    {
        long logicalPosition = nextFreeId();
        initializeGroup( logicalPosition, type );
        set( logicalPosition, inOrOutIndex( direction ), relId );
        return logicalPosition;
    }

    public long put( long relGroupCachePosition, int type, Direction direction, long relId )
    {
        long logicalPosition = relGroupCachePosition;
        long previousPosition = EMPTY;
        while ( !empty( logicalPosition ) && get( logicalPosition, INDEX_TYPE ) != type )
        {
            previousPosition = logicalPosition;
            logicalPosition = get( logicalPosition, INDEX_NEXT );
        }

        if ( empty( logicalPosition ) )
        {
            logicalPosition = nextFreeId();
            initializeGroup( logicalPosition, type );
        }

        if ( !empty( previousPosition ) )
        {
            set( previousPosition, INDEX_NEXT, logicalPosition );
        }

        return set( logicalPosition, inOrOutIndex( direction ), relId );
    }

    private int nextFreeId()
    {
        return nextFreeId++;
    }

    private int inOrOutIndex( Direction direction )
    {
        switch ( direction )
        {
            case OUTGOING:
                return INDEX_OUT;
            case INCOMING:
                return INDEX_IN;
            default:
                throw new UnsupportedOperationException( direction.name() );
        }
    }

    private void initializeGroup( long relGroupCachePosition, int type )
    {
        cache[physicalIndex( relGroupCachePosition, INDEX_TYPE )] = type;
        cache[physicalIndex( relGroupCachePosition, INDEX_ID )] = relationshipGroupIdAssigner.newInstance();
    }

    private long set( long relGroupCachePosition, int index, long newValue )
    {
        int physicalIndex = physicalIndex( relGroupCachePosition, index );
        long previousValue = cache[physicalIndex];
        cache[physicalIndex] = newValue;
        return previousValue;
    }

    private long get( long relGroupCachePosition, int index )
    {
        int physicalIndex = physicalIndex( relGroupCachePosition, index );
        return cache[physicalIndex];
    }

    private int physicalIndex( long relGroupCachePosition, int index )
    {
        return (int) (relGroupCachePosition * ARRAY_ROW_SIZE + index);
    }

    private boolean empty( long value )
    {
        return value == EMPTY;
    }
}
