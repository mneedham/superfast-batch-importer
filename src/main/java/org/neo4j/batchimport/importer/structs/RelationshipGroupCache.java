package org.neo4j.batchimport.importer.structs;

import java.util.Arrays;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;

import static java.lang.String.valueOf;

public class RelationshipGroupCache
{
    private static final int INDEX_NEXT = 0;
    private static final int INDEX_ID = 1;
    private static final int INDEX_TYPE = 2;
    private static final int INDEX_OUT = 3;
    private static final int INDEX_IN = 4;

    public static final int ARRAY_ROW_SIZE = 5;
    public static final int EMPTY = -1;
    private final IdSequence relationshipGroupIdAssigner;
    private final long[] cache;
    private int nextFreeId = 0;

    public RelationshipGroupCache( long denseNodeCount, IdSequence relationshipGroupIdAssigner )
    {
        this.relationshipGroupIdAssigner = relationshipGroupIdAssigner;
        cache = new long[(int) (denseNodeCount * ARRAY_ROW_SIZE)];
        System.out.println( "Rel group cache array initialized to " + denseNodeCount + "*" + ARRAY_ROW_SIZE +
                "=" + cache.length );
        Arrays.fill( cache, EMPTY );
    }

    public long allocate( int type, Direction direction, long relId )
    {
        long logicalPosition = nextFreeId();
        initializeGroup( logicalPosition, type );
        setIdField( logicalPosition, inOrOutIndex( direction ), relId, true );
        return logicalPosition;
    }

    public long put( long relGroupCachePosition, int type, Direction direction, long relId,
            boolean trueForIncrement )
    {
        long currentPosition = relGroupCachePosition;
        long previousPosition = EMPTY;
        while ( !empty( currentPosition ) )
        {
            long foundType = getField( currentPosition, INDEX_TYPE );
            if ( foundType == type )
            {   // Found it
                return setIdField( currentPosition, inOrOutIndex( direction ), relId, trueForIncrement );
            }
            else if ( foundType > type )
            {   // We came too far, create room for it
                break;
            }
            previousPosition = currentPosition;
            currentPosition = getField( currentPosition, INDEX_NEXT );
        }

        long newPosition = nextFreeId();
        if ( empty( previousPosition ) )
        {   // We are at the start
            move( safeCastLongToInt( currentPosition ), safeCastLongToInt( newPosition ) );
            long swap = newPosition;
            newPosition = currentPosition;
            currentPosition = swap;
        }

        initializeGroup( newPosition, type );
        if ( !empty( currentPosition ) )
        {   // We are NOT at the end
            setField( newPosition, INDEX_NEXT, currentPosition );
        }

        if ( !empty( previousPosition ) )
        {   // We are NOT at the start
            setField( previousPosition, INDEX_NEXT, newPosition );
        }

        return setIdField( newPosition, inOrOutIndex( direction ), relId, trueForIncrement );
    }

    private int safeCastLongToInt( long value )
    {
        if ( value > Integer.MAX_VALUE )
        {
            throw new IllegalArgumentException( valueOf( value ) );
        }
        return (int) value;
    }

    private void move( int from, int to )
    {
        int physicalFrom = physicalIndex( from, 0 );
        int physicalTo = physicalIndex( to, 0 );
        System.arraycopy( cache, physicalFrom, cache, physicalTo, ARRAY_ROW_SIZE );
        Arrays.fill( cache, physicalFrom, physicalFrom+ARRAY_ROW_SIZE, EMPTY );
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
        cache[physicalIndex( relGroupCachePosition, INDEX_ID )] = relationshipGroupIdAssigner.nextId();
        cache[physicalIndex( relGroupCachePosition, INDEX_IN )] = IdFieldManipulator.emptyField();
        cache[physicalIndex( relGroupCachePosition, INDEX_OUT )] = IdFieldManipulator.emptyField();
    }

    private long setField( long position, int index, long newValue )
    {
        int physicalIndex = physicalIndex( position, index );
        long previousValue = cache[physicalIndex];
        cache[physicalIndex] = newValue;
        return previousValue;
    }

    private long setIdField( long position, int index, long relId, boolean trueForIncrement )
    {
        int physicalIndex = physicalIndex( position, index );
        long field = cache[physicalIndex];
        long previousId = IdFieldManipulator.getId( field );
        field = IdFieldManipulator.setId( field, relId );
        field = IdFieldManipulator.changeCount( field, trueForIncrement ? 1 : -1 );
        cache[physicalIndex] = field;
        return previousId;
    }

    private long getField( long relGroupCachePosition, int index )
    {
        int physicalIndex = physicalIndex( relGroupCachePosition, index );
        return cache[physicalIndex];
    }

    private int physicalIndex( long relGroupCachePosition, int index )
    {
        return (int) ((relGroupCachePosition * ARRAY_ROW_SIZE) + index);
    }

    private boolean empty( long value )
    {
        return value == EMPTY;
    }

    /**
     * @return relationship group id of the first group for this node.
     */
    public long getFirstRelGroupId( long relGroupIndex )
    {
        return getField( relGroupIndex, INDEX_ID );
    }

    public int getCount( long relGroupIndex, int type, Direction direction )
    {
        long groupIndexForType = findGroupIndexForType( relGroupIndex, type );
        if ( groupIndexForType == EMPTY )
        {
            throw new IllegalStateException( "type " + type + " not found for " + relGroupIndex );
        }
        return IdFieldManipulator.getCount( getField( groupIndexForType, inOrOutIndex( direction ) ) );
    }

    private long findGroupIndexForType( long relGroupIndex, int type )
    {
        long currentPosition = relGroupIndex;
        while ( !empty( currentPosition ) )
        {
            long foundType = getField( currentPosition, INDEX_TYPE );
            if ( foundType == type )
            {   // Found it
                return currentPosition;
            }
            else if ( foundType > type )
            {   // We came too far, create room for it
                break;
            }
            currentPosition = getField( currentPosition, INDEX_NEXT );
        }
        return EMPTY;
    }

    public boolean checkAndSetVisited( long index, int type, Direction direction )
    {
        long groupIndexForType = findGroupIndexForType( index, type );
        if ( groupIndexForType == EMPTY )
        {
            throw new IllegalStateException( "type " + type + " not found for " + index );
        }

        int directionIndex = inOrOutIndex( direction );
        long field = getField( groupIndexForType, directionIndex );
        boolean visited = IdFieldManipulator.isVisited( field );
        if ( visited )
        {
            return true;
        }
        setField( groupIndexForType, directionIndex, IdFieldManipulator.setVisited( field ) );
        return false;
    }
}
