package org.neo4j.batchimport.importer.structs;

import java.util.ArrayList;
import java.util.Arrays;

import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;

import static java.lang.String.valueOf;

public class RelationshipGroupCache
{
    private static final int INDEX_NEXT = 0;
    private static final int INDEX_TYPE = 1;
    private static final int INDEX_OUT = 2;
    private static final int INDEX_IN = 3;
    private static final int INDEX_LOOP = 4;
    public static final int ARRAY_ROW_SIZE = 5;
    private static final int DEFAULT_BLOCK_SIZE = 1000000;
    private IdSequence relationshipGroupIdAssigner;
    private ExtendableLongCache cache;
    private ExtendableLongCache relGroupId;
    private int nextFreeId = 0;

    public RelationshipGroupCache( long denseNodeCount, IdSequence relationshipGroupIdAssigner )
    {
        createRelationshipGroupCache( denseNodeCount, relationshipGroupIdAssigner, DEFAULT_BLOCK_SIZE );
    }

    public RelationshipGroupCache( long denseNodeCount, IdSequence relationshipGroupIdAssigner, int initialSize )
    {
        createRelationshipGroupCache( denseNodeCount, relationshipGroupIdAssigner, initialSize );
    }

    private void createRelationshipGroupCache( long denseNodeCount, IdSequence relationshipGroupIdAssigner,
            int initialSize )
    {
        this.relationshipGroupIdAssigner = relationshipGroupIdAssigner;
        int neededSize =  denseNodeCount * ARRAY_ROW_SIZE > Integer.MAX_VALUE ? Integer.MAX_VALUE : Utils.safeCastLongToInt( denseNodeCount ) * ARRAY_ROW_SIZE ;
        int increment = Math.min( initialSize, neededSize);
        cache = new ExtendableLongCache( increment );
        relGroupId = new ExtendableLongCache( Utils.safeCastLongToInt( denseNodeCount ) );
        System.out.println( "Rel group cache [Max Size:" + denseNodeCount * ARRAY_ROW_SIZE + " Initial size:"
                + cache.size() + " with increment = " + increment + "]" );
        cache.fill( ExtendableLongCache.EMPTY );
        relGroupId.fill( ExtendableLongCache.EMPTY );
    }

    public long size()
    {
        return nextFreeId;
    }

    public long allocate( int type, Direction direction, long relId ) throws BatchImportException
    {
        //System.out.println( "new group in allocate " + type );
        long logicalPosition = nextFreeId();
        initializeGroup( logicalPosition, type, true );
        setIdField( logicalPosition, inOrOutIndex( direction ), relId, true );
        return logicalPosition;
    }

    public long put( long relGroupCachePosition, int type, Direction direction, long relId, boolean trueForIncrement )
            throws BatchImportException
    {
        long currentPosition = relGroupCachePosition;
        long previousPosition = ExtendableLongCache.EMPTY;
        while ( !empty( currentPosition ) )
        {
            long foundType = getField( currentPosition, INDEX_TYPE );
            if ( foundType == type )
            { // Found it
                return setIdField( currentPosition, inOrOutIndex( direction ), relId, trueForIncrement );
            }
            else if ( foundType > type )
            { // We came too far, create room for it
                break;
            }
            previousPosition = currentPosition;
            currentPosition = getField( currentPosition, INDEX_NEXT );
        }
        long newPosition = nextFreeId();
        //System.out.println( "new group in put " + relGroupCachePosition + ", " + type );
        if ( empty( previousPosition ) )
        { // We are at the start
            move( safeCastLongToInt( currentPosition ), safeCastLongToInt( newPosition ) );
            long swap = newPosition;
            newPosition = currentPosition;
            currentPosition = swap;
        }
        initializeGroup( newPosition, type, false );
        if ( !empty( currentPosition ) )
        { // We are NOT at the end
            setField( newPosition, INDEX_NEXT, currentPosition );
        }
        if ( !empty( previousPosition ) )
        { // We are NOT at the start
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
        long physicalFrom = physicalIndex( from, 0 );
        long physicalTo = physicalIndex( to, 0 );
        cache.copy( physicalFrom, physicalTo, ARRAY_ROW_SIZE );
        cache.fill( physicalFrom, physicalFrom + ARRAY_ROW_SIZE, ExtendableLongCache.EMPTY );
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
        case BOTH:
            return INDEX_LOOP;
        default:
            throw new UnsupportedOperationException( direction.name() );
        }
    }

    private void initializeGroup( long relGroupCachePosition, int type, boolean allocateRelGroupId )
    {
        cache.put( physicalIndex( relGroupCachePosition, INDEX_TYPE ), (long) type );
        cache.put( physicalIndex( relGroupCachePosition, INDEX_OUT ), IdFieldManipulator.emptyField() );
        cache.put( physicalIndex( relGroupCachePosition, INDEX_IN ), IdFieldManipulator.emptyField() );
        cache.put( physicalIndex( relGroupCachePosition, INDEX_LOOP ), IdFieldManipulator.emptyField() );
        if ( allocateRelGroupId )
            relGroupId.put( relGroupCachePosition, relationshipGroupIdAssigner.nextId() );
    }
    
    public int getCount( long relGroupCachePosition, Direction direction) throws BatchImportException
    {
        return IdFieldManipulator.getCount( getField( relGroupCachePosition, inOrOutIndex(direction) ) );
    }
    
    private long setField( long position, int index, long newValue ) throws BatchImportException
    {
        long physicalIndex = physicalIndex( position, index );
        long previousValue = cache.get( physicalIndex );
        cache.put( physicalIndex, newValue );
        return previousValue;
    }

    private long setIdField( long position, int index, long relId, boolean trueForIncrement )
            throws BatchImportException
    {
        long physicalIndex = physicalIndex( position, index );
        long field = cache.get( physicalIndex );
        long previousId = IdFieldManipulator.getId( field );
        field = IdFieldManipulator.setId( field, relId );
        if ( trueForIncrement )
        {
            field = IdFieldManipulator.changeCount( field, 1 );
        }
        cache.put( physicalIndex, field );
        return previousId;
    }

    private long getField( long relGroupCachePosition, int index ) throws BatchImportException
    {
        long physicalIndex = physicalIndex( relGroupCachePosition, index );
        return cache.get( physicalIndex );
    }

    private long getIdField( long relGroupCachePosition, int index ) throws BatchImportException
    {
        long physicalIndex = physicalIndex( relGroupCachePosition, index );
        long field = cache.get( physicalIndex );
        return IdFieldManipulator.getId( field );
    }

    private long physicalIndex( long relGroupCachePosition, int index )
    {
        return ((relGroupCachePosition * ARRAY_ROW_SIZE) + index);
    }

    private boolean empty( long value )
    {
        return value == ExtendableLongCache.EMPTY;
    }

    /**
     * @return relationship group id of the first group for this node.
     */
    public long getFirstRelGroupId( long relGroupIndex ) throws BatchImportException
    {
        return relGroupId.get( relGroupIndex );
    }

    public int getCount( long relGroupIndex, int type, Direction direction ) throws BatchImportException
    {
        long groupIndexForType = findGroupIndexForType( relGroupIndex, type );
        if ( groupIndexForType == ExtendableLongCache.EMPTY )
        {
            throw new IllegalStateException( "type " + type + " not found for " + relGroupIndex );
        }
        return IdFieldManipulator.getCount( getField( groupIndexForType, inOrOutIndex( direction ) ) );
    }

    private long findGroupIndexForType( long relGroupIndex, int type ) throws BatchImportException
    {
        long currentPosition = relGroupIndex;
        while ( !empty( currentPosition ) )
        {
            long foundType = getField( currentPosition, INDEX_TYPE );
            if ( foundType == type )
            { // Found it
                return currentPosition;
            }
            else if ( foundType > type )
            { // We came too far, create room for it
                break;
            }
            currentPosition = getField( currentPosition, INDEX_NEXT );
        }
        return ExtendableLongCache.EMPTY;
    }

    public boolean checkAndSetVisited( long index, int type, Direction direction ) throws BatchImportException
    {
        long groupIndexForType = findGroupIndexForType( index, type );
        if ( groupIndexForType == ExtendableLongCache.EMPTY )
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

    public RelationshipGroupRecord[] createRelationshipGroupRecordChain( RelationshipGroupStore relGroupStore,
            long relGroupIndex, long nodeId ) throws BatchImportException
    {
        ArrayList<RelationshipGroupRecord> relGrpRecords = new ArrayList<RelationshipGroupRecord>();
        RelationshipGroupRecord previous = null;
        while ( true )
        {
            int type = Utils.safeCastLongToInt( getField( relGroupIndex, INDEX_TYPE ) );
            RelationshipGroupRecord record = new RelationshipGroupRecord( relGroupStore.nextId(), type );
            relGrpRecords.add( record );
            record.setCreated();
            record.setInUse( true );
            record.setOwningNode( nodeId );
            record.setFirstIn( getIdField( relGroupIndex, INDEX_IN ) );
            record.setFirstOut( getIdField( relGroupIndex, INDEX_OUT ) );
            record.setFirstLoop( getIdField( relGroupIndex, INDEX_LOOP ) );
            long nextIndex = getField( relGroupIndex, INDEX_NEXT );
            if ( previous != null )
            {
                previous.setNext( record.getId() );
                record.setPrev( previous.getId() );
            }
            else
                record.setPrev( Record.NO_NEXT_RELATIONSHIP.intValue() );
            if ( empty( nextIndex ) )
            {
                record.setNext( Record.NO_NEXT_RELATIONSHIP.intValue() );
                break;
            }
            previous = record;
            relGroupIndex = nextIndex;
        }
        return relGrpRecords.toArray( new RelationshipGroupRecord[0] );
    }

    public void clearAllIDs() throws BatchImportException
    {
        long size = size();
        for ( long i = 0; i < size; i++ )
        {
            setIdField( i, INDEX_IN, Record.NO_NEXT_RELATIONSHIP.intValue(), false );
            setIdField( i, INDEX_OUT, Record.NO_NEXT_RELATIONSHIP.intValue(), false );
            setIdField( i, INDEX_LOOP, Record.NO_NEXT_RELATIONSHIP.intValue(), false );
        }
    }
}
