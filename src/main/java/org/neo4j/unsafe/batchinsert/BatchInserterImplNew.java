package org.neo4j.unsafe.batchinsert;

import java.io.BufferedReader;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.batchimport.importer.RelType;
import org.neo4j.batchimport.importer.stages.ImportWorker;
import org.neo4j.batchimport.importer.stages.ReadFileData;
import org.neo4j.batchimport.importer.stages.StageContext;
import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.DiskBlockingQ;
import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.importer.structs.NodesCache;
import org.neo4j.batchimport.importer.structs.RelationshipGroupCache;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.nioneo.store.IdGeneratorImpl;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyBlock;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyType;
import org.neo4j.kernel.impl.nioneo.store.Record;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;

public class BatchInserterImplNew extends BatchInserterImpl
{
    private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
    private ReadFileData dataInput = null;
    private DiskBlockingQ diskBlockingQ;
    private NodesCache nodeCache = null;
    //private RelationshipGroupCache relationshipGroupCache = null;
    private NeoStore neoStore = null;

    public BatchInserterImplNew( String storeDir, Map<String, String> stringParams )
    {
        super( storeDir, new DefaultFileSystemAbstraction(), stringParams, (Iterable) Service
                .load( KernelExtensionFactory.class ) );
        neoStore = this.getNeoStore();
    }

    public void setNodeCache( NodesCache nodesCache )
    {
        this.nodeCache = nodesCache;
    }

    public NodesCache getNodeCache()
    {
        return nodeCache;
    }

    private NodeStore getNodeStore()
    {
        return neoStore.getNodeStore();
    }

    private PropertyStore getPropertyStore()
    {
        return neoStore.getPropertyStore();
    }

    private RelationshipStore getRelationshipStore()
    {
        return neoStore.getRelationshipStore();
    }

    public void setDiskBlockingQ( DiskBlockingQ diskBlockingQ )
    {
        this.diskBlockingQ = diskBlockingQ;
    }

    public void setDataInput( ReadFileData input )
    {
        dataInput = input;
    }

    public ReadFileData getDataInput()
    {
        return dataInput;
    }

    //------------------------------
    private static final Label[] NO_LABELS = new Label[0];
    private Label[] labelsArray = NO_LABELS;

    private Label[] labelsFor( String[] labels )
    {
        if ( labels == null || labels.length == 0 )
        {
            return NO_LABELS;
        }
        if ( labels.length != labelsArray.length )
        {
            labelsArray = new Label[labels.length];
        }
        for ( int i = labels.length - 1; i >= 0; i-- )
        {
            if ( labelsArray[i] == null || !labelsArray[i].name().equals( labels[i] ) )
            {
                labelsArray[i] = DynamicLabel.label( labels[i] );
            }
        }
        return labelsArray;
    }

    public void checkNodeId( long id )
    {
        if ( id < 0 || id > MAX_NODE_ID )
        {
            throw new IllegalArgumentException( "id=" + id );
        }
        if ( id == IdGeneratorImpl.INTEGER_MINUS_ONE )
        {
            throw new IllegalArgumentException( "id " + id + " is reserved for internal use" );
        }
        NodeStore nodeStore = neoStore.getNodeStore();
        if ( neoStore.getNodeStore().loadLightNode( id ) != null )
        {
            throw new IllegalArgumentException( "id=" + id + " already in use" );
        }
        long highId = nodeStore.getHighId();
        if ( highId <= id )
        {
            nodeStore.setHighId( id + 1 );
        }
    }

    public void createNodeRecords( CSVDataBuffer buf ) throws BatchImportException
    {
        try
        {
            for ( int index = 0; index < buf.getCurEntries(); index++ )
            {
                NodeRecord nodeRecord = new NodeRecord( buf.getId( index ), false,
                        Record.NO_NEXT_RELATIONSHIP.intValue(), Record.NO_NEXT_PROPERTY.intValue() );
                nodeRecord.setInUse( true );
                nodeRecord.setCreated();
                this.setNodeLabels( nodeRecord, labelsFor( dataInput.getTypeLabels( buf, index ) ) );
                buf.getDiskRecords( Constants.NODE ).addRecord( nodeRecord, index );
            }
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[createNodeRecords failed]", e );
        }
    }

    public void importNode_writeStore( CSVDataBuffer buf, boolean relation ) throws BatchImportException
    {
        DiskRecordsBuffer recsNode = buf.removeDiskRecords( Constants.NODE );
        if ( !buf.isMoreData() )
        {
            recsNode.setLastBuffer( !buf.isMoreData() );
        }
        DiskRecordsBuffer recsProps = buf.removeDiskRecords( Constants.PROPERTY );
        if ( !buf.isMoreData() )
        {
            recsProps.setLastBuffer( !buf.isMoreData() );
        }
        ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
        try
        {
            diskBlockingQ.putBuffer( Constants.PROPERTY, recsProps );
            diskBlockingQ.putBuffer( Constants.NODE, recsNode );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[importNode_writeNodeStore failed]", e );
        }
    }

    public void importRelationships_createRelationshipRecords( CSVDataBuffer buf ) throws BatchImportException
    {
        long id = 0;
        if ( buf.getDiskRecords( Constants.RELATIONSHIP ).getMaxEntries() < buf.getMaxEntries() )
        {
            buf.getDiskRecords( Constants.RELATIONSHIP ).extend( buf.getMaxEntries() );
        }
        for ( int index = 0; index < buf.getCurEntries(); index++ )
        {
            try
            {
                id = getRelationshipStore().nextId();
                buf.setId( index, id );
                int typeId = createRelTypeId( buf.getString( index, 2 ) );
                RelationshipRecord record = new RelationshipRecord( id, buf.getLong( index, 0 ),
                        buf.getLong( index, 1 ), typeId );
                record.setInUse( true );
                record.setCreated();
                record.setFirstInFirstChain( false );
                record.setFirstInSecondChain( false );
                buf.getDiskRecords( Constants.RELATIONSHIP ).addRecord( record, index );
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[importRelationships_createRelationshipRecords failed]"
                        + e.getMessage() );
            }
        }
    }

    // Forward linking
    public void importRelationships_prepareRecords( CSVDataBuffer buf ) throws BatchImportException
    {
        long firstNextRel, secondNextRel, firstNodeId, secondNodeId;
        RelationshipRecord rel;
        setPropIds( buf, true );
        for ( int index = 0; index < buf.getCurEntries(); index++ )
        {
            try
            {
                rel = null;
                try
                {
                    rel = (RelationshipRecord) buf.getDiskRecords( Constants.RELATIONSHIP ).getRecord( index, 0 );
                }
                catch ( Exception ee )
                {
                    Utils.SystemOutPrintln( "createRelationship3-No relationship record" );
                }
                firstNodeId = rel.getFirstNode();
                secondNodeId = rel.getSecondNode();
                if ( firstNodeId == secondNodeId )
                {
                    firstNextRel = secondNextRel = doNodeLoopStuff( rel, true );
                }
                else
                {
                    firstNextRel = doNodeStuff( firstNodeId, rel, Direction.OUTGOING, true );
                    secondNextRel = doNodeStuff( secondNodeId, rel, Direction.INCOMING, true );
                }
                assert firstNextRel != rel.getId();
                assert secondNextRel != rel.getId();
                /*rel.setFirstNextRel( firstNextRel );
                rel.setSecondNextRel( secondNextRel );*/
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[importRelationships_prepareRecords failed]", e );
            }
        }
    }

    private long doNodeLoopStuff( RelationshipRecord rel, boolean forwardScan ) throws BatchImportException
    {
        long nodeId = rel.getFirstNode();
        if ( nodeCache.nodeIsDense( nodeId ) )
        { // dense
            long relGroupIndex = nodeCache.get( nodeId );
            if ( relGroupIndex == -1 )
            {
                relGroupIndex = nodeCache.getRelationshipGroupCache().allocate( rel.getType(), Direction.BOTH,
                        rel.getId() );
                nodeCache.put( nodeId, relGroupIndex );
                if ( !forwardScan )
                    throw new BatchImportException(
                            "During Back linking of relationships, encountered RelGroupInde = -1" );
                rel.setFirstNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                rel.setSecondNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                return Record.NO_NEXT_RELATIONSHIP.intValue();
            }
            else
            {
                long previousRel = nodeCache.getRelationshipGroupCache().put( relGroupIndex, rel.getType(),
                        Direction.BOTH, rel.getId(), forwardScan );
                if ( !forwardScan && previousRel == Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    //this is first relationship during link back
                    rel.setFirstInFirstChain( true );
                    rel.setFirstInSecondChain( true );
                    rel.setFirstPrevRel( nodeCache.getRelationshipGroupCache().getCount( nodeCache.get( nodeId ),
                            Direction.BOTH ) );
                    rel.setSecondPrevRel( nodeCache.getRelationshipGroupCache().getCount( nodeCache.get( nodeId ),
                            Direction.BOTH ) );
                }
                return previousRel;
            }
        }
        else
        { // sparse
            long result = nodeCache.get( nodeId );
            nodeCache.put( nodeId, rel.getId() );
            if ( forwardScan )
            {
                rel.setFirstNextRel( result );
                rel.setSecondNextRel( result );
            }
            else
            {
                rel.setFirstPrevRel( result );
                rel.setSecondPrevRel( result );
                if ( result == Record.NO_NEXT_RELATIONSHIP.intValue() )
                {
                    rel.setFirstInFirstChain( true );
                    rel.setFirstInSecondChain( true );
                }
            }
            return result;
        }
    }

    private long doNodeStuff( long nodeId, RelationshipRecord rel, Direction direction, boolean forwardScan )
            throws BatchImportException
    {
        if ( nodeCache.nodeIsDense( nodeId ) )
        { // This is a dense node
            long relGroupIndex = nodeCache.get( nodeId );
            if ( relGroupIndex == -1 )
            { // this can happen only in forward scan
                if ( !forwardScan )
                    throw new BatchImportException(
                            "During Back linking of relationships, encountered RelGroupIndex = -1" );
                relGroupIndex = nodeCache.getRelationshipGroupCache().allocate( rel.getType(), direction, rel.getId() );
                nodeCache.put( nodeId, relGroupIndex );
                if ( direction == Direction.OUTGOING )
                    rel.setFirstNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                else
                    rel.setSecondNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
                return Record.NO_NEXT_RELATIONSHIP.intValue();
            }
            else
            {
                long previousRel = nodeCache.getRelationshipGroupCache().put( relGroupIndex, rel.getType(), direction,
                        rel.getId(), forwardScan );
                if ( forwardScan )
                {
                    if ( direction == Direction.OUTGOING )
                        rel.setFirstNextRel( previousRel );
                    else
                        rel.setSecondNextRel( previousRel );
                }
                else
                { //backward scan
                    if ( direction == Direction.OUTGOING )
                    {
                        if ( previousRel == Record.NO_NEXT_RELATIONSHIP.intValue() )
                        { // first relationship
                            rel.setFirstPrevRel( nodeCache.getRelationshipGroupCache().getCount(
                                    nodeCache.get( nodeId ), direction ) );
                            rel.setFirstInFirstChain( true );
                        }
                        else
                            rel.setFirstPrevRel( previousRel );
                    }
                    else
                    {
                        if ( previousRel == Record.NO_NEXT_RELATIONSHIP.intValue() )
                        { // first relationship
                            rel.setSecondPrevRel( nodeCache.getRelationshipGroupCache().getCount(
                                    nodeCache.get( nodeId ), direction ) );
                            rel.setFirstInSecondChain( true );
                        }
                        else
                            rel.setSecondPrevRel( previousRel );
                    }
                }
                return previousRel;
            }
        }
        else
        { // This is a sparse node
            long result = nodeCache.get( nodeId );
            nodeCache.put( nodeId, rel.getId() );
            if ( forwardScan )
            {
                if ( direction == Direction.OUTGOING )
                    rel.setFirstNextRel( result );
                else
                    rel.setSecondNextRel( result );
            }
            else
            {
                if ( direction == Direction.OUTGOING )
                {
                    rel.setFirstPrevRel( result );
                    if ( result == Record.NO_NEXT_RELATIONSHIP.intValue() )
                        rel.setFirstInFirstChain( true );
                }
                else
                {
                    rel.setSecondPrevRel( result );
                    if ( result == Record.NO_NEXT_RELATIONSHIP.intValue() )
                        rel.setFirstInSecondChain( true );
                }
            }
            return result;
        }
    }

    public void importRelationships_writeStore( CSVDataBuffer buf ) throws BatchImportException
    {
        ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
        try
        {
            DiskRecordsBuffer recs = buf.removeDiskRecords( Constants.NODE );
            if ( !buf.isMoreData() )
            {
                recs.setLastBuffer( !buf.isMoreData() );
            }
            diskBlockingQ.putBuffer( Constants.NODE, recs );
            recs = buf.removeDiskRecords( Constants.PROPERTY );
            if ( !buf.isMoreData() )
            {
                recs.setLastBuffer( !buf.isMoreData() );
            }
            diskBlockingQ.putBuffer( Constants.PROPERTY, recs );
            recs = buf.removeDiskRecords( Constants.RELATIONSHIP );
            if ( !buf.isMoreData() )
            {
                recs.setLastBuffer( !buf.isMoreData() );
            }
            diskBlockingQ.putBuffer( Constants.RELATIONSHIP, recs );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[Adding to Property/Node/Relation Q failed][" + buf.getBufSequenceId()
                    + "]", e );
        }
    }

    public void closeStores()
    {
        getNodeStore().close();
        getPropertyStore().close();
        getRelationshipStore().close();
    }

    public void importEncodeProps( CSVDataBuffer buf ) throws BatchImportException
    {
        PropertyStore propStore = getPropertyStore();
        ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
        if ( buf.getDiskRecords( Constants.PROPERTY ).getMaxEntries() < buf.getMaxEntries() )
        {
            buf.getDiskRecords( Constants.PROPERTY ).extend( buf.getMaxEntries() );
        }
        if ( buf.getDiskRecords( Constants.PROPERTY_BLOCK ).getMaxEntries() < buf.getMaxEntries() )
        {
            buf.getDiskRecords( Constants.PROPERTY_BLOCK ).extend( buf.getMaxEntries() );
        }
        for ( int index = 0; index < buf.getCurEntries(); index++ )
        {
            try
            {
                Map<String, Object> properties = dataInput.getProperties( buf, index );
                if ( properties == null || properties.isEmpty() )
                {
                    buf.getDiskRecords( Constants.PROPERTY_BLOCK ).clearRecord( index );
                    continue;
                }
                buf.getDiskRecords( Constants.PROPERTY_BLOCK ).clearRecord( index );
                for ( Entry<String, Object> entry : properties.entrySet() )
                {
                    int keyId = this.propertyKeyTokens.idOf( entry.getKey() );
                    if ( keyId == -1 )
                    {
                        keyId = this.createNewPropertyKeyId( entry.getKey() );
                    }
                    PropertyBlock block = new PropertyBlock();
                    propStore.encodeValue( block, keyId, entry.getValue() );
                    buf.getDiskRecords( Constants.PROPERTY_BLOCK ).addRecord( block, index );
                }
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[importEncodeProps failed]", e );
            }
        }
    }

    public void setPropIds( CSVDataBuffer buf, boolean relation ) throws BatchImportException
    {
        PropertyStore propStore = getPropertyStore();
        ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
        for ( int index = 0; index < buf.getCurEntries(); index++ )
        {
            buf.getDiskRecords( Constants.PROPERTY ).clearRecord( index );
            if ( buf.getDiskRecords( Constants.PROPERTY_BLOCK ).isEmpty( index ) )
            {
                continue;
            }
            long id = propStore.nextId();
            ;
            PropertyRecord currentRecord = new PropertyRecord( id );
            currentRecord.setInUse( true );
            currentRecord.setCreated();
            buf.getDiskRecords( Constants.PROPERTY ).addRecord( currentRecord, index );
            int recSize = currentRecord.size();
            Iterator<Object> it = buf.getDiskRecords( Constants.PROPERTY_BLOCK ).iterator( index );
            while ( it.hasNext() )
            {
                PropertyBlock block = (PropertyBlock) it.next();
                recSize += block.getSize();
                if ( recSize > PropertyType.getPayloadSize() )
                {
                    PropertyRecord prevRecord = currentRecord;
                    long lastPropId = propStore.nextId();
                    currentRecord = new PropertyRecord( lastPropId );
                    prevRecord.setNextProp( currentRecord.getId() );
                    currentRecord.setPrevProp( prevRecord.getId() );
                    currentRecord.setInUse( true );
                    currentRecord.setCreated();
                    buf.getDiskRecords( Constants.PROPERTY ).addRecord( currentRecord, index );
                    recSize = currentRecord.size() + block.getSize();
                }
                currentRecord.addPropertyBlock( block );
            }
            long propId = ((PropertyRecord) buf.getDiskRecords( Constants.PROPERTY ).getRecord( index, 0 )).getId();
            if ( relation )
            {
                ((RelationshipRecord) buf.getDiskRecords( Constants.RELATIONSHIP ).getRecord( index, 0 ))
                        .setNextProp( propId );
            }
            else
            {
                ((NodeRecord) buf.getDiskRecords( Constants.NODE ).getRecord( index, 0 )).setNextProp( propId );
            }
        }
    }

    public void writeProperty( DiskRecordsBuffer buf ) throws BatchImportException
    {
        for ( int index = 0; index < buf.getCurrentEntries(); index++ )
        {
            try
            {
                for ( int i = 0; i < buf.getSize( index ); i++ )
                {
                    getPropertyStore().updateRecord( (PropertyRecord) buf.getRecord( index, i ) );
                }
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[WriteProperty failed]", e );
            }
        }
    }

    long prevId = 0;

    public void writeRelationship( DiskRecordsBuffer buf ) throws BatchImportException
    {
        for ( int index = 0; index < buf.getCurrentEntries(); index++ )
        {
            try
            {
                for ( int i = 0; i < buf.getSize( index ); i++ )
                {
                    RelationshipRecord rel = (RelationshipRecord) buf.getRecord( index, i );
                    getRelationshipStore().updateRecord( rel );
                }
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[WriteRelationship failed]", e );
            }
        }
    }

    public void writeNode( DiskRecordsBuffer buf ) throws BatchImportException
    {
        for ( int index = 0; index < buf.getCurrentEntries(); index++ )
        {
            try
            {
                for ( int i = 0; i < buf.getSize( index ); i++ )
                {
                    NodeRecord node = (NodeRecord) buf.getRecord( index, i );
                    getNodeStore().updateRecord( node );
                }
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[WriteNodes failed]", e );
            }
        }
    }

    public int writeNodeNextRel() throws BatchImportException
    {
        int errorCount = 0;
        long lastId = getNodeStore().getHighId();
        NodeRecord nodeRec = null;
        for ( long id = lastId - 1; id >= 0; id-- )
        {
            try
            {
                nodeRec = getNodeStore().getRecord( id );
                if ( nodeCache.nodeIsDense( id ) )
                {
                    nodeRec.setDense( true );
                    //nodeRec.setNextRel( relationshipGroupCache.getFirstRelGroupId( nodeCache.get( id ) ) );
                    nodeRec.setNextRel( writeRelationshipGroupRecords( id ) );
                }
                else
                {
                    nodeRec.setNextRel( nodeCache.get( id ) );
                }
                getNodeStore().updateRecord( nodeRec );
            }
            catch ( Exception e )
            {
                System.out.println( "[writeNodeNextRel failed]" + e.getMessage() );
                if ( errorCount++ > Constants.errorThreshold )
                {
                    throw new BatchImportException( "[writeNodeNextRel failed]{Errors exceeded error threshold -"
                            + errorCount + "]", e );
                }
            }
        }
        return errorCount;
    }

    public void linkBackRelationships() throws BatchImportException
    {
        int errorCount = writeNodeNextRel();
        if ( errorCount > 0 )
        {
            throw new BatchImportException( "[writeNodeNextRel failed]{Errors :" + errorCount + "]" );
        }
        errorCount = relationshiplinkBack();
        if ( errorCount > 0 )
        {
            throw new BatchImportException( "[relationshiplinkBack failed]{Errors :" + errorCount + "]" );
        }
    }

    public int relationshiplinkBack() throws BatchImportException
    {
        int errorCount = 0;
        long current = 0, prev = 0, relId = 0;
        long firstNode = 0, secondNode = 0;
        RelationshipRecord relRecord = null;
        long maxNodeId = neoStore.getNodeStore().getHighId();
        long maxRelId = neoStore.getRelationshipStore().getHighId();
        if ( nodeCache == null )
        {
            nodeCache = new NodesCache( maxNodeId, neoStore );
        }
        else
        {
            nodeCache.cleanIds( false );
        }
        if ( nodeCache.getRelationshipGroupCache() != null )
            nodeCache.getRelationshipGroupCache().clearAllIDs();
        long startLinkBack = prev = System.currentTimeMillis();
        for ( long id = maxRelId - 1; id >= 0; id-- )
        {
            try
            {
                relRecord = neoStore.getRelationshipStore().getRecord( id );
                if ( !relRecord.inUse() )
                {
                    String msg = "Relationship [" + relRecord.getId() + " [" + relRecord.toString() + "] not in use";
                    System.out.println( msg );
                    throw new BatchImportException( msg );
                }
                relId = relRecord.getId();
                firstNode = relRecord.getFirstNode();
                secondNode = relRecord.getSecondNode();
                if ( firstNode == secondNode )
                {
                    doNodeLoopStuff( relRecord, false );
                }
                else
                {
                    doNodeStuff( firstNode, relRecord, Direction.OUTGOING, false );
                    doNodeStuff( secondNode, relRecord, Direction.INCOMING, false );
                }
                neoStore.getRelationshipStore().updateRecord( relRecord );
                if ( id % 10000000 == 0 )
                {
                    current = System.currentTimeMillis();
                    System.out.println( "\tCompleted Relationship back linking " + (maxRelId - id) + " links in "
                            + (current - startLinkBack) + " ms [" + (current - prev) + "]" );
                    prev = current;
                }
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                System.out.println( "[Error in Relationship link back logic]" + e.getMessage() );
                if ( errorCount++ > Constants.errorThreshold )
                {
                    throw new BatchImportException( "[Error in Relationship link back logic]{Errors exceeded error "
                            + "threshold -" + errorCount + "]" );
                }
            }
        }
        return errorCount;
    }

    private int createRelTypeId( String relTypeName )
    {
        int typeId = this.relationshipTypeTokens.idOf( relTypeName );
        if ( typeId == -1 )
        {
            typeId = this.createNewRelationshipType( relTypeName );
        }
        return typeId;
    }

    public void accumulateNodeCount( ReadFileData input ) throws BatchImportException
    {
        CSVDataBuffer buffer = new CSVDataBuffer( Constants.BUFFER_ENTRIES, Constants.BUFFER_SIZE_BYTES, null, 0 );
        buffer.initRecords( input.getHeaderLength() );
        do
        {
            StageContext.dataExtract( input, buffer );
            for ( int index = 0; index < buffer.getCurEntries(); index++ )
            {
                long firstNode = buffer.getLong( index, 0 );
                long secondNode = buffer.getLong( index, 1 );
                nodeCache.incrementCount( firstNode );
                nodeCache.incrementCount( secondNode );
                createRelTypeId( buffer.getString( index, 2 ) );
            }
        }
        while ( buffer.isMoreData() );
    }

    public void accumulateNodeCount( CSVDataBuffer buffer ) throws BatchImportException
    {
        try
        {
            for ( int index = 0; index < buffer.getCurEntries(); index++ )
            {
                long firstNode = buffer.getLong( index, 0 );
                long secondNode = buffer.getLong( index, 1 );
                nodeCache.incrementCount( firstNode );
                nodeCache.incrementCount( secondNode );
                createRelTypeId( buffer.getString( index, 2 ) );
            }
        }
        catch ( Exception e )
        {
            e.printStackTrace();
            throw new BatchImportException( "[Error in accumulateNodeCount - " + e.getMessage() + "]" );
        }
    }

    public long writeRelationshipGroupRecords( long nodeId ) throws BatchImportException
    {
        if ( !nodeCache.nodeIsDense( nodeId ) )
            return Record.NO_NEXT_RELATIONSHIP.intValue();
        RelationshipGroupStore relGroupStore = neoStore.getRelationshipGroupStore();
        long relGroupIndex = nodeCache.get( nodeId );
        RelationshipGroupRecord[] recordArray = nodeCache.getRelationshipGroupCache()
                .createRelationshipGroupRecordChain( relGroupStore, relGroupIndex, nodeId );
        for ( RelationshipGroupRecord record : recordArray )
        {
            relGroupStore.updateRecord( record );
        }
        return recordArray[0].getId();
    }
}
