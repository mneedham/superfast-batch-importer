package org.neo4j.unsafe.batchinsert;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.neo4j.batchimport.importer.RelType;
import org.neo4j.batchimport.newimport.stages.ReadFileData;
import org.neo4j.batchimport.newimport.structs.Constants;
import org.neo4j.batchimport.newimport.structs.DiskBlockingQ;
import org.neo4j.batchimport.newimport.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.newimport.structs.CSVDataBuffer;
import org.neo4j.batchimport.newimport.structs.NodesCache;
import org.neo4j.batchimport.newimport.utils.Utils;
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
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;


public class BatchInserterImplNew extends BatchInserterImpl{
	private static final long MAX_NODE_ID = IdType.NODE.getMaxValue();
	private ReadFileData dataInput = null;
	private DiskBlockingQ diskBlockingQ;
	private NodesCache nodeCache = null;
	private NeoStore neoStore = null;
	public BatchInserterImplNew( String storeDir,
            Map<String, String> stringParams){
		super(storeDir, new DefaultFileSystemAbstraction(), stringParams, (Iterable) Service.load( KernelExtensionFactory.class ));
		neoStore = this.getNeoStore();
	}
	public void setNodesCache(NodesCache nodesCache){
		this.nodeCache = nodesCache;
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

	public void setDiskBlockingQ(DiskBlockingQ diskBlockingQ){
		this.diskBlockingQ = diskBlockingQ;
	}
	public void setDataInput(ReadFileData input){
		dataInput = input;
	}

	public int writeNodeNextRel()throws BatchImportException{
		int errorCount = 0;
		long lastId = getNodeStore().getHighId();
		NodeRecord nodeRec = null;
		try {
			for (long id = lastId-1; id >= 0 ; id--){
				try {
					nodeRec = getNodeStore().getRecord(id);
					nodeRec.setNextRel(nodeCache.get(id));
					getNodeStore().updateRecord(nodeRec);
				} catch (Exception e){
					errorCount++;
				} 		
			}
		}catch (Exception e){
			throw new BatchImportException("[writeNodeNextRel failed]"+e.getMessage());
		}
		return errorCount;
	}
	public ReadFileData getDataInput(){
		return dataInput;
	}
	//------------------------------
	private static final Label[] NO_LABELS = new Label[0];
	private Label[] labelsArray = NO_LABELS;
	private Label[] labelsFor(String[] labels) {
		if (labels == null || labels.length == 0) return NO_LABELS;
		if (labels.length != labelsArray.length) labelsArray = new Label[labels.length];
		for (int i = labels.length - 1; i >= 0; i--) {
			if (labelsArray[i] == null || !labelsArray[i].name().equals(labels[i]))
				labelsArray[i] = DynamicLabel.label(labels[i]);
		}
		return labelsArray;
	}

	public void checkNodeId( long id)
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

	public void createNodeRecords(CSVDataBuffer buf)throws BatchImportException{
		try {
			for (int index = 0; index < buf.getCurEntries(); index++)
			{ 	
				NodeRecord nodeRecord = new NodeRecord( buf.getId(index), Record.NO_NEXT_RELATIONSHIP.intValue(),
						Record.NO_NEXT_PROPERTY.intValue() );
				nodeRecord.setInUse( true );
				nodeRecord.setCreated();
				this.setNodeLabels( nodeRecord, labelsFor(dataInput.getTypeLabels(buf, index)) );
				buf.getDiskRecords(Constants.NODE).addRecord(nodeRecord, index);
			}
		}catch (Exception e){
			throw new BatchImportException("[createNodeRecords failed]"+e.getMessage());
		}
	}

	public void importNode_EncodeProps(CSVDataBuffer buf)throws BatchImportException{
		importEncodeProps(buf);	
	}

	public void importNode_writeStore(CSVDataBuffer buf, boolean relation)throws BatchImportException{
		DiskRecordsBuffer recsNode = buf.removeDiskRecords(Constants.NODE);
		if (!buf.isMoreData())
			recsNode.setLastBuffer(!buf.isMoreData());

		DiskRecordsBuffer recsProps = buf.removeDiskRecords(Constants.PROPERTY);
		if (!buf.isMoreData())
			recsProps.setLastBuffer(!buf.isMoreData());

		try {
			diskBlockingQ.putBuffer(Constants.PROPERTY, recsProps);
			diskBlockingQ.putBuffer(Constants.NODE, recsNode);
		}catch (Exception e){
			throw new BatchImportException("[importNode_writeNodeStore failed]"+e.getMessage());
		}	
	}

	public void importRelationships_createRelationshipRecords(CSVDataBuffer buf)throws BatchImportException{
		final RelType relType = new RelType();
		long id  = 0;
		if (buf.getDiskRecords(Constants.RELATIONSHIP).getMaxEntries() < buf.getMaxEntries())
			buf.getDiskRecords(Constants.RELATIONSHIP).extend(buf.getMaxEntries());
		for (int index = 0; index < buf.getCurEntries(); index++)
		{ 	
			try{
				id = getRelationshipStore().nextId();
				buf.setId(index, id); 
				final RelType type = relType.update(buf.getString(index, 2));
				int typeId = this.relationshipTypeTokens.idOf( type.name() );
				if ( typeId == -1 )
				{
					typeId = this.createNewRelationshipType( type.name() );
				}
				RelationshipRecord record = new RelationshipRecord( id, 
						buf.getLong(index, 0), 
						buf.getLong(index, 1), 
						typeId );
				record.setInUse( true );
				record.setCreated();
				buf.getDiskRecords(Constants.RELATIONSHIP).addRecord(record, index);
			} catch (Exception e){
				throw new BatchImportException("[importRelationships_createRelationshipRecords failed]"+e.getMessage());
			}
		}  	
	}

	public void importRelationships_prepareRecords(CSVDataBuffer buf, boolean relation)throws BatchImportException{
		long firstNextRel, secondNextRel, firstNodeId, secondNodeId;
		RelationshipRecord rel;

		setPropIds(buf, relation);
		for (int index = 0; index < buf.getCurEntries(); index++)
		{ 	
			try{
				rel = null;
				try {
					rel = (RelationshipRecord)buf.getDiskRecords(Constants.RELATIONSHIP).getRecord(index, 0);
				} catch (Exception ee){
					Utils.SystemOutPrintln("createRelationship3-No relationship record");
				}
				firstNodeId = rel.getFirstNode();
				secondNodeId = rel.getSecondNode();

				if (firstNodeId == secondNodeId){
					firstNextRel = secondNextRel = nodeCache.get(firstNodeId);
					nodeCache.put(firstNodeId, rel.getId());
				} else {
					firstNextRel  = nodeCache.get(firstNodeId);	
					nodeCache.put(firstNodeId, rel.getId());
					secondNextRel = nodeCache.get(secondNodeId);
					nodeCache.put(secondNodeId, rel.getId());
				}
				assert firstNextRel != rel.getId();
				assert secondNextRel != rel.getId();
				rel.setFirstNextRel( firstNextRel );
				rel.setSecondNextRel( secondNextRel );
			} catch (Exception e){
				throw new BatchImportException("[importRelationships_prepareRecords failed]"+e.getMessage());
			}
		}
	}

	public void importRelationships_writeStore(CSVDataBuffer buf)throws BatchImportException{

		try{
			DiskRecordsBuffer recs = buf.removeDiskRecords(Constants.NODE);
			if (!buf.isMoreData())
				recs.setLastBuffer(!buf.isMoreData());
			diskBlockingQ.putBuffer(Constants.NODE, recs);

			recs = buf.removeDiskRecords(Constants.PROPERTY);
			if (!buf.isMoreData())
				recs.setLastBuffer(!buf.isMoreData());
			diskBlockingQ.putBuffer(Constants.PROPERTY, recs);

			recs = buf.removeDiskRecords(Constants.RELATIONSHIP);
			if (!buf.isMoreData())
				recs.setLastBuffer(!buf.isMoreData());
			diskBlockingQ.putBuffer(Constants.RELATIONSHIP, recs);
		} catch (Exception e){
			throw new BatchImportException("[Adding to Property/Node/Relation Q failed]["+buf.getBufSequenceId()+"]"+e.getMessage());
		}
	}
	public void closeStores(){
		getNodeStore().close();
		getPropertyStore().close();
		getRelationshipStore().close();
	}
	public void importEncodeProps(CSVDataBuffer buf)throws BatchImportException
	{
		PropertyStore propStore = getPropertyStore();
		if (buf.getDiskRecords(Constants.PROPERTY).getMaxEntries() < buf.getMaxEntries())
			buf.getDiskRecords(Constants.PROPERTY).extend(buf.getMaxEntries());
		if (buf.getDiskRecords(Constants.PROPERTY_BLOCK).getMaxEntries() < buf.getMaxEntries())
			buf.getDiskRecords(Constants.PROPERTY_BLOCK).extend(buf.getMaxEntries());
		for (int index = 0; index < buf.getCurEntries(); index++){	
			try{
				Map<String, Object> properties = dataInput.getProperties(buf, index);;
				if ( properties == null || properties.isEmpty() ) {
					buf.getDiskRecords(Constants.PROPERTY_BLOCK).clearRecord(index);
					continue;
				}  	
				buf.getDiskRecords(Constants.PROPERTY_BLOCK).clearRecord(index);  		
				for ( Entry<String, Object> entry : properties.entrySet() )
				{
					int keyId = this.propertyKeyTokens.idOf( entry.getKey() );
					if ( keyId == -1 )
						keyId = this.createNewPropertyKeyId( entry.getKey() );		
					PropertyBlock block = new PropertyBlock();
					propStore.encodeValue(block, keyId, entry.getValue() );
					buf.getDiskRecords(Constants.PROPERTY_BLOCK).addRecord(block, index);
				}
			}catch (Exception e){
				throw new BatchImportException("[importEncodeProps failed]"+e.getMessage());
			}
		}
	}
	public void setPropIds(CSVDataBuffer buf, boolean relation)throws BatchImportException{
		PropertyStore propStore = getPropertyStore();
		for (int index = 0; index < buf.getCurEntries(); index++){	
			buf.getDiskRecords(Constants.PROPERTY).clearRecord(index);
			if (buf.getDiskRecords(Constants.PROPERTY_BLOCK).isEmpty(index)) 
				continue;

			long id = propStore.nextId();;
			PropertyRecord currentRecord = new PropertyRecord(id);
			currentRecord.setInUse( true );
			currentRecord.setCreated();
			buf.getDiskRecords(Constants.PROPERTY).addRecord(currentRecord, index);
			int recSize = currentRecord.size();
			Iterator<Object> it = buf.getDiskRecords(Constants.PROPERTY_BLOCK).iterator(index);
			while (it.hasNext() )
			{
				PropertyBlock block = (PropertyBlock)it.next();
				recSize += block.getSize();
				if (  recSize  > PropertyType.getPayloadSize() ){
					PropertyRecord prevRecord = currentRecord;
					long lastPropId = propStore.nextId();
					currentRecord = new PropertyRecord(lastPropId);				
					prevRecord.setNextProp( currentRecord.getId() );
					currentRecord.setPrevProp( prevRecord.getId()  );
					currentRecord.setInUse( true );
					currentRecord.setCreated();
					buf.getDiskRecords(Constants.PROPERTY).addRecord(currentRecord, index);
					recSize = currentRecord.size() + block.getSize();
				}
				currentRecord.addPropertyBlock( block );
			}
			long propId = ((PropertyRecord)buf.getDiskRecords(Constants.PROPERTY).getRecord(index, 0)).getId();
			if (relation)			
				((RelationshipRecord)buf.getDiskRecords(Constants.RELATIONSHIP).getRecord(index, 0)).setNextProp(propId);
			else
				((NodeRecord)buf.getDiskRecords(Constants.NODE).getRecord(index, 0)).setNextProp(propId);

		}
	}

	public void writeProperty(DiskRecordsBuffer buf)throws BatchImportException{
		for (int index = 0; index < buf.getCurrentEntries(); index++)
		{ 	
			try{
				for ( int i = 0; i <  buf.getSize(index); i++ )	
					getPropertyStore().updateRecord((PropertyRecord)buf.getRecord(index, i ));	
			}catch (Exception e){
				throw new BatchImportException("[WriteProperty failed]"+e.getMessage());
			}
		}
		buf.cleanup();
	}
	long prevId = 0;
	public void writeRelationship(DiskRecordsBuffer buf)throws BatchImportException{
		for (int index = 0; index < buf.getCurrentEntries(); index++)
		{ 	
			try{
				for ( int i = 0; i <  buf.getSize(index); i++ ){
					RelationshipRecord rel = (RelationshipRecord)buf.getRecord(index, i );
					getRelationshipStore().updateRecord( rel );	
				}
			}catch (Exception e){
				throw new BatchImportException("[WriteRelationship failed]"+e.getMessage());
			}
		}
		buf.cleanup();
	}
	public void writeNode(DiskRecordsBuffer buf)throws BatchImportException{
		for (int index = 0; index < buf.getCurrentEntries(); index++)
		{
			try{
				for ( int i = 0; i <  buf.getSize(index); i++ ){
					NodeRecord node =  (NodeRecord)buf.getRecord(index, i );
					getNodeStore().updateRecord( node );
				}
			}catch (Exception e){
				throw new BatchImportException("[WriteNodes failed]"+e.getMessage());
			}
		}
		buf.cleanup();
	}

	public void linkBackRelationships() throws BatchImportException{       	
		writeNodeNextRel();
		linkBack();
	}
	public int linkBack() throws BatchImportException{
		int errorCount = 0;
		long current = 0, prev = 0, relId = 0;
		long firstNode = 0, secondNode = 0;
		RelationshipRecord relRecord = null;
		long maxNodeId = neoStore.getNodeStore().getHighId();
		long maxRelId = neoStore.getRelationshipStore().getHighId();
		System.out.println(Utils.memoryStats());
		if (nodeCache == null)
			nodeCache = new NodesCache(this.getNeoStore().getNodeStore().getHighId());
		else
			nodeCache.clean();
		System.out.println(Utils.memoryStats());
		long startLinkBack = prev = System.currentTimeMillis();
		for (long id = maxRelId-1; id >= 0; id--){
			try {
				relRecord =  neoStore.getRelationshipStore().getRecord(id);
				if (!relRecord.inUse()){
					throw new BatchImportException("Relationship ["+relRecord.getId()+"] not in use", null);
				}
				relId = relRecord.getId();
				firstNode = relRecord.getFirstNode();
				secondNode = relRecord.getSecondNode();
				if (relRecord.getFirstPrevRel() != nodeCache.get(firstNode) || 
						relRecord.getSecondPrevRel() != nodeCache.get(secondNode)){
					relRecord.setFirstPrevRel(nodeCache.get(firstNode));
					relRecord.setSecondPrevRel(nodeCache.get(secondNode));
					neoStore.getRelationshipStore().updateRecord(relRecord);
				}
				nodeCache.put(firstNode, relId);
				nodeCache.put(secondNode, relId);						
				if (id % 10000000 == 0){
					current = System.currentTimeMillis();
					System.out.println("\tCompleted linking "+(maxRelId-id)+ " links in "+(current-startLinkBack)+" ms ["+(current-prev)+"]");
					prev = current;
				}
			} catch (Exception e){
				throw new BatchImportException("[Error in link back logic]"+ e.getMessage());
			}
		}
		return errorCount;
	}
}
