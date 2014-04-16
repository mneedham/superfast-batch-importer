package org.neo4j.batchimport;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;


import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.importer.stages.*;
import org.neo4j.batchimport.importer.structs.*;
import org.neo4j.batchimport.importer.structs.Constants.ImportStageState;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.batchimport.index.MapDbCachingIndexProvider;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;
import static org.neo4j.batchimport.Utils.join;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer {
    private static final Map<String, String> SPATIAL_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"spatial");
    public static final int BATCH =  1 * 1000 * 1000;
    private static Report report;
    private final Config config;
    private BatchInserterIndexProvider indexProvider;
    Map<String,BatchInserterIndex> indexes=new HashMap<String, BatchInserterIndex>();
    public static long startImport = 0;
    
    private BatchInserterImplNew db;
    private Stages importStages = null;
    private NodesCache nodeCache = null;
    private boolean relationLinkbackNeeded = false;
    private ImportStageState importStageState = ImportStageState.Uninitialized;

    public Importer(File graphDb, final Config config) {
        this.config = config;
		Timestamp ts = new Timestamp(System.currentTimeMillis());
        System.out.println("[Current time:"+ts.toString()+"][Compile Time:"+Utils.getVersionfinal(this.getClass())+"]");
    	setDebugInfo(config);
        db = createBatchInserter(graphDb, config);
        final boolean luceneOnlyIndex = config.isCachedIndexDisabled();
        indexProvider = createIndexProvider(luceneOnlyIndex);
        Collection<IndexInfo> indexInfos = config.getIndexInfos();
        if (indexInfos!=null) {
            for (IndexInfo indexInfo : indexInfos) {
                BatchInserterIndex index = indexInfo.isNodeIndex() ? nodeIndexFor(indexInfo.indexName, indexInfo.indexType) : relationshipIndexFor(indexInfo.indexName, indexInfo.indexType);
                indexes.put(indexInfo.indexName, index);
            }
        }
        report = createReport();
    }

    protected StdOutReport createReport() {
        return new StdOutReport(BATCH, 1);
    }

    protected BatchInserterIndexProvider createIndexProvider(boolean luceneOnlyIndex) {
        return luceneOnlyIndex ? new LuceneBatchInserterIndexProvider(db) : new MapDbCachingIndexProvider(db);
    }

    protected BatchInserterImplNew createBatchInserter(File graphDb, Config config) {
    	return new BatchInserterImplNew(graphDb.getAbsolutePath(), config.getConfigData());
    }

    public static void main(String... args) throws IOException {
    	startImport= System.currentTimeMillis();
    	System.err.println("New Importer - parallel and scalable");
    	System.err.println("Usage: NewImporter data/dir nodes.csv relationships.csv [node_index node-index-name fulltext|exact nodes_index.csv rel_index rel-index-name fulltext|exact rels_index.csv ....]");
    	System.err.println("Using: NewImporter "+join(args," "));
    	System.err.println();

    	final Config config = Config.convertArgumentsToConfig(args);

    	File graphDb = new File(config.getGraphDbDirectory());
    	if (graphDb.exists() && !config.keepDatabase()) {
    		FileUtils.deleteRecursively(graphDb);
    	}

    	Importer importer = new Importer(graphDb, config);
    	importer.doImport();
    	System.out.println("Total time taken: "+(System.currentTimeMillis()-startImport));
    }
 
    void finish() {
    	if (importStages != null && importStages.getState() != ImportStageState.Exited)
    		importStages.stop();
        indexProvider.shutdown();
        db.shutdown();
        report.finish();
    }

    private LineData createLineData(Reader reader, int offset) {
        final boolean useQuotes = config.quotesEnabled();
        if (useQuotes) return new CsvLineData(reader, config.getDelimChar(),offset);
        return new ChunkerLineData(reader, config.getDelimChar(), offset);
    }

    void importIndex(String indexName, BatchInserterIndex index, Reader reader) throws IOException {
        final LineData data = createLineData(reader, 1);
        report.reset();
        while (data.processLine(null)) {
            final Map<String, Object> properties = data.getProperties();
            index.add(id(data.getValue(0)), properties);
            report.dots();
        }
                
        report.finishImport("Done inserting into " + indexName + " Index");
    }

    private BatchInserterIndex nodeIndexFor(String indexName, String indexType) {
        return indexProvider.nodeIndex(indexName, configFor(indexType));
    }

    private BatchInserterIndex relationshipIndexFor(String indexName, String indexType) {
        return indexProvider.relationshipIndex(indexName, configFor(indexType));
    }

    private Map<String, String> configFor(String indexType) {
        if (indexType.equalsIgnoreCase("fulltext")) return FULLTEXT_CONFIG;
        if (indexType.equalsIgnoreCase("spatial")) return SPATIAL_CONFIG;
        return EXACT_CONFIG;
    }

    private long id(Object id) {
        return Long.parseLong(id.toString());
    }

    private void importIndex(IndexInfo indexInfo) throws IOException {
        File indexFile = new File(indexInfo.indexFileName);
        if (!indexFile.exists()) {
            System.err.println("Index file "+indexFile+" does not exist");
            return;
        }
        importIndex(indexInfo.indexName, indexes.get(indexInfo.indexName), Utils.createFileReader(indexFile));
    }
    
    private void report(String header, long prevTime){
    	if (prevTime == -1)
    		System.out.println(Utils.getCurrentTimeStamp()+header);
    	else
    		System.out.println(Utils.getCurrentTimeStamp()+header +(System.currentTimeMillis()- prevTime)/1000+ " secs - ["+Utils.getMaxIds(db.getNeoStore())+"]");
    }
    private void doImport() throws IOException {
    	long stepTime = System.currentTimeMillis();
    	long milestone = stepTime;
    	try {
    		for (File file : config.getNodesFiles()) {
    			importNodes(Utils.createFileReader(file));
    			report("\tNode file ["+file.getName()+"] imported in ", stepTime);
    			stepTime = System.currentTimeMillis();
    		}
    		report("Node Import complete in ", milestone);
    		stepTime = milestone = System.currentTimeMillis();

    		for (File file : config.getRelsFiles()) {
    			importRelationships(Utils.createFileReader(file));
    			report("\tRelationship file ["+file.getName()+"] imported in ", stepTime);
    			stepTime = System.currentTimeMillis();
    		}
    		importStages.stop();
    		if (relationLinkbackNeeded)
    		db.linkBackRelationships();
    		report("Relationship Import complete in " , milestone);
    		report("Import complete with Indexing pending in " , startImport);
    		for (IndexInfo indexInfo : config.getIndexInfos()) {
    			if (indexInfo.shouldImportFile()) importIndex(indexInfo);
    		} 
    	} catch (BatchImportException be){            	
    		report("[Batch Import failed]"+be.getMessage(), -1);
    	}
    	finally {
    		finish();
    	}
    }

    private WriterStage setWriterStage(Stages importStages) throws BatchImportException{	
    	WriterStage writerStage = new WriterStage(importStages);
    	Class[] parameterTypes = new Class[1];
    	parameterTypes[0] = DiskRecordsBuffer.class;
    	try {
    	writerStage.init(StageMethods.WriterStage.class.getMethod("writeProperty", parameterTypes),
    			//StageMethods.WriterStage.class.getMethod("writeProperty", parameterTypes),
    			StageMethods.WriterStage.class.getMethod("writeNode", parameterTypes),
    			StageMethods.WriterStage.class.getMethod("writeRelationship", parameterTypes)
    			);
    	} catch (Exception e){
    		throw new BatchImportException("[Writer setup failed]"+e.getMessage());
    	} 
    	return writerStage;
    }

    private Stages setupStages() throws BatchImportException{
    	Stages importStages = new Stages(new StageMethods(db, db, indexes));
		WriterStage writerStage = setWriterStage(importStages);
		db.setDiskBlockingQ(writerStage.getDiskBlockingQ());
		importStages.setDataBuffers(writerStage.getDiskRecordsCache());
		writerStage.start();
		return importStages;
    }
    public void importNodes(Reader reader)throws BatchImportException{
    	if (importStages == null)
    		importStages = setupStages();
    	if (importStages.getState() != ImportStageState.NodeImport)
    		setupStagesForNodes();
    	importNew(reader, 0);
    }
    public void importRelationships(Reader reader)throws BatchImportException{
    	if (importStages == null)
    		importStages = setupStages();
    	if (importStages.getState() != ImportStageState.RelationshipImport)
    		setupStagesForRelationships();	
    	importNew(reader, 3);
    }
    private void importNew(Reader reader, int offset){
    	ReadFileData input = new ReadFileData( new BufferedReader(reader,Constants.BUFFERED_READER_BUFFER), 
				config.getDelimChar(), offset, config.quotesEnabled());
    	db.setDataInput(input);
    	importStages.start(input);
		importStages.pollResults(db);
    }
    
    private void setupStagesForNodes()throws BatchImportException{	
		try {
    		Class[] parameterTypes = new Class[2];
    		parameterTypes[0] = ReadFileData.class;
    		parameterTypes[1] = CSVDataBuffer.class;
    		importStages.init(Constants.NODE, StageMethods.ImportNode.class.getMethod("stage0", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage1", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage2", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage3", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage4", parameterTypes));
    		importStages.setSingleThreaded(false, true, false, false, false);
    		importStageState = ImportStageState.NodeImport;
    	}catch (Exception e){
    		throw new BatchImportException("[Nodes setup failed]"+e.getMessage());
    	} 
    }
    
    private void setupStagesForRelationships()throws BatchImportException{
    	nodeCache = new NodesCache(db.getNeoStore().getNodeStore().getHighId());
		db.setNodesCache(nodeCache);
    	relationLinkbackNeeded = true;
		try {
    		Class[] parameterTypes = new Class[2];
    		parameterTypes[0] = ReadFileData.class;
    		parameterTypes[1] = CSVDataBuffer.class;
    		importStages.init(Constants.RELATIONSHIP, StageMethods.ImportRelationship.class.getMethod("stage0", parameterTypes),
    				StageMethods.ImportRelationship.class.getMethod("stage1", parameterTypes),
    				StageMethods.ImportRelationship.class.getMethod("stage2", parameterTypes),
    				StageMethods.ImportRelationship.class.getMethod("stage3", parameterTypes),
    				StageMethods.ImportRelationship.class.getMethod("stage4", parameterTypes));
    		importStages.setSingleThreaded(false,true,false,true,false);
    	}catch (Exception e){
    		throw new BatchImportException("[Relationship setup failed]"+e.getMessage());
    	} 
    }
    
    private void setDebugInfo(Config config){
    	String debug = config.get(Config.BATCH_IMPORT_DEBUG_MODE);
    	if (debug != null){
    		String[] parts = debug.split(":");
    		if (parts[0].equalsIgnoreCase("debug")){
    			Constants.debugData = true;
    			try {
    				if (parts.length > 1)   				
    					Constants.printPollInterval = Integer.parseInt(parts[1]) * 1000;
    				if (parts.length > 2)   				
    					Constants.progressPollInterval = Integer.parseInt(parts[2]) * 1000;
    			} catch (Exception e){
    				//do nothing, just ignore and leave the setting at default
    			}
    			System.out.println("Debug Mode [Poll interval:"+Constants.printPollInterval/1000+" secs][Progess interval:"+Constants.progressPollInterval/1000+" secs]");
    		}
    	}
    }
}