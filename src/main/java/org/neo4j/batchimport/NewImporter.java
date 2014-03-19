package org.neo4j.batchimport;

import java.io.*;
import java.sql.Timestamp;
import java.util.*;

import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.index.MapDbCachingIndexProvider;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.batchimport.newimport.stages.*;
import org.neo4j.batchimport.newimport.structs.*;
import org.neo4j.batchimport.newimport.utils.Utils;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterWrapper;
import static org.neo4j.batchimport.Utils.join;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class NewImporter {
    private static final Map<String, String> SPATIAL_CONFIG = Collections.singletonMap(IndexManager.PROVIDER,"spatial");
    public static final int BATCH =  1 * 1000 * 1000;
    private static Report report;
    private final Config config;
    private BatchInserterIndexProvider indexProvider;
    Map<String,BatchInserterIndex> indexes=new HashMap<String, BatchInserterIndex>();
    private static long startImport = 0;
    
    private BatchInserterImpl batchInserter;
    private BatchInserterWrapper db;

    public NewImporter(File graphDb, final Config config) {
        this.config = config;
		Timestamp ts = new Timestamp(System.currentTimeMillis());
        System.out.println("[Current time:"+ts.toString()+"][Compile Time:"+Utils.getVersionfinal(this.getClass())+"]");
        batchInserter = createBatchInserter(graphDb, config);
        db = new BatchInserterWrapper(batchInserter);

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
        return luceneOnlyIndex ? new LuceneBatchInserterIndexProvider(batchInserter) : new MapDbCachingIndexProvider(batchInserter);
    }

    protected BatchInserterImpl createBatchInserter(File graphDb, Config config) {
       return (BatchInserterImpl)BatchInserters.inserter(graphDb.getAbsolutePath(), config.getConfigData());
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

    	NewImporter importer = new NewImporter(graphDb, config);
    	importer.doImport();
    	System.out.println("Total time taken: "+(System.currentTimeMillis()-startImport));
    }
 
    void finish() {
        indexProvider.shutdown();
        batchInserter.shutdown();
        report.finish();
    }

    private LineData createLineData(BufferedReader reader, int offset) {
        final boolean useQuotes = config.quotesEnabled();
        if (useQuotes) return new CsvLineData(reader, config.getDelimChar(),offset);
        return new ChunkerLineData(reader, config.getDelimChar(), offset);
    }

    void importIndex(String indexName, BatchInserterIndex index, BufferedReader reader) throws IOException {
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
        importIndex(indexInfo.indexName, indexes.get(indexInfo.indexName), Utils.createFileBufferedReader(indexFile));
    }
    
    private void report(String header, long prevTime){
    	 System.out.println(header +(System.currentTimeMillis()- prevTime)/1000+ " secs - ["+Utils.getMaxIds(batchInserter.getNeoStore())+"]");
    }
    private void doImport() throws IOException {
    	long prevMilestone = System.currentTimeMillis();
    	long prevMilestone1 = prevMilestone;
    	try {
    		Stages importStages = setupStagesForNodes();
    		for (File file : config.getNodesFiles()) {
    			importNew(Utils.createFileBufferedReader(file), importStages);
    			report("\tNode file ["+file.getName()+"] imported in ", prevMilestone);
    			prevMilestone = System.currentTimeMillis();
    		}
    		report("Node Import complete in ", prevMilestone1);
    		prevMilestone = prevMilestone1 = System.currentTimeMillis();
    		setupStagesForRelationships(importStages);
    		for (File file : config.getRelsFiles()) {
    			importNew(Utils.createFileBufferedReader(file), importStages);
    			report("\tRelationship file ["+file.getName()+"] imported in ", prevMilestone);
    			prevMilestone = System.currentTimeMillis();
    		}
    		importStages.stop();
    		db.linkBackRelationships();
    		report("Relationship Import complete in " , prevMilestone1);
    		report("Import complete with Indexing pending in " , startImport);
    		for (IndexInfo indexInfo : config.getIndexInfos()) {
    			if (indexInfo.shouldImportFile()) importIndex(indexInfo);
    		} 
    	} catch (BatchImportException be){            	
    		System.out.println("[Batch Import failed]"+be.getMessage());
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
    			StageMethods.WriterStage.class.getMethod("writeNode", parameterTypes),
    			StageMethods.WriterStage.class.getMethod("writeRelationship", parameterTypes));
    	} catch (Exception e){
    		throw new BatchImportException("[Writer setup failed]"+e.getMessage());
    	} 
    	return writerStage;
    }

    private void importNew(BufferedReader reader, Stages importStages)throws BatchImportException{
    	ReadFileData input = new ReadFileData(reader, 
				config.getDelimChar(), 0, config.quotesEnabled());
    	db.setDataInput(input);
    	importStages.start(input);
		importStages.pollResults(batchInserter);
    }
    
    private Stages setupStagesForNodes()throws BatchImportException{
    	Stages importStages = new Stages(new StageMethods(db, batchInserter, indexes));
		WriterStage writerStage = setWriterStage(importStages);
		db.setDiskBlockingQ(writerStage.getDiskBlockingQ());
		importStages.setDataBuffers(writerStage.getDiskRecordsCache());
		try {
    		Class[] parameterTypes = new Class[2];
    		parameterTypes[0] = ReadFileData.class;
    		parameterTypes[1] = CSVDataBuffer.class;
    		importStages.init(Constants.NODE, StageMethods.ImportNode.class.getMethod("stage0", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage1", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage2", parameterTypes),
    				StageMethods.ImportNode.class.getMethod("stage3", parameterTypes));
    		importStages.setSingleThreaded(false, true, false, false);
    	}catch (Exception e){
    		throw new BatchImportException("[Nodes setup failed]"+e.getMessage());
    	} 
		// start writers
		writerStage.start();
		return importStages;
    }
    
    private void setupStagesForRelationships(Stages importStages)throws BatchImportException{
    	NodesCache nodeCache = new NodesCache(batchInserter.getNeoStore().getNodeStore().getHighId());
		db.setNodesCache(nodeCache);
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
}