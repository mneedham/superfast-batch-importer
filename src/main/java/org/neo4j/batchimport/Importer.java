package org.neo4j.batchimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.batchimport.importer.ChunkerLineData;
import org.neo4j.batchimport.importer.CsvLineData;
import org.neo4j.batchimport.importer.stages.ImportNodeStage;
import org.neo4j.batchimport.importer.stages.ImportRelationshipStage;
import org.neo4j.batchimport.importer.stages.ReadFileData;
import org.neo4j.batchimport.importer.stages.StageContext;
import org.neo4j.batchimport.importer.stages.MultiStage;
import org.neo4j.batchimport.importer.stages.WriterStage;
import org.neo4j.batchimport.importer.stages.WriterStages;
import org.neo4j.batchimport.importer.stages.NodeStatsAccumulatorStage;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.Constants.ImportStageState;
import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.importer.structs.NodesCache;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.batchimport.index.MapDbCachingIndexProvider;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;
import org.neo4j.unsafe.batchinsert.BatchInserterIndexProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.FULLTEXT_CONFIG;

public class Importer
{
    private static final Map<String, String> SPATIAL_CONFIG = Collections.singletonMap( IndexManager.PROVIDER,
            "spatial" );
    public static final int BATCH = 1 * 1000 * 1000;
    private static Report report;
    private final Config config;
    private final BatchInserterIndexProvider indexProvider;
    Map<String, BatchInserterIndex> indexes = new HashMap<>();
    public static long startImport = 0;
    private final BatchInserterImplNew db;
    private MultiStage importStages = null;
    private NodesCache nodeCache = null;
    private boolean relationLinkbackNeeded = false;

    public Importer( File graphDb, final Config config )
    {
        this.config = config;
        Timestamp ts = new Timestamp( System.currentTimeMillis() );
        System.out.println( "[Current time:" + ts.toString() + "][Compile Time:"
                + Utils.getVersionfinal( this.getClass() ) + "]" );
        setDebugInfo( config );
        db = createBatchInserter( graphDb, config );
        final boolean luceneOnlyIndex = config.isCachedIndexDisabled();
        indexProvider = createIndexProvider( luceneOnlyIndex );
        Collection<IndexInfo> indexInfos = config.getIndexInfos();
        if ( indexInfos != null )
        {
            for ( IndexInfo indexInfo : indexInfos )
            {
                BatchInserterIndex index = indexInfo.isNodeIndex() ? nodeIndexFor( indexInfo.indexName,
                        indexInfo.indexType ) : relationshipIndexFor( indexInfo.indexName, indexInfo.indexType );
                indexes.put( indexInfo.indexName, index );
            }
        }
        report = createReport();
    }

    protected StdOutReport createReport()
    {
        return new StdOutReport( BATCH, 1 );
    }

    protected BatchInserterIndexProvider createIndexProvider( boolean luceneOnlyIndex )
    {
        return luceneOnlyIndex ? new LuceneBatchInserterIndexProvider( db ) : new MapDbCachingIndexProvider( db );
    }

    protected BatchInserterImplNew createBatchInserter( File graphDb, Config config )
    {
        return new BatchInserterImplNew( graphDb.getAbsolutePath(), config.getConfigData() );
    }

    public static void main( String... args ) throws IOException
    {
        startImport = System.currentTimeMillis();
        System.err.println( "Neo4j Data Importer" );
        System.err.println( Config.usage() );
        System.err.println();
        Config config = null;
        try {
            config = Config.convertArgumentsToConfig( args );
        } catch (IllegalArgumentException ie){
            StringBuilder params = new StringBuilder();
            for (String str : args)
                params.append( str + " ");
            System.err.println("Illegal param: " + ie.getMessage() +"["+params+"]" );
            System.err.println( Config.usage() );
            return;
        }
        File graphDb = new File( config.getGraphDbDirectory() );
        if ( graphDb.exists() && !config.keepDatabase() )
        {
            FileUtils.deleteRecursively( graphDb );
        }
        Importer importer = new Importer( graphDb, config );
        importer.doImport();
        System.out.println( "Total time taken: " + (System.currentTimeMillis() - startImport) );
        checkConsistency( config );
    }

    void finish()
    {
        if ( importStages != null && importStages.getState() != ImportStageState.Exited )
        {
            importStages.stop();
        }
        indexProvider.shutdown();
        db.shutdown();
        report.finish();
    }

    private LineData createLineData( Reader reader, int offset )
    {
        final boolean useQuotes = config.quotesEnabled();
        if ( useQuotes )
        {
            return new CsvLineData( reader, config.getDelimChar(), offset );
        }
        return new ChunkerLineData( reader, config.getDelimChar(), offset );
    }

    void importIndex( String indexName, BatchInserterIndex index, Reader reader ) throws IOException
    {
        final LineData data = createLineData( reader, 1 );
        report.reset();
        while ( data.processLine( null ) )
        {
            final Map<String, Object> properties = data.getProperties();
            index.add( id( data.getValue( 0 ) ), properties );
            report.dots();
        }
        report.finishImport( "Done inserting into " + indexName + " Index" );
    }

    private BatchInserterIndex nodeIndexFor( String indexName, String indexType )
    {
        return indexProvider.nodeIndex( indexName, configFor( indexType ) );
    }

    private BatchInserterIndex relationshipIndexFor( String indexName, String indexType )
    {
        return indexProvider.relationshipIndex( indexName, configFor( indexType ) );
    }

    private Map<String, String> configFor( String indexType )
    {
        if ( indexType.equalsIgnoreCase( "fulltext" ) )
        {
            return FULLTEXT_CONFIG;
        }
        if ( indexType.equalsIgnoreCase( "spatial" ) )
        {
            return SPATIAL_CONFIG;
        }
        return EXACT_CONFIG;
    }

    private long id( Object id )
    {
        return Long.parseLong( id.toString() );
    }

    private void importIndex( IndexInfo indexInfo ) throws IOException
    {
        File indexFile = new File( indexInfo.indexFileName );
        if ( !indexFile.exists() )
        {
            System.err.println( "Index file " + indexFile + " does not exist" );
            return;
        }
        importIndex( indexInfo.indexName, indexes.get( indexInfo.indexName ), Utils.createFileReader( indexFile ) );
    }

    private void report( String header, long prevTime )
    {
        if ( prevTime == -1 )
        {
            System.out.print( Utils.getCurrentTimeStamp() + header );
        }
        else
        {
            System.out.print( Utils.getCurrentTimeStamp() + header + (System.currentTimeMillis() - prevTime) / 1000
                    + " secs - [" + Utils.getMaxIds( db.getNeoStore() ) + "]" );
        }
    }

    private void doImport() throws IOException
    {
        long stepTime = System.currentTimeMillis();
        long milestone = stepTime;
        try
        {
            for ( File file : config.getNodesFiles() )
            {
                importNodes( Utils.createFileReader( file ) );
                report( "\tNode file [" + file.getName() + "] imported in ", stepTime );
                stepTime = System.currentTimeMillis();
            }
            report( "Node Import complete in ", milestone );
            stepTime = milestone = System.currentTimeMillis();
            long nodeCount = db.getNeoStore().getNodeStore().getHighId();
            nodeCache = new NodesCache( nodeCount, db.getNeoStore() );
            db.setNodeCache( nodeCache );
            stepTime = System.currentTimeMillis();
            for ( File file : config.getRelsFiles() )
            {
                getRelStats( Utils.createFileReader( file ) );
                report( "\tRelationship file [" + file.getName() + "] scanned in ", stepTime );
                stepTime = System.currentTimeMillis();
            }
            nodeCache.calculateDenseNodeThreshold();
            nodeCache.createRelationshipGroupCache();
            for ( File file : config.getRelsFiles() )
            {
                importRelationships( Utils.createFileReader( file ) );
                report( "\tRelationship file [" + file.getName() + "] imported in ", stepTime );
                stepTime = System.currentTimeMillis();
            }
            importStages.stop();
            if ( relationLinkbackNeeded )
            {
                db.linkBackRelationships();
            }
            report( "Relationship Import complete in ", milestone );
            report( "Import complete with Indexing pending in ", startImport );
            for ( IndexInfo indexInfo : config.getIndexInfos() )
            {
                if ( indexInfo.shouldImportFile() )
                {
                    importIndex( indexInfo );
                }
            }
        }
        catch ( Exception be )
        {
            report( "[Batch Import failed]" + be.getMessage(), -1 );
        }
        finally
        {
            finish();
        }
    }

    private WriterStages setWriterStage( MultiStage importStages ) throws BatchImportException
    {
        WriterStages writerStages = new WriterStages( importStages, db );
        Class[] parameterTypes = new Class[1];
        parameterTypes[0] = DiskRecordsBuffer.class;
        try
        {
            writerStages.init( WriterStage.values() );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[Writer setup failed]", e );
        }
        return writerStages;
    }

    private MultiStage setupStages() throws BatchImportException
    {
        StageContext stageContext = new StageContext( db, db, indexes );
        int maxCPU = config.get("MaxCPU") == null ? -1 : Integer.parseInt(config.get("MaxCPU"));
        MultiStage importStages = new MultiStage( stageContext, maxCPU);
        stageContext.setStages( importStages );
        WriterStages writerStages = setWriterStage( importStages );
        db.setDiskBlockingQ( writerStages.getDiskBlockingQ() );
        importStages.setDataBuffers( writerStages.getDiskRecordsCache() );
        writerStages.start();
        return importStages;
    }

    public void importNodes( Reader reader ) throws Exception
    {
        if ( importStages == null )
            importStages = setupStages();
        if ( importStages.getState() != ImportStageState.NodeImport )
        {
            setupStagesForNodes();
        }
        importNew( reader, 0, "Node Import" );
    }

    public void importRelationships( Reader reader ) throws Exception
    {
        if ( importStages == null )
            importStages = setupStages();
        if ( importStages.getState() != ImportStageState.RelationshipImport )
        {
            setupStagesForRelationships();
        }
        importNew( reader, 3, "Relationship Import" );
    }

    public void getRelStats( Reader reader ) throws Exception
    {
        if ( importStages == null )
            importStages = setupStages();
        if ( importStages.getState() != ImportStageState.RelationshipPresacan )
        {
            setupStagesForRelationshipPrescan();
        }
        importNew( reader, 3, "Relationship Prescan" );
    }

    private void importNew( Reader reader, int offset, String message ) throws Exception
    {
        try
        {
            ReadFileData input = new ReadFileData( new BufferedReader( reader, Constants.BUFFERED_READER_BUFFER ),
                    config.getDelimChar(), offset, config.quotesEnabled() );
            db.setDataInput( input );
            importStages.start( input );
            importStages.pollResults( db, message );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[ImportNew failed]["+message+"]["+e.getMessage()+"]", e );
        }
    }

    private void setupStagesForNodes() throws BatchImportException
    {
        try
        {
            importStages.init( ImportStageState.NodeImport, ImportNodeStage.values() );
            importStages.setSingleThreaded( false, true, false, false, false );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[Nodes setup failed]["+e.getMessage()+"]", e );
        }
    }

    private void setupStagesForRelationships() throws BatchImportException
    {
        relationLinkbackNeeded = true;
        try
        {
            importStages.init( ImportStageState.RelationshipImport, ImportRelationshipStage.values() );
            importStages.setSingleThreaded( false, true, false, true, false );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[Relationship setup failed]["+e.getMessage()+"]", e );
        }
    }

    private void setupStagesForRelationshipPrescan() throws BatchImportException
    {
        try
        {
            importStages.init( ImportStageState.RelationshipPresacan, NodeStatsAccumulatorStage.values() );
            importStages.setSingleThreaded( false, true );
        }
        catch ( Exception e )
        {
            throw new BatchImportException( "[RelationshipPrescan setup failed]["+e.getMessage()+"]", e );
        }
    }

    private void setDebugInfo( Config config )
    {
        String debug = config.get( Config.BATCH_IMPORT_DEBUG_MODE );
        if ( debug != null )
        {
            String[] parts = debug.split( ":" );
            if ( parts[0].equalsIgnoreCase( "debug" ) )
            {
                Constants.debugData = true;
                try
                {
                    if ( parts.length > 1 )
                    {
                        Constants.printPollInterval = Integer.parseInt( parts[1] ) * 1000;
                    }
                    if ( parts.length > 2 )
                    {
                        Constants.progressPollInterval = Integer.parseInt( parts[2] ) * 1000;
                    }
                }
                catch ( Exception e )
                {
                    //do nothing, just ignore and leave the setting at default
                }
                System.out.println( "Debug Mode [Poll interval:" + Constants.printPollInterval / 1000
                        + " secs][Progess interval:" + Constants.progressPollInterval / 1000 + " secs]" );
            }
        }
    }

    static private void checkConsistency( Config config )
    {
        String check = config.get( Config.CHECK_CONSISTENCY );
        if ( check != null && check.equalsIgnoreCase( "true" ) )
        {
            Map<String, String> specifiedProperties = stringMap();
            specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), config.getGraphDbDirectory() );
            try
            {
                ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck( config
                        .getGraphDbDirectory(), new org.neo4j.kernel.configuration.Config( specifiedProperties,
                        GraphDatabaseSettings.class, ConsistencyCheckSettings.class ), ProgressMonitorFactory
                        .textual( System.err ), StringLogger.SYSTEM );
            }
            catch ( ConsistencyCheckIncompleteException ce )
            {
                System.out.println( "Could not do consistency check - " + ce.getMessage() );
            }
        }
    }
}
