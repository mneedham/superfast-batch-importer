package org.neo4j.batchimport;

import java.io.BufferedReader;
import java.io.Reader;

import org.neo4j.batchimport.importer.stages.ReadFileData;
import org.neo4j.batchimport.importer.stages.StageContext;
import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.NodesCache;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.kernel.api.Exceptions.BatchImportException;

public class NodeDegreeAccumulator
{
    private final Config config;
    private final NodesCache nodeCache;

    public NodeDegreeAccumulator( Config config, NodesCache nodeCache )
    {
        this.config = config;
        this.nodeCache = nodeCache;
    }

    public void accumulate( Reader nodesInput ) throws BatchImportException
    {
        CSVDataBuffer buffer = new CSVDataBuffer( Constants.BUFFER_ENTRIES,
                Constants.BUFFER_SIZE_BYTES, null, 0 );
        ReadFileData input =  new ReadFileData( new BufferedReader( nodesInput, Constants.BUFFERED_READER_BUFFER ),
                config.getDelimChar(), 3, config.quotesEnabled() );
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
            }
        }
        while ( buffer.isMoreData() );
        nodeCache.calculateDenseNodeThreshold( 0.02d );

        System.out.println( "dense node " + nodeCache.getDenseNodeCount() );
    }
}
