package org.neo4j.batchimport;

import java.io.BufferedReader;
import java.io.Reader;

import org.neo4j.batchimport.importer.stages.ReadFileData;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.batchimport.utils.Config;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;

public class NodeDegreeAccumulator extends java.lang.Thread
{
    private final Config config;
    private final BatchInserterImplNew db;
    private ReadFileData input;
    private boolean isComplete = false;

    public NodeDegreeAccumulator( Config config, BatchInserterImplNew db )
    {
        this.config = config;
        this.db = db;
    }

    public void setInput( Reader nodesInput ) throws BatchImportException
    {
        input = new ReadFileData( new BufferedReader( nodesInput, Constants.BUFFERED_READER_BUFFER ),
                config.getDelimChar(), 3, config.quotesEnabled() );
    }

    @Override
    public void run()
    {
        try
        {
            db.accumulateNodeCount( input );
            db.getNodeCache().calculateDenseNodeThreshold(
                    (int) db.getNeoStore().getRelationshipTypeStore().getHighId() );
        }
        catch ( BatchImportException be )
        {
        }
        isComplete = true;
    }

    public void pollResults( String progressHeader ) throws Exception
    {
        long waitCount = 0;
        while ( true )
        {
            try
            {
                Thread.sleep( 500 );
                waitCount += 500;
                if ( waitCount % Constants.progressPollInterval == 0 && db.getNeoStore() != null )
                {
                    System.out.print( progressHeader + ": [" + waitCount / 1000 + "] "
                            + Utils.getMaxIds( db.getNeoStore(), true ) + '\r' );
                }
                if ( isComplete )
                {
                    return;
                }
            }
            catch ( Exception e )
            {
                Utils.SystemOutPrintln( e.getMessage() + " Poll thread exception" );
                throw e;
            }
        }
    }
}
