package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.DiskBlockingQ;
import org.neo4j.batchimport.importer.structs.DiskRecordsCache;
import org.neo4j.batchimport.importer.structs.RunData;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;

public class WriterStages
{
    protected int numWriters = 0;
    protected DiskRecordsCache diskRecCache;
    protected DiskBlockingQ diskBlockingQ;
    protected WriterWorker[] writerWorker = null;
    protected WriterStage[] writerMethods = null;
    protected RunData[] writerRunData;
    protected MultiStage stages;
    protected BatchInserterImplNew db;

    public WriterStages( MultiStage stages, BatchInserterImplNew db )
    {
        this.stages = stages;
        this.db = db;
        diskRecCache = new DiskRecordsCache( Constants.BUFFERQ_SIZE * 2, Constants.BUFFER_ENTRIES );
        diskBlockingQ = new DiskBlockingQ( 3, Constants.BUFFERQ_SIZE );
        stages.registerWriterStage( this );
    }

    public void init( WriterStage... methods )
    {
        this.numWriters = methods.length;
        writerWorker = new WriterWorker[numWriters];
        writerMethods = new WriterStage[numWriters];
        writerRunData = new RunData[numWriters];
        for ( int i = 0; i < methods.length; i++ )
        {
            writerMethods[i] = methods[i];
        }
        for ( int i = 0; i < numWriters; i++ )
        {
            int type = 0;
            if ( writerMethods[i].name().contains( "Property" ) )
            {
                type = Constants.PROPERTY;
            }
            else if ( writerMethods[i].name().contains( "Node" ) )
            {
                type = Constants.NODE;
            }
            else if ( writerMethods[i].name().contains( "Relationship" ) )
            {
                type = Constants.RELATIONSHIP;
            }
            writerWorker[i] = new WriterWorker( i, type, stages, this );
            writerRunData[i] = new RunData( "Writer" + i );
        }
    }

    public void start()
    {
        for ( int i = 0; i < numWriters; i++ )
        {
            writerWorker[i].start();
        }
    }

    public DiskRecordsCache getDiskRecordsCache()
    {
        return diskRecCache;
    }

    public DiskBlockingQ getDiskBlockingQ()
    {
        return diskBlockingQ;
    }

    public RunData getRunData( int index )
    {
        return writerRunData[index];
    }

}
