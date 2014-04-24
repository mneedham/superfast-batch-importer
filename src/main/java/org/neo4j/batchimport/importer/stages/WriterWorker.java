package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.importer.utils.Utils;

public class WriterWorker extends java.lang.Thread
{
    protected int workerType;
    protected String threadName;
    protected Stages stages;
    protected WriterStages writerStages;
    protected int writerIndex;

    WriterWorker( int writerIndex, int type, Stages stages, WriterStages writerStages )
    {
        this.writerIndex = writerIndex;
        this.workerType = type;
        this.stages = stages;
        this.writerStages = writerStages;
    }

    public void run()
    {
        threadName = "Writer_" + Constants.RECORD_TYPE_NAME[workerType];
        Thread.currentThread().setName( threadName );
        this.setPriority( NORM_PRIORITY + 2 );
        while ( !isDone() )
        {
            try
            {
                DiskRecordsBuffer buf = writerStages.getDiskBlockingQ().getBuffer( workerType );
                if ( buf == null )
                {
                    continue;
                }
                try
                {
                    writerStages.writerMethods[writerIndex].execute( writerStages.db, buf );
                }
                catch ( Exception e )
                {
                    Utils.SystemOutPrintln( "Writer Method Failed :" + e.getMessage() );
                }
                writerStages.getRunData( writerIndex ).linesProcessed += buf.getRecordCount();
                buf.cleanup();
                writerStages.getDiskRecordsCache().putDiskRecords( buf, workerType );//diskRecCache.putDiskRecords(buf, workerType);
            }
            catch ( Exception e )
            {
                Utils.SystemOutPrintln( "Writer died:" + threadName );
                break;
            }
        }
    }

    public boolean isDone()
    {
        return !stages.moreWork();
    }
}
