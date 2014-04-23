package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.batchimport.importer.utils.Utils;

public class WriterWorker extends java.lang.Thread
{
    protected int workerType;
    protected String threadName;
    protected Stages stages;
    protected WriterStage writerStage;
    protected int writerIndex;

    WriterWorker( int writerIndex, int type, Stages stages, WriterStage writerStage )
    {
        this.writerIndex = writerIndex;
        this.workerType = type;
        this.stages = stages;
        this.writerStage = writerStage;
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
                DiskRecordsBuffer buf = writerStage.getDiskBlockingQ().getBuffer( workerType );//diskRecordsQ
                // .getBuffer(workerType);
                if ( buf == null )
                {
                    continue;
                }
                Object[] parameters = new Object[1];
                parameters[0] = buf;
                try
                {
                    writerStage.writerMethods[writerIndex].invoke( stages.getStageMethods().writerStage, parameters );
                }
                catch ( Exception e )
                {
                    Utils.SystemOutPrintln( "Writer Method Failed :" + e.getMessage() );
                }
                writerStage.getRunData( writerIndex ).linesProcessed += buf.getRecordCount();
                buf.cleanup();
                writerStage.getDiskRecordsCache().putDiskRecords( buf, workerType );//diskRecCache.putDiskRecords(buf, workerType);
            }
            catch ( Exception e )
            {
                Utils.SystemOutPrintln( "Writer died:" + threadName );
                break;
            }
        }
        //System.out.println("Exiting writer "+ threadName);
    }

    public boolean isDone()
    {
        if ( stages.moreWork() )
        {
            return false;
        }
        return true;
    }
}
