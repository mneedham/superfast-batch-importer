package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.kernel.api.Exceptions.BatchImportException;

public class ImportWorker extends java.lang.Thread
{
    public static ThreadLocal<ImportWorker> threadImportWorker = new ThreadLocal<>();
    private Exception excep;
    private String name = "";
    private Stage[] importWorkerMethods;
    private ReadFileData input;
    private int stageIndex = -1;
    private final int threadIndex;
    private MultiStage stages;
    private boolean isPinned = false;
    private String threadCurrentMethod;

    ImportWorker( ReadFileData inp, int threadIndex, MultiStage stages )
    {
        input = inp;
        this.threadIndex = threadIndex;
        this.stages = stages;
    }

    public static void debugSetCurrentMethod()
    {
        if ( threadImportWorker.get() != null )
        {
            threadImportWorker.get().setCurrentMethod();
        }
    }

    public static void debugSetCurrentMethod( String method )
    {
        if ( threadImportWorker.get() != null )
        {
            threadImportWorker.get().setCurrentMethod( method );
        }
    }

    public int getThreadIndex()
    {
        return threadIndex;
    }

    public void setInput( ReadFileData inp )
    {
        input = inp;
    }

    public void setImportWorkers( Stage[] importWorkerMethods )
    {
        this.importWorkerMethods = importWorkerMethods;
    }

    public int getStageIndex()
    {
        return stageIndex;
    }

    public void setStageIndex( int stage, boolean pinned )
    {
        stageIndex = stage;
        if ( pinned )
        {
            isPinned = true;
        }
    }

    public boolean isPinned()
    {
        return isPinned;
    }

    public void setCurrentMethod( String tag )
    {
        // for debug info
        threadCurrentMethod = Utils.getCodeLocation( true, 3, tag );
    }

    public void setCurrentMethod()
    {
        // for debug info
        threadCurrentMethod = Utils.getCodeLocation( true, 4 );
    }

    public String getCurrentMethod()
    {
        // for debug info
        return threadCurrentMethod;
    }

    private void setThreadParams( CSVDataBuffer buffer )
    {
        if ( buffer == null )
            return;
        this.setCurrentMethod( " " + buffer.getBufSequenceId() );
        stageIndex = buffer.getStageIndex();
        if ( stages.getBufferQ().isSingleThreaded( stageIndex ) )
        {
            this.setPriority( Thread.NORM_PRIORITY + 1 );
        }
        else
        {
            this.setPriority( Thread.NORM_PRIORITY );
        }
        String methodName = importWorkerMethods[buffer.getStageIndex()].name();
        this.name = Thread.currentThread().getName() + "ImportNode_" + methodName;
    }

    private CSVDataBuffer readData() throws InterruptedException, BatchImportException
    {
        CSVDataBuffer buffer = null;
        while ( !isDone() && buffer == null )
        {
            try
            {
                buffer = stages.getBufferQ().getBuffer( this.stageIndex, this );
            }
            catch ( Exception e )
            {
                String errMsg = "Exception in getBuffer:" +e.getStackTrace()[2];
                Utils.SystemOutPrintln( errMsg + e.getMessage() );
                throw new BatchImportException( errMsg + e.getMessage() );
            }
            if ( buffer == null )
            {
                Thread.sleep( Constants.READ_THREAD_WAIT );
                continue;
            }
        }
        return buffer;
    }

    public void processData( CSVDataBuffer buffer ) throws BatchImportException
    {
        if ( buffer == null )
            return;
        try
        {
            importWorkerMethods[buffer.getStageIndex()].execute( stages.getStageContext(), this.input, buffer );
            stages.getStageRunData( buffer.getStageIndex(), threadIndex ).linesProcessed += buffer.getCurEntries();
        }
        catch ( Exception e )
        {
            this.excep = e;
            Utils.SystemOutPrintln( "Invoke stage method failed:" + name + ":" + e.getMessage() + ":"
                    + this.stages.getBufferQ().getThreadCount( stageIndex ) );
            throw new BatchImportException( e.getMessage() );
        }
    }

    public void writeData( CSVDataBuffer buffer ) throws InterruptedException
    {
        if ( buffer == null )
            return;
        stages.getBufferQ().putBuffer( buffer );
        this.setCurrentMethod();
    }

    private void lastBufferProcessing( CSVDataBuffer buffer ) throws InterruptedException
    {
        if ( buffer == null )
            return;
        // if last buffer and buffer has entries
        if ( !buffer.isMoreData() && buffer.getCurEntries() > 0 )
        {
            stages.setStageComplete( buffer.getStageIndex(), true );
            if ( buffer.getStageIndex() == 0 )
            {
                //wait till all the fellow threads in stage 0 (reader) to complete.
                //wait count is to avoid indefinite wait.
                int waitCount = 200;
                while ( stages.getBufferQ().getThreadCount( 0 ) > 1 && waitCount-- > 0 )
                {
                    Thread.sleep( 100 );
                }
            }
        }
    }

    public void run()
    {
        threadImportWorker.set( this );
        Thread.currentThread().setName( name );
        CSVDataBuffer buffer = null;
        try
        {
            while ( !isDone() )
            {
                buffer = readData();
                setThreadParams( buffer );
                processData( buffer );
                lastBufferProcessing( buffer );
                writeData( buffer );
            }
        }
        catch ( Exception e )
        {
            this.excep = e;
            e.printStackTrace();
            Utils.SystemOutPrintln( "Import worker:" + name + ":" + e.getMessage() );
            throw new RuntimeException( e.getMessage() );
        }
    }

    public Exception getException()
    {
        return this.excep;
    }

    public boolean isDone() throws BatchImportException
    {
        if ( !stages.isComplete() )
        {
            return false;
        }
        //now check with parent (stages) if the thread has more work
        try
        {
            stages.startStagesSyncPoint.await();
        }
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        return !stages.moreWork();
    }
}
