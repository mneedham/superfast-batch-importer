package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.utils.Utils;

public class ImportWorker extends java.lang.Thread
{
    public static ThreadLocal<ImportWorker> threadImportWorker = new ThreadLocal<>();
    private Exception excep;
    private String name = "";
    private boolean isRunning = false;
    private Stage[] importWorkerMethods;
    private ReadFileData input;
    private int stageIndex = -1;
    private final int threadIndex;
    private Stages stages;
    private boolean isPinned = false;
    private String threadCurrentMethod;

    ImportWorker( ReadFileData inp, int threadIndex, Stages stages )
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
        threadCurrentMethod = Utils.getCodeLocation( true, 3, tag );
    }

    public void setCurrentMethod()
    {
        threadCurrentMethod = Utils.getCodeLocation( true, 4 );
    }

    public String getCurrentMethod()
    {
        return threadCurrentMethod;
    }

    public void run()
    {
        threadImportWorker.set( this );
        stages.upThreadCount();
        isRunning = true;
        Thread.currentThread().setName( name );
        CSVDataBuffer buffer = null;

        while ( !isDone() )
        {
            try
            {
                try
                {
                    buffer = stages.getBufferQ().getBuffer( this.stageIndex, this );
                }
                catch ( Exception e )
                {
                    Utils.SystemOutPrintln( "Thread exception: in getBuffer" );
                }
                if ( buffer == null )
                {
                    Thread.sleep( 100 );
                    continue;
                }
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
                if ( buffer != null )
                {
                    try
                    {
                        importWorkerMethods[buffer.getStageIndex()].execute( stages.getStageContext(), this.input,
                                buffer );
                    }
                    catch ( Exception e )
                    {
                        this.excep = e;
                        e.printStackTrace();
                        Utils.SystemOutPrintln( "Invoke stage method failed:" + name + ":" + e.getMessage() + ":" +
                                this.stages.getBufferQ().getThreadCount( stageIndex ) );
                    }
                    stages.getStageRunData( buffer.getStageIndex(), threadIndex ).linesProcessed += buffer
                            .getCurEntries();
                    if ( !buffer.isMoreData() && buffer.getCurEntries() > 0 )
                    {
                        stages.setStageComplete( buffer.getStageIndex(), true );

                        if ( buffer.getStageIndex() == 0 )
                        {
                            //wait till all the fellow threads in stage 0 to complete.
                            //wait count is to avoid indefinite wait.
                            int waitCount = 200;
                            while ( stages.getBufferQ().getThreadCount( 0 ) > 1 && waitCount-- > 0 )
                            {
                                Thread.sleep( 100 );
                            }
                        }
                    }
                    buffer = stages.getBufferQ().putBuffer( buffer );
                    this.setCurrentMethod();
                }
            }
            catch ( Exception e )
            {
                this.excep = e;
                Utils.SystemOutPrintln( "Import worker:" + name + ":" + e.getMessage() );
                isRunning = false;
                //break;
            }
        }
        isRunning = false;
        stages.downThreadCount();
    }

    public Exception getException()
    {
        return this.excep;
    }

    public boolean isDone()
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
