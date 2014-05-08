package org.neo4j.batchimport.importer.stages;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.structs.Constants;
import org.neo4j.batchimport.importer.structs.Constants.ImportStageState;
import org.neo4j.batchimport.importer.structs.DataBufferBlockingQ;
import org.neo4j.batchimport.importer.structs.DiskRecordsCache;
import org.neo4j.batchimport.importer.structs.RunData;
import org.neo4j.batchimport.importer.utils.Utils;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;

import static java.lang.String.format;

public class MultiStage
{
    private ImportStageState stageState = ImportStageState.Uninitialized;
    private DataBufferBlockingQ<CSVDataBuffer> bufferQ;
    private int threadCount;
    private RunData[][] stageRunData;
    private boolean[] stageComplete;
    private int numStages;
    private ImportWorker[] importWorkers = null;
    private Stage[] importStageMethods;
    private CSVDataBuffer[] bufArray;
    private boolean moreWork = true;
    private ReadFileData currentInput = null;
    private long startImport = System.currentTimeMillis();
    protected CountDownLatch startStagesSyncPoint = new CountDownLatch( 1 );
    private StageContext stageContext;
    private WriterStages writerStages;
    private String threadException = null;

    public MultiStage( StageContext stageContext )
    {
        this.stageContext = stageContext;
    }

    public ImportStageState getState()
    {
        return stageState;
    }
    
    Thread.UncaughtExceptionHandler uncaughtException = new Thread.UncaughtExceptionHandler() {
        public void uncaughtException(Thread th, Throwable ex) {
            threadException = ex.getMessage();
            System.out.println("Uncaught exception: " + ex);
            stopWorkers();
            stop();
        }
    };
    

    public void init( Constants.ImportStageState mode, Stage... methods )
    {
        stageState = mode;
        this.numStages = methods.length;
        threadCount = Math.max( Runtime.getRuntime().availableProcessors(), numStages );
        if ( importWorkers == null )
            importWorkers = new ImportWorker[threadCount];
        stageRunData = new RunData[numStages][threadCount];
        for ( int i = 0; i < numStages; i++ )
        {
            for ( int j = 0; j < threadCount; j++ )
            {
                stageRunData[i][j] = new RunData( i + "_" + j );
            }
        }
        stageComplete = new boolean[numStages];
        bufferQ = new DataBufferBlockingQ<>( numStages, Constants.BUFFERQ_SIZE, threadCount );
        initInputQ( bufferQ.getQ( 0 ), bufArray );
        importStageMethods = new Stage[methods.length];
        for ( int i = 0; i < methods.length; i++ )
        {
            importStageMethods[i] = methods[i];
        }
        for ( int i = 0; i < threadCount; i++ )
        {
            if ( importWorkers[i] != null )
            {
                importWorkers[i].setImportWorkers( importStageMethods );
            }
        }
    }

    public void setSingleThreaded( boolean... type )
    {
        for ( int i = 0; i < type.length; i++ )
        {
            bufferQ.setSingleThreaded( i, type[i] );
        }
    }

    public void setDataBuffers( DiskRecordsCache diskCache )
    {
        bufArray = createInputBufferArray( Constants.BUFFERQ_SIZE, diskCache );
    }

    public boolean moreWork()
    {
        return moreWork;
    }

    public void registerWriterStage( WriterStages writerStages )
    {
        this.writerStages = writerStages;
    }

    public void start( ReadFileData input )
    {
        for ( int i = 0; i < stageComplete.length; i++ )
        {
            stageComplete[i] = false;
        }
        currentInput = input;
        setDataInput( currentInput, bufArray );
        startWorkers();
        bufferQ.reset();
        assignThreadsToStages();
        startStagesSyncPoint.countDown();
        startStagesSyncPoint = new CountDownLatch( 1 );
    }

    private void startWorkers()
    {
        for ( int i = 0; i < threadCount; i++ )
        {
            if ( importWorkers[i] == null )
            {
                importWorkers[i] = new ImportWorker( currentInput, i, this );
                importWorkers[i].setImportWorkers( importStageMethods );
                importWorkers[i].setUncaughtExceptionHandler(uncaughtException);
                importWorkers[i].start();
                try
                {
                    Thread.sleep( 200 );
                }
                catch ( Exception e )
                {
                    e.printStackTrace();
                }
            }
            else
            {
                importWorkers[i].setInput( currentInput );
                importWorkers[i].setImportWorkers( importStageMethods );
            }
        }
    }
    
    private void stopWorkers()
    {
        for ( int i = 0; i < threadCount; i++ )
        {
            if ( importWorkers[i] != null && importWorkers[i].isAlive())
            {
                importWorkers[i].interrupt();
            }
        }
    }

    private void assignThreadsToStages()
    {
        int threadIndex = 0, stageIndex = 0;
        //one thread to each stage
        for ( int i = 0; i < numStages; i++ )
        {
            importWorkers[threadIndex].setStageIndex( i, true );
            threadIndex++;
        }
        //remaining up to maxThreads/2, pin to multithreaded stages
        for ( ; threadIndex < this.threadCount / 2; stageIndex++ )
        {
            if ( !bufferQ.isSingleThreaded( stageIndex % numStages ) )
            {
                importWorkers[threadIndex].setStageIndex( stageIndex % numStages, true );
                threadIndex++;
            }
        }
        for ( ; threadIndex < this.threadCount; stageIndex++ )
        {
            if ( !bufferQ.isSingleThreaded( stageIndex % numStages ) )
            {
                importWorkers[threadIndex].setStageIndex( stageIndex % numStages, false );
                threadIndex++;
            }
        }
    }

    public ImportStageState stop()
    {
        moreWork = false;
        startStagesSyncPoint.countDown();
        return ImportStageState.Exited;
    }

    public DataBufferBlockingQ<CSVDataBuffer> getBufferQ()
    {
        return bufferQ;
    }

    public RunData getStageRunData( int index1, int index2 )
    {
        return stageRunData[index1][index2];
    }

    public void setStageComplete( int stageIndex, boolean value )
    {
        stageComplete[stageIndex] = value;
    }

    public StageContext getStageContext()
    {
        return stageContext;
    }

    private void setDataInput( ReadFileData input, CSVDataBuffer[] bufferArray )
    {
        for ( int i = 0; i < bufferArray.length; i++ )
        {
            bufferArray[i].initRecords( input.getHeaderLength() );
        }
    }

    private CSVDataBuffer[] createInputBufferArray( int size, DiskRecordsCache diskCache )
    {
        CSVDataBuffer[] bufferArray = new CSVDataBuffer[size];
        for ( int i = 0; i < size; i++ )
        {
            CSVDataBuffer buf = new CSVDataBuffer( Constants.BUFFER_ENTRIES, Constants.BUFFER_SIZE_BYTES, diskCache, 4 );
            bufferArray[i] = buf;
        }
        return bufferArray;
    }

    private void initInputQ( ArrayBlockingQueue<CSVDataBuffer> inputQ, CSVDataBuffer[] bufArray )
    {
        int size = inputQ.remainingCapacity();
        try
        {
            for ( int i = 0; i < size; i++ )
            {
                inputQ.put( bufArray[i] );
            }
            //System.out.println("EmptyQ Size:"+ inputQ.size());
        }
        catch ( InterruptedException ie )
        {
            Utils.SystemOutPrintln( "Interruped empty queue creation" );
        }
    }

    public boolean isComplete() throws BatchImportException
    {
        if (threadException != null)
            throw new BatchImportException(threadException);
        for ( boolean status : stageComplete )
        {
            if ( !status )
            {
                return false;
            }
        }
        return true;
    }

    public void pollResults( BatchInserterImpl batchInserter, String progressHeader ) throws Exception
    {
        long startTime = System.currentTimeMillis();
        long waitCount = 0, billion = 1;
        while ( true )
        {
            try
            {
                Thread.sleep( 500 );
                waitCount += 500;
                if ( Constants.debugData && Constants.printPollInterval > 0 )
                {
                    if ( waitCount % Constants.printPollInterval == 0 )
                    {
                        printRunData( false, this.numStages, startTime, batchInserter );
                    }
                }
                if ( waitCount % Constants.progressPollInterval == 0 && batchInserter.getNeoStore() != null )
                {
                    System.out.print( progressHeader+": [" + waitCount / 1000 + "] "
                            + Utils.getMaxIds( batchInserter.getNeoStore(), true ) + '\r' );
                }
                if ( batchInserter.getNeoStore() != null
                        && Utils.getTotalIds( batchInserter.getNeoStore() ) > billion * 1000000000 )
                {
                    // for strange reason, process abort for lack of memory.
                    // This is a forced gc every billion elements to avoid out-of-memory failures.
                    billion++;
                    System.gc();
                }
                if ( isComplete() )
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

    public void printRunData( boolean detailed, int stages, long startTime, BatchInserterImpl batchInserter )
    {
        long curTime = System.currentTimeMillis();
        long timeTaken = (curTime - startTime);
        System.out.println( "At time [" + (curTime - startImport) + " ms][" + timeTaken + " ms]" );
        System.out.println( "\t" + Utils.memoryStats() );
        StringBuilder str = new StringBuilder();
        str.append( "Q Stats" );
        for ( int i = 0; i < bufferQ.getLength(); i++ )
        {
            str.append( "[" + bufferQ.getQ( i ).size() + ":" + bufferQ.getThreadCount( i ) + "]" );
        }
        str.append( "][" );
        int[] threadAssigment = bufferQ.getThreadAssignment();
        for ( int i = 0; i < threadCount; i++ )
        {
            if ( i % 3 == 0 )
            {
                str.append( "\n" );
            }
            str.append( "\t(" + i + ":" + threadAssigment[i] + ":" + importWorkers[i].getCurrentMethod() + ")" );
        }
        str.append( "]" );
        System.out.println( "\t" + str );
        str.setLength( 0 );
        str.append( "StageStats[" + timeTaken + " ms]" );
        for ( int j = 0; j < stages; j++ )
        {
            long linesProcessed = 0;
            long snapRate = 1, rate = 1;
            if ( stageComplete[j] )
            {
                continue;
            }
            for ( int i = 0; i < stageRunData[j].length && stageRunData[j][i] != null; i++ )
            {
                linesProcessed += stageRunData[j][i].linesProcessed;
                rate += stageRunData[j][i].totalRate();
                snapRate += stageRunData[j][i].snapshotRate();
            }
            String type = "M";
            if ( bufferQ.isSingleThreaded( j ) )
            {
                type = "S";
            }
            str.append( format( "[%s:%d:%d:%d]", type, linesProcessed, rate, snapRate ) );
            if ( detailed )
            {
                for ( int i = 0; i < stageRunData[j].length && stageRunData[j][i] != null; i++ )
                {
                    stageRunData[j][i].printData( 0 );
                }
            }
        }
        System.out.println( "\t" + str );
        str.setLength( 0 );
        str.append( "Writers:" );
        for ( int i = 0; i < writerStages.numWriters; i++ )
        {
            str.append( format( "[%s:%d:%d]", Constants.RECORD_TYPE_NAME[writerStages.writerWorker[i].workerType],
                    writerStages.diskBlockingQ.getLength( writerStages.writerWorker[i].workerType ),
                    writerStages.getRunData( i ).linesProcessed ) );
        }
        System.out.println( "\t" + str );
        str.setLength( 0 );
        if ( batchInserter.getNeoStore() != null )
        {
            System.out.println( "\t" + "MaxIds:" + Utils.getMaxIds( batchInserter.getNeoStore() ) );
        }
    }
    
    public String getException(){
        return threadException;
    }
}
