package org.neo4j.batchimport.importer.stages;

import java.util.Map;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public class StageContext
{
    protected BatchInserterImplNew newBatchImporter;
    protected BatchInserterImpl batchInserter;
    private Map<String, BatchInserterIndex> indexes;
    protected MultiStage stages;

    public StageContext( BatchInserterImplNew newBatchImporter,
                         BatchInserterImpl batchImp,
                         Map<String, BatchInserterIndex> indexes )
    {
        this.newBatchImporter = newBatchImporter;
        this.batchInserter = batchImp;
        this.indexes = indexes;
    }

    public void setStages(MultiStage stages){
        this.stages = stages;
    }
    public static void dataExtract( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
    {
        try
        {
            boolean hasMoreData = input.fillBuffer( buf );
            if ( !hasMoreData )
            {
                buf.setMoreData( hasMoreData );
            }
        }
        catch ( Exception ioe )
        {
            ioe.printStackTrace();
            throw new BatchImportException( "[Bad input data]" + ioe.getMessage() );
        }
    }

    public BatchInserterIndex indexFor( String index )
    {
        return indexes.get( index );
    }

}
