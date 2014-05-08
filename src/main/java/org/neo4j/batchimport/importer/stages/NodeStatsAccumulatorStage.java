package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;

public enum NodeStatsAccumulatorStage implements Stage
{
    Stage0
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    StageContext.dataExtract( input, buf );
                }
            },
    Stage1
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    BatchInserterImplNew db = stageContext.newBatchImporter;
                    db.accumulateNodeCount( buf );
                    //db.getNodeCache().calculateDenseNodeThreshold( (int)db.getNeoStore().getRelationshipTypeStore().getHighId() );
                }
            }
}
/*
{
    Accumulate
    {
        public void execute(Config config, BatchInserterImplNew db, Reader nodesInput) throws BatchImportException
        {
            ReadFileData input =  new ReadFileData( new BufferedReader( nodesInput, Constants.BUFFERED_READER_BUFFER ),
                    config.getDelimChar(), 3, config.quotesEnabled() );
           
            db.accumulateNodeCount( input );
            db.getNodeCache().calculateDenseNodeThreshold( (int)db.getNeoStore().getRelationshipTypeStore().getHighId() );
        }
    }
}
*/