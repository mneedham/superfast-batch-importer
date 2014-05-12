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
                int relsProcessed = 0;
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    BatchInserterImplNew db = stageContext.newBatchImporter;
                    db.accumulateNodeCount( buf );
                    relsProcessed += buf.getCurEntries();
                    stageContext.stages.setStatusMessage( "Processed ["+relsProcessed +"] relationships");
                }
            }
}
