package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;

public enum ImportRelationshipStage implements Stage
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
                    stageContext.newBatchImporter.importRelationships_createRelationshipRecords( buf );
                }
            },
    Stage2
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    stageContext.newBatchImporter.importEncodeProps( buf );
                }
            },
    Stage3
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    stageContext.newBatchImporter.importRelationships_prepareRecords( buf );
                }
            },
    Stage4
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    stageContext.newBatchImporter.importRelationships_writeStore( buf );
                }
            }
}
