package org.neo4j.batchimport.importer.stages;

import java.util.Map;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;

public enum ImportNodeStage implements Stage
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
                    try
                    {
                        for ( int index = 0; index < buf.getCurEntries(); index++ )
                        {
                            if ( input.hasId() )
                            {
                                stageContext.newBatchImporter.checkNodeId( buf.setIdFromData( index, 0 ) );
                            }
                            else
                            {
                                buf.setId( index, stageContext.batchInserter.getNeoStore().getNodeStore().nextId() );
                            }
                        }
                        stageContext.newBatchImporter.createNodeRecords( buf );
                    }
                    catch ( Exception e )
                    {
                        throw new BatchImportException( "[ImportNode Stage1 failed]", e );
                    }

                }
            },
    Stage2
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    try
                    {
                        stageContext.newBatchImporter.importEncodeProps( buf );
                    }
                    catch ( Exception e )
                    {
                        throw new BatchImportException( "[ImportNode Stage2 failed]", e );
                    }

                }
            },
    Stage3
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    try
                    {
                        stageContext.newBatchImporter.setPropIds( buf, false );
                        stageContext.newBatchImporter.importNode_writeStore( buf, false );
                    }
                    catch ( Exception e )
                    {
                        throw new BatchImportException( "[ImportNode Stage2 failed]", e );
                    }

                }
            },
    Stage4
            {
                @Override
                public void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
                {
                    ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
                    for ( int i = 0; i < buf.getCurEntries(); i++ )
                    {
                        Map<String, Map<String, Object>> entries = stageContext.newBatchImporter.getDataInput().getIndexData( buf, i );
                        for ( Map.Entry<String, Map<String, Object>> entry : entries.entrySet() )
                        {
                            String indexId = entry.getKey();
                            final BatchInserterIndex index = stageContext.indexFor( indexId );
                            if ( index == null )
                            {
                                throw new BatchImportException( "[Index " + entry.getKey() + " not configured." );
                            }
                            index.add( buf.getId( i ), entry.getValue() );
                        }
                    }
                }
            }
}
