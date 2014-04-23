package org.neo4j.batchimport.importer.stages;

import java.util.Map;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImpl;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;
import org.neo4j.unsafe.batchinsert.BatchInserterIndex;


public class StageMethods
{
    private BatchInserterImplNew newBatchImporter;
    private BatchInserterImpl batchInserter;
    private Map<String, BatchInserterIndex> indexes;
    protected ImportNode importNode = new ImportNode();
    protected ImportRelationship importRelationship = new ImportRelationship();
    protected WriterStage writerStage = new WriterStage();

    public StageMethods( BatchInserterImplNew newBatchImporter,
                         BatchInserterImpl batchImp,
                         Map<String, BatchInserterIndex> indexes )
    {
        this.newBatchImporter = newBatchImporter;
        this.batchInserter = batchImp;
        this.indexes = indexes;
    }

    //Extract
    public void DataExtract( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
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
            throw new BatchImportException( "[Bad input data]" + ioe.getMessage() );
        }
    }

    //Transform
    public class ImportNode
    {
        public void stage0( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            DataExtract( input, buf );
        }

        public void stage1( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            try
            {
                for ( int index = 0; index < buf.getCurEntries(); index++ )
                {
                    if ( input.hasId() )
                    {
                        newBatchImporter.checkNodeId( buf.setIdFromData( index, 0 ) );
                    }
                    else
                    {
                        buf.setId( index, batchInserter.getNeoStore().getNodeStore().nextId() );
                    }
                }
                newBatchImporter.createNodeRecords( buf );
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[ImportNode Stage1 failed]" + e.getMessage() );
            }
        }

        public void stage2( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            try
            {
                newBatchImporter.importEncodeProps( buf );
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[ImportNode Stage2 failed]" + e.getMessage() );
            }
        }

        public void stage3( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            try
            {
                newBatchImporter.setPropIds( buf, false );
                newBatchImporter.importNode_writeStore( buf, false );
            }
            catch ( Exception e )
            {
                throw new BatchImportException( "[ImportNode Stage2 failed]" + e.getMessage() );
            }
        }

        public void stage4( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            ImportWorker.threadImportWorker.get().setCurrentMethod( " " + buf.getBufSequenceId() );
            for ( int i = 0; i < buf.getCurEntries(); i++ )
            {
                Map<String, Map<String, Object>> entries = newBatchImporter.getDataInput().getIndexData( buf, i );
                for ( Map.Entry<String, Map<String, Object>> entry : entries.entrySet() )
                {
                    String indexId = entry.getKey();
                    Map<String, Object> value = entry.getValue();
                    final BatchInserterIndex index = indexFor( indexId );
                    if ( index == null )
                    {
                        throw new BatchImportException( "[Index " + entry.getKey() + " not configured." );
                    }
                    index.add( buf.getId( i ), entry.getValue() );
                }
            }
        }

        private BatchInserterIndex indexFor( String index )
        {
            return indexes.get( index );
        }
    }

    public class ImportRelationship
    {
        public void stage0( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            DataExtract( input, buf );
        }

        public void stage1( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            newBatchImporter.importRelationships_createRelationshipRecords( buf );
        }

        public void stage2( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            newBatchImporter.importEncodeProps( buf );
        }

        public void stage3( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            newBatchImporter.importRelationships_prepareRecords( buf );
        }

        public void stage4( ReadFileData input, CSVDataBuffer buf ) throws BatchImportException
        {
            newBatchImporter.importRelationships_writeStore( buf );
        }
    }

    //Load
    public class WriterStage
    {
        public void writeProperty( DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeProperty( buf );
        }

        public void writeNode( DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeNode( buf );
        }

        public void writeRelationship( DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeRelationship( buf );
        }
    }
}
