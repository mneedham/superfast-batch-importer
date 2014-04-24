package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.DiskRecordsBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;
import org.neo4j.unsafe.batchinsert.BatchInserterImplNew;

public enum WriterStage
{
    WriteProperty {
        public void execute( BatchInserterImplNew newBatchImporter, DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeProperty( buf );
        }
    },
    WriteNode {
        public void execute( BatchInserterImplNew newBatchImporter, DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeNode( buf );
        }
    },
    WriteRelationship {
        public void execute( BatchInserterImplNew newBatchImporter, DiskRecordsBuffer buf ) throws BatchImportException
        {
            newBatchImporter.writeRelationship( buf );
        }
    };

    abstract void execute( BatchInserterImplNew newBatchImporter, DiskRecordsBuffer buf ) throws BatchImportException;
}
