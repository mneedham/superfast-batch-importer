package org.neo4j.batchimport.importer.stages;

import org.neo4j.batchimport.importer.structs.CSVDataBuffer;
import org.neo4j.kernel.api.Exceptions.BatchImportException;

public interface Stage
{
    String name();

    void execute( StageContext stageContext, ReadFileData input, CSVDataBuffer buf ) throws BatchImportException;
}
