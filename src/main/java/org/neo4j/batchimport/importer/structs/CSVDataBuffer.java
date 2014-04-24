package org.neo4j.batchimport.importer.structs;

public class CSVDataBuffer extends AbstractDataBuffer
{

    public CSVDataBuffer( int maxEntries, int bufSize, DiskRecordsCache diskCache, int numberOfDiskRecordBuffers )
    {
        super( maxEntries, bufSize, diskCache, numberOfDiskRecordBuffers );
    }

    public long setIdFromData( int index, int column )
    {
        this.id[index] = getLong( index, 0 );
        return id[index];
    }
}
