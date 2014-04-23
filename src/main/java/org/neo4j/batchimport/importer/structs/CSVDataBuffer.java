package org.neo4j.batchimport.importer.structs;

public class CSVDataBuffer extends AbstractDataBuffer
{

    public CSVDataBuffer( int maxEntries, int bufSize, int index, DiskRecordsCache diskCache )
    {
        super( maxEntries, bufSize, index, diskCache );
    }

    public long setIdFromData( int index, int column )
    {
        this.id[index] = getLong( index, 0 );
        return id[index];
    }
}
