package org.neo4j.batchimport.importer.structs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NodesCacheTest
{
    @Test
    public void shouldStoreAndRetrieveRelationshipId() throws Exception
    {
        // given
        NodesCache cache = new NodesCache( 100 );

        // when
        cache.put( 1, 2 );

        // then
        long retrievedRelationshipId = cache.get( 1 );
        assertEquals( 2, retrievedRelationshipId );
    }

    @Test
    public void shouldRetrieveUnsetRelationshipId() throws Exception
    {
        // given
        NodesCache cache = new NodesCache( 100 );

        // then
        long retrievedRelationshipId = cache.get( 1 );
        assertEquals( -1, retrievedRelationshipId );
    }
}
