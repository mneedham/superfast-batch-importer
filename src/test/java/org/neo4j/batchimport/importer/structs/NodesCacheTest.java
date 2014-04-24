package org.neo4j.batchimport.importer.structs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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

    @Test
    public void shouldCountRelationships() throws Exception
    {
        // given
        NodesCache cache = new NodesCache( 100 );

        // when
        assertEquals( 0, cache.getCount( 1 ) );
        cache.incrementCount( 1 );
        assertEquals( 1, cache.getCount( 1 ) );
        cache.incrementCount( 1 );
        assertEquals( 2, cache.getCount( 1 ) );
        cache.incrementCount( 1 );
        assertEquals( 3, cache.getCount( 1 ) );

        cache.decrementCount( 1 );
        assertEquals( 2, cache.getCount( 1 ) );
        cache.decrementCount( 1 );
        assertEquals( 1, cache.getCount( 1 ) );
        cache.decrementCount( 1 );
        assertEquals( 0, cache.getCount( 1 ) );
    }

    @Test
    public void shouldPreventNegativeCount() throws Exception
    {
        // given
        NodesCache cache = new NodesCache( 100 );
        assertEquals( 0, cache.getCount( 1 ) );

        // when
        try
        {
            cache.decrementCount( 1 );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    public void shouldPreventOverflowCount() throws Exception
    {
        // given
        NodesCache cache = new NodesCache( 100 );
        cache.changeCount( 1, cache.maxCount() );
        assertEquals( Math.pow( 2, 29 ) - 1, cache.getCount( 1 ), 0.1 );

        // when
        try
        {
            cache.incrementCount( 1 );
            fail( "Should have thrown exception" );
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }
}
