package org.neo4j.batchimport.importer.structs;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RelationshipGroupCacheTest
{
    @Test
    public void shouldStoreRelationships() throws Exception
    {
        // given
        long nodeCount = 1000;
        int type = 1;
        Direction direction = Direction.INCOMING;
        long relId = 15;
        IdSequence assigner = new CapturingRelationshipGroupIdAssigner();
        RelationshipGroupCache cache = new RelationshipGroupCache( nodeCount, assigner );

        // then
        long relGroupCachePosition = cache.allocate( type, direction, relId );
        long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1, true );
        assertEquals( relId, previousRel );
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentTypesInAscendingOrder() throws Exception
    {
        // given
        int[] types = new int[] {1,2};
        testRelationshipsOfDifferentTypes( types );
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentTypesInDescendingOrder() throws Exception
    {
        // given
        int[] types = new int[] {2,1};
        testRelationshipsOfDifferentTypes( types );
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentTypesInRandomOrder1() throws Exception
    {
        // given
        int[] types = new int[] {1,3,2};
        testRelationshipsOfDifferentTypes( types );
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentTypesInRandomOrder2() throws Exception
    {
        // given
        int[] types = new int[] {3,1,2};
        testRelationshipsOfDifferentTypes( types );
    }

    private void testRelationshipsOfDifferentTypes( int[] types )
    {
        long nodeCount = 1000;
        Direction direction = Direction.INCOMING;
        IdSequence assigner = new CapturingRelationshipGroupIdAssigner();
        RelationshipGroupCache cache = new RelationshipGroupCache( nodeCount, assigner );

        // then
        long relGroupCachePosition = -1;
        for ( int i = 0; i < types.length; i++ )
        {
            int type = types[i];
            long relId = 15*type;
            if ( i == 0 )
            {
                relGroupCachePosition = cache.allocate( type, direction, relId );
                long previousRel = cache.put( relGroupCachePosition, type, direction, relId+1, true );
                assertEquals( relId, previousRel );
            }
            else
            {
                long noSuchRel = cache.put( relGroupCachePosition, type, direction, relId, true );
                assertEquals( -1, noSuchRel );
                long previousRel = cache.put( relGroupCachePosition, type, direction, relId+1, true );
                assertEquals( relId, previousRel );
            }
        }
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentDirections() throws Exception
    {
        // given
        long nodeCount = 1000;
        int type = 1;
        Direction direction1 = Direction.INCOMING;
        Direction direction2 = Direction.OUTGOING;
        IdSequence assigner = new CapturingRelationshipGroupIdAssigner();
        RelationshipGroupCache cache = new RelationshipGroupCache( nodeCount, assigner );

        // then
        long type1RelId = 15;
        long type2RelId = 22;
        long relGroupCachePosition = cache.allocate( type, direction1, type1RelId );
        assertEquals( 1, cache.getCount( relGroupCachePosition, type, direction1 ) );
        {
            long previousRel = cache.put( relGroupCachePosition, type, direction1, type1RelId + 1, true );
            assertEquals( type1RelId, previousRel );
            assertEquals( 2, cache.getCount( relGroupCachePosition, type, direction1 ) );
        }
        {
            long noSuchRel = cache.put( relGroupCachePosition, type, direction2, type2RelId, true );
            assertEquals( -1, noSuchRel );
            assertEquals( 1, cache.getCount( relGroupCachePosition, type, direction2 ) );
            long previousRel = cache.put( relGroupCachePosition, type, direction2, type2RelId + 1, true );
            assertEquals( type2RelId, previousRel );
            assertEquals( 2, cache.getCount( relGroupCachePosition, type, direction2 ) );
        }
    }

    @Test
    public void shouldStoreRelationshipsForDifferentNodes() throws Exception
    {
        // given
        long nodeCount = 1000;
        int type = 1;
        Direction direction = Direction.INCOMING;
        IdSequence assigner = new CapturingRelationshipGroupIdAssigner();
        RelationshipGroupCache cache = new RelationshipGroupCache( nodeCount, assigner );

        // then
        {
            long relId = 15;
            long relGroupCachePosition = cache.allocate( type, direction, relId );
            long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1, true );
            assertEquals( relId, previousRel );
        }
        {
            long relId = 22;
            long relGroupCachePosition = cache.allocate( type, direction, relId );
            long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1, true );
            assertEquals( relId, previousRel );
        }
    }

    @Test
    public void shouldVisitNodeOnce() throws Exception
    {
        // GIVEN
        RelationshipGroupCache cache = new RelationshipGroupCache( 100, new CapturingRelationshipGroupIdAssigner() );
        long index = cache.allocate( 0, Direction.OUTGOING, 0 );
        cache.put( index, 1, Direction.OUTGOING, 10, true );
        cache.put( index, 2, Direction.OUTGOING, 12, true );

        // THEN
        assertCorrectVisit( cache, index, 1, Direction.OUTGOING );
        assertCorrectVisit( cache, index, 1, Direction.INCOMING );
        assertCorrectVisit( cache, index, 2, Direction.OUTGOING );
        assertCorrectVisit( cache, index, 2, Direction.INCOMING );
    }

    private void assertCorrectVisit( RelationshipGroupCache cache, long index, int type, Direction direction )
    {
        assertFalse( cache.checkAndSetVisited( index, type, direction ) );
        assertTrue( cache.checkAndSetVisited( index, type, direction ) );
        assertTrue( cache.checkAndSetVisited( index, type, direction ) );
    }

    private class CapturingRelationshipGroupIdAssigner implements IdSequence
    {
        private long nextId;

        @Override
        public long nextId()
        {
            return nextId++;
        }
    }
}
