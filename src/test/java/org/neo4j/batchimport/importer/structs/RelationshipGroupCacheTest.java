package org.neo4j.batchimport.importer.structs;

import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;

import static org.junit.Assert.assertEquals;

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
        long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1 );
        assertEquals( relId, previousRel );
    }

    @Test
    public void shouldStoreRelationshipsOfDifferentTypes() throws Exception
    {
        // given
        long nodeCount = 1000;
        int type1 = 1;
        int type2 = 2;
        Direction direction = Direction.INCOMING;
        IdSequence assigner = new CapturingRelationshipGroupIdAssigner();
        RelationshipGroupCache cache = new RelationshipGroupCache( nodeCount, assigner );

        // then
        long type1RelId = 15;
        long type2RelId = 22;
        long relGroupCachePosition = cache.allocate( type1, direction, type1RelId );
        {
            long previousRel = cache.put( relGroupCachePosition, type1, direction, type1RelId + 1 );
            assertEquals( type1RelId, previousRel );
        }
        {
            long noSuchRel = cache.put( relGroupCachePosition, type2, direction, type2RelId );
            assertEquals( -1, noSuchRel );
            long previousRel = cache.put( relGroupCachePosition, type2, direction, type2RelId + 1 );
            assertEquals( type2RelId, previousRel );
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
        {
            long previousRel = cache.put( relGroupCachePosition, type, direction1, type1RelId + 1 );
            assertEquals( type1RelId, previousRel );
        }
        {
            long noSuchRel = cache.put( relGroupCachePosition, type, direction2, type2RelId );
            assertEquals( -1, noSuchRel );
            long previousRel = cache.put( relGroupCachePosition, type, direction2, type2RelId + 1 );
            assertEquals( type2RelId, previousRel );
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
            long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1 );
            assertEquals( relId, previousRel );
        }
        {
            long relId = 22;
            long relGroupCachePosition = cache.allocate( type, direction, relId );
            long previousRel = cache.put( relGroupCachePosition, type, direction, relId + 1 );
            assertEquals( relId, previousRel );
        }
    }

    @Test
    public void shouldRelease() throws Exception
    {
        // given


        // when

        // then
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
