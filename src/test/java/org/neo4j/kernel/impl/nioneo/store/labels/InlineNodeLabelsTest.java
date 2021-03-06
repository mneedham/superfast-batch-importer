package org.neo4j.kernel.impl.nioneo.store.labels;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;

/**
 * @author Michael Hunger @since 02.11.13
 */
public class InlineNodeLabelsTest
{

    public static final List<DynamicRecord> NONE = Collections.<DynamicRecord>emptyList();
    private final NodeRecord nodeRecord = Mockito.mock( NodeRecord.class );
    private final InlineNodeLabels inlineNodeLabels = new InlineNodeLabels( 0L, nodeRecord );

    @Test
    public void testEncodeTooManyLabels() throws Exception
    {
        boolean result = inlineNodeLabels.tryInlineInNodeRecord( new long[]{3, 2, 6, 4, 150, 2, 5, 7}, NONE );
        assertEquals( false, result );
        Mockito.verifyZeroInteractions( nodeRecord );
    }

    @Test
    public void testEncodeTooLargeLabels() throws Exception
    {
        boolean result = inlineNodeLabels.tryInlineInNodeRecord( new long[]{3, 2, 6, 4, 150}, NONE );
        assertEquals( false, result );
        Mockito.verifyZeroInteractions( nodeRecord );
    }

    @Test
    public void testEncodeLabels() throws Exception
    {
        boolean result = inlineNodeLabels.tryInlineInNodeRecord( new long[]{3, 2, 6, 4}, NONE );
        assertEquals( true, result );
        Mockito.verify( nodeRecord ).setLabelField( eq( 275416351747L ), eq( NONE ) );
    }

    @Test
    public void testEncodeNoLabels() throws Exception
    {
        boolean result = inlineNodeLabels.tryInlineInNodeRecord( new long[]{}, NONE );
        assertEquals( true, result );
        Mockito.verify( nodeRecord ).setLabelField( eq( 0L ), eq( NONE ) );
    }

    @Test
    public void testPerformance() throws Exception
    {
        long[] ids = {3, 2, 6, 4};
        InlineNodeLabels inlineNodeLabels = new InlineNodeLabels( 0L, new NodeRecord( 0, false, -1, -1 ) );
        long time = System.currentTimeMillis();
        for ( int i = 0; i < 1_0000_000; i++ )
        {
            inlineNodeLabels.tryInlineInNodeRecord( ids, NONE );
        }
        time = System.currentTimeMillis() - time;
        System.out.println( "time = " + time );
    }
}
