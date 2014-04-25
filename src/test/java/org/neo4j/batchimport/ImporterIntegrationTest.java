package org.neo4j.batchimport;

import java.io.File;
import java.util.Map;

import org.junit.Test;

import org.neo4j.consistency.ConsistencyCheckService;
import org.neo4j.consistency.ConsistencyCheckSettings;
import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.tooling.GlobalGraphOperations;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

/**
 * @author Michael Hunger @since 05.11.13
 */
public class ImporterIntegrationTest
{
    public static final String DB_DIRECTORY = "target/index-reuse1.db";
    public static final int NODE_COUNT = 50;
    public static final int RELATIONSHIPS_PER_NODE = 10;

    @Test
    public void shouldImportData() throws Exception
    {
        // given
        FileUtils.deleteRecursively( new File( DB_DIRECTORY ) );
        DataGenerator.main( "dir=" + System.getProperty( "user.dir" ), "nodes=" + NODE_COUNT,
                "relsPerNode=" + RELATIONSHIPS_PER_NODE,
                "relTypes=4", "unsorted" );

        // when
        Importer.main( "-db-directory", DB_DIRECTORY, "-nodes", "nodes.csv", "-rels", "rels.csv" );

        // then
        assertDatabaseConsistent();
        assertDatabaseContainsData();
    }

    private void assertDatabaseConsistent() throws ConsistencyCheckIncompleteException
    {
        Map<String, String> specifiedProperties = stringMap();
        specifiedProperties.put( GraphDatabaseSettings.store_dir.name(), DB_DIRECTORY );

        ConsistencyCheckService.Result result = new ConsistencyCheckService().runFullConsistencyCheck(
                DB_DIRECTORY,
                new Config( specifiedProperties, GraphDatabaseSettings.class, ConsistencyCheckSettings.class ),
                ProgressMonitorFactory.textual( System.err ),
                StringLogger.SYSTEM );
        assertTrue( result.toString(), result.isSuccessful() );
    }

    private void assertDatabaseContainsData()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_DIRECTORY );
        try ( Transaction ignored = db.beginTx() )
        {
            assertEquals( NODE_COUNT, count( GlobalGraphOperations.at( db ).getAllNodes() ) );
            int relationshipCount = count( GlobalGraphOperations.at( db ).getAllRelationships() );
            assertTrue( format( "Expected at least half of %d but got %d",
                    NODE_COUNT * RELATIONSHIPS_PER_NODE, relationshipCount ),
                    NODE_COUNT * RELATIONSHIPS_PER_NODE / 2 < relationshipCount );
        }
        finally
        {
            db.shutdown();
        }
    }
}
