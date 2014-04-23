package org.neo4j.batchimport;

import java.io.File;
import java.io.FileWriter;

import org.junit.Test;

import org.neo4j.consistency.ConsistencyCheckTool;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.junit.Assert.assertTrue;

/**
 * @author Michael Hunger @since 05.11.13
 */
public class NewImporterIntegrationTest
{

    public static final String DB_DIRECTORY = "target/index-reuse1.db";

    @Test
    public void testMain() throws Exception
    {
        FileUtils.deleteRecursively( new File( DB_DIRECTORY ) );
        NewTestDataGenerator.main( "dir=" + System.getProperty( "user.dir" ), "nodes=10000", "relsPerNode=10",
                "relTypes=4", "sorted" );
        Importer.main( "-db-directory", DB_DIRECTORY, "-nodes", "nodes.csv", "-rels", "rels.csv" );
        ConsistencyCheckTool.main( new String[]{DB_DIRECTORY} );
    }

    @Test
    public void testImportHashes() throws Exception
    {
        FileUtils.deleteRecursively( new File( DB_DIRECTORY ) );
        FileWriter writer = new FileWriter( "target/hashes.csv" );
        String testData = "a\n000000F8BE951D6DE6480F4AFDFB670C553E47C0\r\n0000021449360C1A398ED9A18800B2B13AA098A4\r" +
                "\n00000DABDE4C555FC82F7D534835247B94873C2C\r\n00001BE4128DB41729365A41D3AC1D019E5ED8A6\r\n";
        writer.write( testData );
        writer.close();
        Importer.main( "-db-directory", DB_DIRECTORY, "-nodes", "target/hashes.csv" );
        ConsistencyCheckTool.main( new String[]{DB_DIRECTORY} );
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( DB_DIRECTORY );
        try ( Transaction tx = db.beginTx() )
        {
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                Object value = node.getProperty( "a", null );
                assertTrue( value != null );
            }
            tx.success();
        }
        db.shutdown();
    }
}

