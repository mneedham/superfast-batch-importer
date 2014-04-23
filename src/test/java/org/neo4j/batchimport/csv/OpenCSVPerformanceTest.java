package org.neo4j.batchimport.csv;

import java.io.BufferedReader;
import java.io.FileReader;

import au.com.bytecode.opencsv.CSVReader;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.neo4j.batchimport.csv.PerformanceTestFile.COLS;
import static org.neo4j.batchimport.csv.PerformanceTestFile.ROWS;
import static org.neo4j.batchimport.csv.PerformanceTestFile.TEST_CSV;
import static org.neo4j.batchimport.csv.PerformanceTestFile.createTestFileIfNeeded;

/**
 * @author mh
 * @since 11.06.13
 */
@Ignore("Performance")
public class OpenCSVPerformanceTest
{

    @Before
    public void setUp() throws Exception
    {
        createTestFileIfNeeded();
    }

    @Test
    public void testReadLineWithCommaSeparator() throws Exception
    {
        final BufferedReader reader = new BufferedReader( new FileReader( TEST_CSV ) );
        final CSVReader csvReader = new CSVReader( reader, '\t', '"' );

        int res = 0;
        long time = System.currentTimeMillis();
        String[] line = null;
        while ( (line = csvReader.readNext()) != null )
        {
            res += line.length;
        }
        time = System.currentTimeMillis() - time;
        System.out.println( "time = " + time + " ms." );
        Assert.assertEquals( ROWS * COLS, res );
    }
}
