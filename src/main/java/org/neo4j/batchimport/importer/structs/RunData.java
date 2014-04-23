package org.neo4j.batchimport.importer.structs;

import java.util.Iterator;
import java.util.LinkedHashMap;

public class RunData
{
    public static String QGET = "QGet";
    public static String QPUT = "QPut";
    public static String CLOCK = "myClock";

    public long[] maxId = new long[3];
    public int[][] recordCounts = new int[3][2];
    public long prevLinesProcessed, linesProcessed = 0;
    public long elapsedTime = 0;
    public long startTime = 0;
    public LinkedHashMap<String, long[]> tagMap = new LinkedHashMap<String, long[]>();
    public long prevPrint = 0, prevSnapshot = 0;
    public String name;
    public int[][] accessCount = new int[3][2];
    String currentClockTag = null;

    public RunData( String name )
    {
        long curTime = System.currentTimeMillis();
        this.startTime = curTime;
        prevPrint = prevSnapshot = curTime;
        this.name = name;
        this.startClock( CLOCK );

    }

    public long snapshotRate()
    {
        long temp = prevLinesProcessed;
        long curTime = System.currentTimeMillis();
        long timeTaken = curTime - prevSnapshot;
        prevSnapshot = curTime;
        prevLinesProcessed = linesProcessed;
        return ((linesProcessed - temp) / timeTaken) * 1000;
    }

    public long totalRate()
    {
        long curTime = System.currentTimeMillis();
        long timeTaken = curTime - startTime;
        return (linesProcessed / timeTaken) * 1000;
    }

    public void updateAccessCount( int recType, int accessType )
    {
        accessCount[recType][accessType]++;
    }

    public void startClock( String tag )
    {
        if ( !tagMap.containsKey( tag ) )
        {
            long[] timeTaken = new long[3];
            tagMap.put( tag, timeTaken );
        }
        long[] timeTaken = tagMap.get( tag );
        timeTaken[0] = System.currentTimeMillis();
        tagMap.put( tag, timeTaken );
    }

    public void switchClock( String tag )
    {
        long currentTime = System.currentTimeMillis();
        if ( currentClockTag != null && tag.equalsIgnoreCase( currentClockTag ) )
        {
            return;
        }
        if ( currentClockTag != null )
        {
            endClock( currentClockTag, currentTime );
        }
        if ( !tagMap.containsKey( tag ) )
        {
            long[] timeTaken = new long[3];
            tagMap.put( tag, timeTaken );
        }
        long[] timeTaken = tagMap.get( tag );
        timeTaken[0] = currentTime;
        tagMap.put( tag, timeTaken );
        currentClockTag = tag;
    }

    public void endClock( String tag, long time )
    {
        if ( !tagMap.containsKey( tag ) )
        {
            return;
        }
        long[] timeTaken = tagMap.get( tag );
        if ( time == 0 )
        {
            time = System.currentTimeMillis();
        }
        long curTime = time - timeTaken[0];
        timeTaken[2] += curTime;
        timeTaken[1] += curTime;
    }

    private String getPercent( long[] times, long timeInterval )
    {
        if ( times == null )
        {
            return "";
        }
        StringBuilder str = new StringBuilder();
        if ( timeInterval == 0 )
        {
            str.append( (times[1] / 1000) + "-0" );
        }
        else
        {
            long percent = ((times[2] * 100) / timeInterval) > 100 ? 100 : (times[2] * 100) / timeInterval;
            str.append( (times[1] / 1000) + "-" + percent + "%" );
        }
        return str.toString();
    }

    public boolean printData( int timeGap )
    {
        long currentTime = System.currentTimeMillis();
        if ( currentTime - prevPrint < timeGap * 1000 )
        {
            return false;
        }
        if ( linesProcessed <= 0 )
        {
            return true;
        }
        //---
        StringBuilder str = new StringBuilder();
        this.endClock( CLOCK, currentTime );
        long[] timeTakenAll = tagMap.get( CLOCK );
        long snapLines = (linesProcessed - prevLinesProcessed);
        long snapRate = 0;
        if ( timeTakenAll[1] != 0 )
        {
            snapRate = snapLines * 1000 / timeTakenAll[1];
        }
        prevLinesProcessed = linesProcessed;
        this.startClock( CLOCK );
        //---
        str.append( name + "[" + linesProcessed + "][/Sec:" + snapRate + "]" );
        prevPrint = currentTime;

        for ( int i = 0; i < 3; i++ )
        {
            recordCounts[i][1] += recordCounts[i][0];
            if ( recordCounts[i][1] > 0 )
            {
                str.append( Constants.RECORD_TYPE_NAME[i] + "[" + recordCounts[i][0] + ":" + recordCounts[i][1] + "]" );
            }
            recordCounts[i][0] = 0;
        }
        Iterator<String> ite = tagMap.keySet().iterator();
        long timeInterval = 0;
        while ( ite.hasNext() )
        {
            String key = ite.next();
            long[] times = tagMap.get( key );
            if ( key.equalsIgnoreCase( CLOCK ) )
            {
                timeInterval = times[2];
            }
            else
            {
                str.append( "[" + key + ":" + getPercent( tagMap.get( key ), timeInterval ) + "]" );
            }
            times[2] = 0;
            tagMap.put( key, times );
        }

        System.out.println( "\t" + str );
        return true;
    }
}