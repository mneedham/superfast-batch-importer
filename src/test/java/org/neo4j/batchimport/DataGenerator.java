package org.neo4j.batchimport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.Random;

import org.junit.Ignore;

/**
 * @author mh
 * @since 13.01.12
 */
@Ignore
public class DataGenerator
{
    static int MAX_SHORT_STRING = -1;
    static int MAX_SHORT_STRING_SELECT = 0;
    private static int MAX_LABELS = 100;
    private static int MAX_ID = 1000000;
    private static final int NODES = 1 * 1000 * 1000;
    private static final int RELS_PER_NODE = 10;
    private static final String[] TYPES = {"ONE", "TWO", "THREE", "FOUR", "FIVE", "SIX", "SEVEN", "EIGHT", "NINE",
            "TEN"};
    private static Random rnd = new Random();
    static String dir = null;
    static int relsPerNode = RELS_PER_NODE;
    static int nodes = NODES;
    static String[] relTypes = TYPES;
    static String[] nodeLabels = genDistinctStrings( MAX_LABELS, true );
    static boolean sorted = false;
    static String[] columns = null;
    static int propsPerNode = 1, propsPerRel = 1;
    static public int startNodeId = 0;
    static long longPropCount = 0;
    static long propCount = 0;


    public static void main( String... args ) throws IOException
    {
        System.out.println( "Usage: TestDataGenerator NODES=n1 relsPerNode=n2 relTypes=n3 sorted" );
        long relCount = 0, time = System.currentTimeMillis();
        for ( String arg : args )
        {
            String[] argParts = arg.split( "=" );
            if ( argParts[0].equalsIgnoreCase( "startNodeId" ) )
            {
                startNodeId = Integer.parseInt( argParts[1] );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "dir" ) )
            {
                dir = argParts[1];
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "nodes" ) )
            {
                nodes = Integer.parseInt( argParts[1] );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "relsPerNode" ) )
            {
                relsPerNode = Integer.parseInt( argParts[1] );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "maxLabels" ) )
            {
                MAX_LABELS = Integer.parseInt( argParts[1] );
                nodeLabels = genDistinctStrings( MAX_LABELS );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "sorted" ) )
            {
                sorted = true;
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "relTypes" ) )
            {
                relTypes = genDistinctStrings( Integer.parseInt( argParts[1] ) );
                StringBuilder str = new StringBuilder();
                str.append( relTypes[0] );
                for ( int i = 1; i < relTypes.length; i++ )
                {
                    str.append( "," + relTypes[i] );
                }
                System.out.println( "RelationshipTypes used:" + str );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "columns" ) )
            {
                columns = argParts[1].split( "\t" );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "propsPerNode" ) )
            {
                propsPerNode = Integer.parseInt( argParts[1] );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "propsPerRel" ) )
            {
                propsPerRel = Integer.parseInt( argParts[1] );
                continue;
            }
            if ( argParts[0].equalsIgnoreCase( "maxPropSize" ) )
            {
                if ( argParts[1].contains( ":" ) )
                {
                    String[] props = argParts[1].split( ":" );
                    MAX_SHORT_STRING = Integer.parseInt( props[0] );
                    MAX_SHORT_STRING_SELECT = (int) Math.ceil( 100 / Float.valueOf( props[1] ) );
                }
                else
                {
                    MAX_SHORT_STRING = Integer.parseInt( argParts[1] );
                }
                continue;
            }
            System.out.println( "Unknown argument: " + arg );
        }
        String nodeHeader = "Node:ID\tRels\t" + getPropsHeader( propsPerNode,
                "Property" ) + "\tLabel:label\tCounter:int\n";
        String relHeader = "Start\tEnd\tType\t" + getPropsHeader( propsPerRel, "Property" ) + "\tCounter:long\n";
        //System.out.println("Using: TestDataGenerator "+nodes+" "+relsPerNode+" "+ Utils.join(relTypes, ",
        // ")+" "+(sorted?"sorted":""));
        File dirFile = new File( dir + File.separator + "nodes.csv" );
        FileOutputStream nodeFile = new FileOutputStream( dirFile );
        nodeFile.write( nodeHeader.getBytes() );
        FileOutputStream relFile = new FileOutputStream( dir + File.separator + "rels.csv" );
        relFile.write( relHeader.getBytes() );
        relCount = generate( nodeFile, relFile, nodes, startNodeId, relsPerNode, relTypes, sorted );
        nodeFile.close();
        relFile.close();
        long seconds = (System.currentTimeMillis() - time) / 1000;
        System.out.println( "Creating " + nodes + " Nodes and " + relCount + (sorted ? " sorted " : "") + " " +
                "Relationships with [" + propCount + ":" + longPropCount + "] took " + seconds + " seconds." );
    }

    private static long generate( FileOutputStream nodeFile, FileOutputStream relFile, int nodes, int startNodeId,
                                  int relsPerNode, String[] types, boolean sorted ) throws IOException
    {
        long relCount = 0;
        int numTypes = types.length;
        int bufLen = 1024 * 1024;
        long start = System.currentTimeMillis();
        StringBuilder strNode = new StringBuilder( bufLen );
        StringBuilder strRel = new StringBuilder( bufLen );
        String nextLine = null;
        String type = null;

        for ( int node = startNodeId; node < nodes + startNodeId; node++ )
        {
            final int rels = rnd.nextInt( relsPerNode );
            nextLine = node + "\t" + rels + "\t" +
                    getPropStr( node, rels, relsPerNode, propsPerNode ) + "\t" +
                    getLabel( node, rels, relsPerNode ) + "\t" + node + "\n";
            if ( strNode.length() + nextLine.length() > strNode.capacity() )
            {
                nodeFile.write( strNode.toString().getBytes() );
                strNode.setLength( 0 );
            }
            strNode.append( nextLine );
            if ( node % 1000000 == 0 )
            {
                System.out.println( node + " nodes in " + ((System.currentTimeMillis() - start) / 1000) + "secs with " +
                        "[" + longPropCount + "] long property strings" );
            }
            for ( int rel = rels; rel >= 0; rel-- )
            {
                relCount++;
                try
                {
                    type = types[rel % numTypes];
                }
                catch ( Exception e )
                {
                    type = "Two";
                }
                if ( sorted )
                {
                    final int target = node + rnd.nextInt( nodes - node );
                    final boolean outgoing = rnd.nextBoolean();

                    if ( outgoing )
                    {
                        nextLine = node + "\t" + target + "\t" + type + "\t" +
                                getPropStr( node, rels, relsPerNode, propsPerRel ) + "\t" + relCount + "\n";
                    }
                    else
                    {
                        nextLine = target + "\t" + node + "\t" + type + "\t" +
                                getPropStr( node, rels, relsPerNode, propsPerRel ) + "\t" + relCount + "\n";
                    }
                }
                else
                {
                    final int node1 = rnd.nextInt( nodes + startNodeId );
                    final int node2 = rnd.nextInt( nodes + startNodeId );
                    nextLine = node1 + "\t" + node2 + "\t" + type + "\t" +
                            getPropStr( node, rels, relsPerNode, propsPerRel ) + "\t" + relCount + "\n";
                }
                if ( strRel.length() + nextLine.length() > strRel.capacity() )
                {
                    relFile.write( strRel.toString().getBytes() );
                    strRel.setLength( 0 );
                }
                strRel.append( nextLine );
            }
        }
        nodeFile.write( strNode.toString().getBytes() );
        relFile.write( strRel.toString().getBytes() );
        return relCount;
    }

    static String getPropStr( int node, int rels, int relsPerNode, int count )
    {
        StringBuilder str = new StringBuilder();
        str.append( getProp( node, rels, relsPerNode ) );
        for ( int i = 1; i < count; i++ )
        {
            str.append( "\t" + getProp( node, rels, relsPerNode ) );
        }
        return str.toString();
    }

    static String getPropsHeader( int count, String hdr )
    {
        StringBuilder str = new StringBuilder();
        str.append( hdr );
        for ( int i = 1; i < count; i++ )
        {
            str.append( "\t" + hdr + i );
        }
        return str.toString();
    }

    static private String[] genDistinctStrings( int count, boolean limit, String[] reference )
    {
        HashSet<String> labels = new HashSet<String>();
        Random rnd = new Random();
        while ( labels.size() < count )
        {
            int tmp = 0;
            if ( reference != null || limit )
            {
                if ( reference == null )
                {
                    tmp = Math.abs( (rnd.nextInt() * rnd.nextInt() % count) );
                }
                else
                {
                    tmp = Math.abs( (rnd.nextInt()) % reference.length );
                }
            }
            else
            {
                tmp = Math.abs( (rnd.nextInt() % (10 * count)) );
            }

            String tmpStr = reference == null ? EnglishNumberToWords.convert( tmp ) : reference[tmp];
            //if (tmpStr.length() > MAX_SHORT_STRING)
            //	tmpStr = tmpStr.substring(0, MAX_SHORT_STRING-1);
            if ( !tmpStr.isEmpty() )
            {
                labels.add( tmpStr );
            }
        }
        return labels.toArray( new String[count] );
    }

    static String[] genDistinctStrings( int count )
    {
        return genDistinctStrings( count, true, null );
    }

    static String[] genDistinctStrings( int count, String[] reference )
    {
        return genDistinctStrings( count, false, reference );
    }

    static String[] genDistinctStrings( int count, boolean limit )
    {
        return genDistinctStrings( count, false, null );
    }

    static String getLabel( int node, int rels, int relsPerNode )
    {
        HashSet<String> labels = new HashSet<String>();
        String type = null;
        try
        {
            int numLabels = (node * rels) % relsPerNode;
            numLabels = numLabels == 0 ? 1 : numLabels;
            String[] lblStr = genDistinctStrings( numLabels, nodeLabels );
            type = lblStr[0];
            for ( int i = 1; i < lblStr.length; i++ )
            {
                type += "," + lblStr[i];
            }
        }
        catch ( Exception e )
        {
            type = "One";
        }
        return type;
    }

    static String getProp( int node, int rels, int relsPerNode )
    {
        HashSet<String> labels = new HashSet<String>();
        String type = null;
        try
        {
            int numLabels = (node * rels) % (relsPerNode + 2);
            numLabels = numLabels == 0 ? 1 : numLabels;
            String[] lblStr = genDistinctStrings( numLabels, nodeLabels );
            type = lblStr[0];
            for ( int i = 1; i < lblStr.length; i = i + 2 )
            {
                type += lblStr[i];
            }
            propCount++;
            if ( type.length() > MAX_SHORT_STRING )
            {
                if ( propCount % MAX_SHORT_STRING_SELECT == 0 )
                {
                    longPropCount++;
                }
                else
                {
                    type = type.substring( 0, MAX_SHORT_STRING );
                }
            }
        }
        catch ( Exception e )
        {
            type = "ONE";
        }
        return type;
    }

    public static class EnglishNumberToWords
    {
        private static final String[] tensNames =
                {"", "Ten", "Twenty", "Thirty", "Forty", "Fifty", "Sixty", "Seventy", "Eighty", "Ninety"};
        private static final String[] numNames =
                {"", "One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eight", "Nine", "Ten",
                        "Eleven", "Twelve", "Thirteen", "Fourteen", "Fifteen", "Sixteen", "Seventeen", "Eighteen",
                        "Nineteen"};

        private static String convertLessThanOneThousand( int number )
        {
            String soFar;
            if ( number % 100 < 20 )
            {
                soFar = numNames[number % 100];
                number /= 100;
            }
            else
            {
                soFar = numNames[number % 10];
                number /= 10;
                soFar = tensNames[number % 10] + soFar;
                number /= 10;
            }
            if ( number == 0 )
            {
                return soFar;
            }
            return numNames[number] + "Hundred" + soFar;
        }

        public static String convert( long number )
        {
            // 0 to 999 999 999 999
            if ( number == 0 )
            {
                return "zero";
            }
            String snumber = Long.toString( number );
            // pad with "0"
            String mask = "000000000000";
            DecimalFormat df = new DecimalFormat( mask );
            snumber = df.format( number );
            // XXXnnnnnnnnn
            int billions = Integer.parseInt( snumber.substring( 0, 3 ) );
            // nnnXXXnnnnnn
            int millions = Integer.parseInt( snumber.substring( 3, 6 ) );
            // nnnnnnXXXnnn
            int hundredThousands = Integer.parseInt( snumber.substring( 6, 9 ) );
            // nnnnnnnnnXXX
            int thousands = Integer.parseInt( snumber.substring( 9, 12 ) );
            String tradBillions;
            switch ( billions )
            {
                case 0:
                    tradBillions = "";
                    break;
                case 1:
                    tradBillions = convertLessThanOneThousand( billions )
                            + "Billion";
                    break;
                default:
                    tradBillions = convertLessThanOneThousand( billions )
                            + "Billion";
            }
            String result = tradBillions;

            String tradMillions;
            switch ( millions )
            {
                case 0:
                    tradMillions = "";
                    break;
                case 1:
                    tradMillions = convertLessThanOneThousand( millions )
                            + "Million";
                    break;
                default:
                    tradMillions = convertLessThanOneThousand( millions )
                            + "Million";
            }
            result = result + tradMillions;

            String tradHundredThousands;
            switch ( hundredThousands )
            {
                case 0:
                    tradHundredThousands = "";
                    break;
                case 1:
                    tradHundredThousands = "OneThousand";
                    break;
                default:
                    tradHundredThousands = convertLessThanOneThousand( hundredThousands )
                            + "Thousand";
            }
            result = result + tradHundredThousands;
            String tradThousand;
            tradThousand = convertLessThanOneThousand( thousands );
            result = result + tradThousand;
            // remove extra spaces!
            return result.replaceAll( "^\\s+", "" ).replaceAll( "\\b\\s{2,}\\b", " " );
        }
    }
}
