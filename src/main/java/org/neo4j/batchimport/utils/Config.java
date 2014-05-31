package org.neo4j.batchimport.utils;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Stack;

import org.neo4j.batchimport.Importer;
import org.neo4j.batchimport.IndexInfo;
import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.MapUtil;

import static java.lang.String.format;

public class Config
{
    public static final String BATCH_IMPORT_RELS_FILES = "rels";
    public static final String BATCH_IMPORT_GRAPH_DB = "db-directory";
    public static final String BATCH_IMPORT_KEEP_DB = "keep-db";
    public static final String BATCH_IMPORT_CONFIG_FILE = "config";
    public static final String CONFIG_FILE_NAME = "batch.properties";
    public static final String BATCH_IMPORT_NODES_FILES = "nodes";
    public static final String BATCH_IMPORT_MAPDB_CACHE_DISABLE = "batch_import.mapdb_cache.disable";
    public static final String BATCH_IMPORT_CSV_QUOTES = "batch_import.csv.quotes";
    public static final String BATCH_IMPORT_CSV_DELIM = "batch_import.csv.delim";
    public static final String ARRAY_SEPARATOR_CONFIG = "batch_array_separator";
    public static final String BATCH_IMPORT_DEBUG_MODE = "debug";
    public static final String BATCH_IMPORT_MAX_CPU = "max-cpu";
    public static final String CHECK_CONSISTENCY = "check";
    public static String ARRAYS_SEPARATOR = ",";

    private final Map<String, String> configData;

    public Config( Map<String, String> configData )
    {
        this.configData = configData;
        if ( this.configData.containsKey( ARRAY_SEPARATOR_CONFIG ) )
        {
            Config.ARRAYS_SEPARATOR = configData.get( ARRAY_SEPARATOR_CONFIG );
        }
    }

    public static Config convertArgumentsToConfig( String[] args )
    {
        Args arguments = new Args( args );

        String configFileName = arguments.get( BATCH_IMPORT_CONFIG_FILE, CONFIG_FILE_NAME );
        Map<String, String> config = config( configFileName );

        addConfigFromArgument( arguments, BATCH_IMPORT_GRAPH_DB, "graph.db", config );
        addConfigFromArgument( arguments, BATCH_IMPORT_NODES_FILES, null, config );
        addConfigFromArgument( arguments, BATCH_IMPORT_RELS_FILES, null, config );
        addConfigFromArgument( arguments, BATCH_IMPORT_DEBUG_MODE, null, config );
        addConfigFromArgument( arguments, CHECK_CONSISTENCY, null, config );
        addConfigFromArgument( arguments, BATCH_IMPORT_MAX_CPU, "-1", config );

        // TODO legacy indexes

        validateConfig( config );
        return new Config( config );
    }

    private static void addConfigFromArgument( Args arguments, String key, String defaultValue, Map<String,String> targetConfig )
    {
        String value = arguments.get( key, defaultValue );
        if ( value != null )
        {
            targetConfig.put( key, value );
        }
    }

    private static Stack<String> toStack( String[] args )
    {
        final Stack<String> argumentList = new Stack<String>();
        for ( int i = args.length - 1; i >= 0; i-- )
        {
            argumentList.push( args[i] );
        }
        return argumentList;
    }

    // todo more checks ?
    private static void validateConfig( Map<String, String> config )
    {
        if ( !config.containsKey( BATCH_IMPORT_GRAPH_DB ) )
        {
            throw new IllegalArgumentException( "Missing parameter for graphdb directory" );
        }
        if (!config.containsKey( BATCH_IMPORT_NODES_FILES ))
        {
            throw new IllegalArgumentException( "Missing parameter for nodes file" );
        }
        if (!config.containsKey( BATCH_IMPORT_RELS_FILES ))
        {
            throw new IllegalArgumentException( "Missing parameter for relationship file" );
        }
    }

//    private static Collection<IndexInfo> createIndexInfos( Stack<String> args )
//    {
//        Collection<IndexInfo> indexes = new ArrayList<IndexInfo>();
//        while ( !args.isEmpty() )
//        {
//            indexes.add( new IndexInfo( popOrNull( args ), popOrNull( args ), popOrNull( args ), popOrNull( args ) ) );
//        }
//        return indexes;
//    }

    public static Map<String, String> config( String fileName )
    {
        Map<String, String> config = new HashMap<String, String>();
        try
        {
            if ( new File( fileName ).exists() )
            {
                System.out.println( "Using Existing Configuration File" );
            }
            else
            {
                System.out.println( "Writing Configuration File to batch.properties" );
                FileWriter fw = new FileWriter( fileName );
                fw.append( "use_memory_mapped_buffers=true\n"
                        + "neostore.nodestore.db.mapped_memory=100M\n"
                        + "neostore.relationshipstore.db.mapped_memory=500M\n"
                        + "neostore.propertystore.db.mapped_memory=1G\n"
                        + "neostore.propertystore.db.strings.mapped_memory=200M\n"
                        + "neostore.propertystore.db.arrays.mapped_memory=0M\n"
                        + "neostore.propertystore.db.index.keys.mapped_memory=15M\n"
                        + "neostore.propertystore.db.index.mapped_memory=15M" );
                fw.close();
            }

            config = MapUtil.load( new File( fileName ) );

        }
        catch ( Exception e )
        {
            System.out.println( e.getMessage() );
        }
        return config;
    }

    public static Collection<IndexInfo> extractIndexInfos( Map<String, String> config )
    {
        Collection<IndexInfo> result = new ArrayList<IndexInfo>();
        for ( Map.Entry<String, String> entry : config.entrySet() )
        {
            final IndexInfo info = IndexInfo.fromConfigEntry( entry );
            if ( info != null )
            {
                result.add( info );
            }
        }
        return result;
    }

    public static boolean configOptionEnabled( Config config, String option )
    {
        return "true".equalsIgnoreCase( config.get( option ) );
    }

    public static boolean configOptionDisabled( Config config, String option )
    {
        return "false".equalsIgnoreCase( config.get( option ) );
    }

    public static Collection<File> toFiles( String commaSeparatedFileList )
    {
        Collection<File> files = new ArrayList<File>();
        if ( commaSeparatedFileList == null || commaSeparatedFileList.isEmpty() )
        {
            return files;
        }
        for ( String part : commaSeparatedFileList.split( "," ) )
        {
            final File file = new File( part );
            if ( file.exists() && file.canRead() && file.isFile() )
            {
                files.add( file );
            }
            else
            {
                System.err.println( "File " + file + " does not exist, can not be read or is not a file." );
            }
        }
        return files;
    }

    public static String NODE_INDEX( String indexName )
    {
        return "batch_import.node_index." + indexName;
    }

    public static String RELATIONSHIP_INDEX( String indexName )
    {
        return "batch_import.relationship_index." + indexName;
    }

    public boolean isCachedIndexDisabled()
    {
        return configOptionEnabled( this, BATCH_IMPORT_MAPDB_CACHE_DISABLE );
    }

    public Collection<IndexInfo> getIndexInfos()
    {
        return extractIndexInfos( configData );
    }

    public Collection<File> getRelsFiles()
    {
        return toFiles( get( BATCH_IMPORT_RELS_FILES ) );
    }

    public Collection<File> getNodesFiles()
    {
        return toFiles( get( BATCH_IMPORT_NODES_FILES ) );
    }

    public char getDelimChar()
    {
        final String delim = get( BATCH_IMPORT_CSV_DELIM );
        if ( delim == null || delim.isEmpty() )
        {
            return '\t';
        }
        return delim.trim().charAt( 0 );
    }

    public boolean quotesEnabled()
    {
        return !configOptionDisabled( this, BATCH_IMPORT_CSV_QUOTES );
    }

    public String getGraphDbDirectory()
    {
        return get( BATCH_IMPORT_GRAPH_DB );
    }

    public String get( String option )
    {
        return configData.get( option );
    }

    public boolean keepDatabase()
    {
        return configOptionEnabled( this, BATCH_IMPORT_KEEP_DB );
    }

    public Map<String, String> getConfigData()
    {
        return configData;
    }

    public static String usage()
    {
        StringBuilder usage = new StringBuilder();
        usage.append( Importer.class.getSimpleName() );
        usage.append( format( " -%s <%s>", BATCH_IMPORT_GRAPH_DB, "graph.db" ) );
        usage.append( format( " -%s <%s>", BATCH_IMPORT_NODES_FILES, "nodes.csv" ) );
        usage.append( format( " -%s <%s>", BATCH_IMPORT_RELS_FILES, "rels.csv" ) );
        usage.append( format( " -%s <%s>", BATCH_IMPORT_DEBUG_MODE, "debug config" ) );
        // TODO legacy index
        return usage.toString();
    }
}
