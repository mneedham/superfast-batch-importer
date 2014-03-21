package org.neo4j.batchimport.newimport.structs;

public class Constants {
	
	public static int progressPollInterval = 1000;
	public static int printPollInterval = 10000;
	public static int errorThreshold = 50;

	public static int PROPERTY = 0;
	public static int NODE = 1;
	public static int RELATIONSHIP = 2;
	public static int PROPERTY_BLOCK = 3;
	public static int RECORDS_TYPE_MIN = PROPERTY;
	public static int RECORDS_TYPE_MAX = PROPERTY_BLOCK+1;
	public static int READ = 0;
	public static int WRITE = 1;
	public static String[] RECORD_TYPE_NAME = new String[]{"Property", "Node", "Relation"};
	public static int mb = 1024*1024;
	public static int DATA_INCREMENT = 1000;
	public final static int BUFFERED_READER_BUFFER = 4096*512;
	public static int BUFFER_SIZE_BYTES = 320 *1024;//314566;
	public static int BUFFER_ENTRIES = 4000;
	public static int BUFFERQ_SIZE = 32;
	public static int IMPORT_NODE_THREADS = 
			Runtime.getRuntime().availableProcessors() < 6 ? 6 : Runtime.getRuntime().availableProcessors();
	public static int BINARY_FIELD_SIZE = 8192;
	public static final int DATA_LIMIT         = BINARY_FIELD_SIZE;
	public static final int BINARY_DATA_LIMIT  = BINARY_FIELD_SIZE * 2;
	public static boolean debugData = false;
	public enum ImportStageState {Uninitialized, Initialized, NodeImport, RelationshipImport, Exited};
}
