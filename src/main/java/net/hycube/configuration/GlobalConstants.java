package net.hycube.configuration;



public class GlobalConstants {

	//HyCube package
	public static final String HYCUBE_PACKAGE = "net.hycube";

	//Properties file names
	public static final String DEFAULT_PROPERTIES_FILE_NAME = "conf/hycube_default.cfg";
	public static final String APP_PROPERTIES_FILE_NAME = "hycube.cfg";


	
	//Protocol version - this will be inserted to message headers
	public static final short PROTOCOL_VERSION = 1;


	
	//maximum ack timeout (ms):
	public static final int MAX_ACK_TIMEOUT = 60000;
	
	//maximum process ack interval (ms):
	public static final int MAX_PROCESS_ACK_INTERVAL = 600000;
	
	//maximum send retries:
	public static final int MAX_SEND_RETRIES = 10;
	
	


	//default hash map load factor:
	public static final float DEFAULT_HASH_MAP_LOAD_FACTOR = 0.75f;
	
	
	
	
	//initial collections sizes:
	public static final int DEFAULT_INITIAL_COLLECTION_SIZE = 8;
	
	public static final int INITIAL_RTE_DATA_MAP_SIZE = 1;

	//public static final int INITIAL_EVENT_QUEUE_SIZE = 10;
	
	public static final int INITIAL_LOOKUPS_DATA_COLLECTION_SIZE = DEFAULT_INITIAL_COLLECTION_SIZE;
	
	public static final int INITIAL_SEARCHES_DATA_COLLECTION_SIZE = DEFAULT_INITIAL_COLLECTION_SIZE;
	
	public static final int INITIAL_JOINS_DATA_COLLECTION_SIZE = 1;
	
	public static final int INITIAL_DHT_REQUESTS_DATA_COLLECTION_SIZE = 1;
	
	public static final int INITIAL_FOUND_NEXT_HOPS_COLLECTION_SIZE = 16;
	
	public static final int INITIAL_REGISTERED_ROUTES_COLLECTION_SIZE = 32;
	
		
	
	
	
	//compile time parameters:
	
	//wait on messages sent in background (whether messages are sent immediately or are queued)
	public static final boolean WAIT_ON_BKG_MSG_SEND = false;
	
	//default value for "wait on messages sent in foreground" (whether messages are sent immediately or are queued)
	public static final boolean WAIT_ON_MSG_SEND_DEFAULT = false;
	
	
	
	
	//global property names:
	public static final String PROP_KEY_CLASS = "Class";
	
	
	
}
