package net.hycube.environment;

import net.hycube.logging.LogHelper;

public abstract class Environment {
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(Environment.class);
	
	public static final boolean TRIM_PROP_VALUES = true;
	
	public static final String PROP_KEY_ENVIRONMENT = "Environment";
	
	protected NodePropertiesReader propertiesReader;
	protected NodeProperties properties;
	
	protected boolean discarded;
	
	protected boolean trimValues;

	
	
	public abstract TimeProvider getTimeProvider();
	public abstract TimeProviderEventScheduler getEventScheduler();
	
	
	
	public NodeProperties getNodeProperties() {
		return properties;
	}
	
	public NodeProperties setNodeProperties(NodeProperties properties) {
		return properties;
	}
	
	
	
	public boolean isDiscarded() {
		return discarded;
	}
	
	
	public void readProperties(String propertiesFile) throws NodePropertiesInitializationException {
		readProperties(propertiesFile, TRIM_PROP_VALUES);
	}
	
	public void readProperties(String propertiesFile, boolean trimValues) throws NodePropertiesInitializationException {
		readProperties(propertiesFile, null, trimValues);
	}
	
	public void readProperties(String propertiesFile, String configurationNS) throws NodePropertiesInitializationException {
		readProperties(propertiesFile, configurationNS, TRIM_PROP_VALUES);
	}
	
	public void readProperties(String propertiesFile, String configurationNS, boolean trimValues) throws NodePropertiesInitializationException {

		//read properties:
		if (propertiesFile == null) this.propertiesReader = FileNodePropertiesReader.loadProperties(trimValues);
		else this.propertiesReader = FileNodePropertiesReader.loadPropertiesFromClassPath(propertiesFile, trimValues);
		
		if (configurationNS == null) {
			this.properties = propertiesReader.getNodeProperties();
		}
		else {
			this.properties = propertiesReader.getNodeProperties(configurationNS);
		}
		
		userLog.info("Node properties read from the configuration files.");
		devLog.info("Node properties read from the configuration files.");
		
	}
	
	
	
	public void readPropertiesWithDefaultValues(String defaultPropertiesFile, String propertiesFile) throws NodePropertiesInitializationException {
		readPropertiesWithDefaultValues(defaultPropertiesFile, propertiesFile, TRIM_PROP_VALUES);
	}
	
	public void readPropertiesWithDefaultValues(String defaultPropertiesFile, String propertiesFile, boolean trimValues) throws NodePropertiesInitializationException {
		readPropertiesWithDefaultValues(defaultPropertiesFile, propertiesFile, null, trimValues);
	}
	
	public void readPropertiesWithDefaultValues(String defaultPropertiesFile, String propertiesFile, String configurationNS) throws NodePropertiesInitializationException {
		readPropertiesWithDefaultValues(defaultPropertiesFile, propertiesFile, configurationNS, TRIM_PROP_VALUES);
	}
	
	public void readPropertiesWithDefaultValues(String defaultPropertiesFile, String propertiesFile, String configurationNS, boolean trimValues) throws NodePropertiesInitializationException {

		//read properties:
		if (propertiesFile != null && defaultPropertiesFile == null) {
			this.propertiesReader = FileNodePropertiesReader.loadPropertiesFromClassPath(propertiesFile, null, trimValues);
		}
		else {
			this.propertiesReader = FileNodePropertiesReader.loadPropertiesFromClassPath(defaultPropertiesFile, propertiesFile, trimValues);
		}
		
		if (configurationNS == null) {
			this.properties = propertiesReader.getNodeProperties();
		}
		else {
			this.properties = propertiesReader.getNodeProperties(configurationNS);
		}
		
		userLog.info("Node properties read from the configuration files.");
		devLog.info("Node properties read from the configuration files.");

	}
	
	
	public void discard() {
		discarded = true;
	}
	
	
}
