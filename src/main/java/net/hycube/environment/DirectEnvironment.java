package net.hycube.environment;

import net.hycube.core.InitializationException;
import net.hycube.logging.LogHelper;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class DirectEnvironment extends Environment {

	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(DirectEnvironment.class);
	
	
	public static final String PROP_KEY_SCHEDULER_THREAD_POOL_SIZE = "SchedulerThreadPoolSize";
	
	public static final boolean TRIM_PROP_VALUES = true;
	
	
	public static final int DEFAULT_SCHEDULER_THREAD_POOL_SIZE = 1;
	
	
	protected SystemTimeProvider timeProvider;
	protected TimeProviderEventScheduler eventScheduler;
	protected NodeProperties nodeProperties;
	
	
	private DirectEnvironment() {
		
	}
	
	
	
	public static DirectEnvironment initialize() throws InitializationException {
		return initialize((String)null, (String)null);
	}
	
	public static DirectEnvironment initialize (String propertiesFileName) throws InitializationException {
		return initialize(propertiesFileName, (String)null);
	}
	
	public static DirectEnvironment initialize(String propertiesFileName, String configurationNS) throws InitializationException {
		DirectEnvironment directEnvironment = new DirectEnvironment();

		//read properties first:
		try {
			directEnvironment.readProperties(propertiesFileName, configurationNS, TRIM_PROP_VALUES);
		} catch (NodePropertiesInitializationException e) {
			userLog.error("An error occured while trying to read the configuration files.");
			devLog.error("An error occured while trying to read the configuration files.", e);
			throw new InitializationException(InitializationException.Error.PARAMETERS_READ_ERROR, null, "An error occured while trying to read the configuration files.", e);
		}


		
		int schedulerThreadPoolSize = DEFAULT_SCHEDULER_THREAD_POOL_SIZE;
				

		//read the properties for the environment itseld:
		NodeProperties properties = directEnvironment.properties;
		
		String environmentKey = properties.getProperty(PROP_KEY_ENVIRONMENT);
		if (environmentKey == null || environmentKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_ENVIRONMENT), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_ENVIRONMENT));
		NodeProperties environmentProperties = properties.getNestedProperty(PROP_KEY_ENVIRONMENT, environmentKey);

		try {
		
			if (environmentProperties.containsKey(PROP_KEY_SCHEDULER_THREAD_POOL_SIZE)) {
				schedulerThreadPoolSize = (Integer) environmentProperties.getProperty(PROP_KEY_SCHEDULER_THREAD_POOL_SIZE, MappedType.INT);
			}
				
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize DirectEnvironment instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
	

			

		
		directEnvironment.timeProvider = new SystemTimeProvider(schedulerThreadPoolSize);
		directEnvironment.eventScheduler = new TimeProviderEventScheduler(directEnvironment.timeProvider);

		
		return directEnvironment;
	}
	
	
	@Override
	public TimeProvider getTimeProvider() {
		return timeProvider;
	}

	@Override
	public TimeProviderEventScheduler getEventScheduler() {
		return eventScheduler;
	}

	@Override
	public void discard() {
		
		this.timeProvider.discard();
		
		super.discard();
		
	}



}
