package net.hycube.environment;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import net.hycube.configuration.GlobalConstants;
import net.hycube.environment.NodePropertiesInitializationException.InitializationError;
import net.hycube.logging.LogHelper;
import net.hycube.utils.ObjectToStringConverter;
import net.hycube.utils.ObjectToStringConverter.ConversionException;
import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * 
 * @author Artur Olszak
 *
 */
public class FileNodePropertiesReader implements NodePropertiesReader {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5892503647307738797L;
	
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(FileNodePropertiesReader.class);

	public static final boolean DEFAULT_TRIM_PROP_VALUES = true;
	
	protected boolean trimValues;
	
	protected Properties defaultProperties;
	protected Properties applicationProperties;
	

	protected FileNodePropertiesReader() {
		
	}
	
	
	public static FileNodePropertiesReader loadProperties() throws NodePropertiesInitializationException {
		return loadPropertiesFromClassPath(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, GlobalConstants.APP_PROPERTIES_FILE_NAME, DEFAULT_TRIM_PROP_VALUES);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromClassPath(String propertiesFileName) throws NodePropertiesInitializationException {
		return loadPropertiesFromClassPath(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, propertiesFileName, DEFAULT_TRIM_PROP_VALUES);
	}
	
	public static FileNodePropertiesReader loadProperties(boolean trimValues) throws NodePropertiesInitializationException {
		return loadPropertiesFromClassPath(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, GlobalConstants.APP_PROPERTIES_FILE_NAME, trimValues);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromClassPath(String propertiesFileName, boolean trimValues) throws NodePropertiesInitializationException {
		return loadPropertiesFromClassPath(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, propertiesFileName, trimValues);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromClassPath(String defaultPropertiesFileName, String propertiesFileName) throws NodePropertiesInitializationException {
		return loadPropertiesFromClassPath(defaultPropertiesFileName, propertiesFileName, DEFAULT_TRIM_PROP_VALUES);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromClassPath(String defaultPropertiesFileName, String propertiesFileName, boolean trimValues) throws NodePropertiesInitializationException {
		
		userLog.debug("Loading properties. Default properties file name: " + defaultPropertiesFileName + ". Application properties file name: " + propertiesFileName);
		devLog.debug("Loading properties. Default properties file name: " + defaultPropertiesFileName + ". Application properties file name: " + propertiesFileName);
		
		ClassLoader loader = ClassLoader.getSystemClassLoader();
		
		FileNodePropertiesReader nodeProperties = new FileNodePropertiesReader();
		
		nodeProperties.trimValues = trimValues;
		
		// create and load default properties
		nodeProperties.defaultProperties = new Properties();
		InputStream in = loader.getResourceAsStream(defaultPropertiesFileName);
		
		if (in != null) {
//			if (in == null) {
//				throw new NodePropertiesInitializationException(InitializationError.DEFAULT_PROPERTY_FILE_READ_ERROR, "Default properties file not found.");
//			}
			try {
				nodeProperties.defaultProperties.load(in);
			}
			catch (IOException e) {
				throw new NodePropertiesInitializationException(InitializationError.DEFAULT_PROPERTY_FILE_READ_ERROR, "An error occured while reading default properties.", e);
			}
			finally {
	            if (in != null) try { in.close (); } catch (Throwable ignore) {}
	        }
		}
		else {
			userLog.warn("Default properties file not found.");
			devLog.warn("Default properties file not found.");
		}

		// create application properties with default values and load application properties:
		boolean loadApplicationProperties = true;
		nodeProperties.applicationProperties = new Properties(nodeProperties.defaultProperties);
		if (propertiesFileName == null) {
//			userLog.warn("Application properties file not specified. Using default values.");
//			devLog.warn("Application properties file not specified. Using default values.");
			loadApplicationProperties = false;
		}
		else {
			in = loader.getResourceAsStream(propertiesFileName);
			if (in == null) {
				loader = Thread.currentThread().getContextClassLoader();
				in = loader.getResourceAsStream(propertiesFileName);
			}
			if (in == null) {
				userLog.warn("Application properties file not found. Using default values.");
				devLog.warn("Application properties file not found. Using default values.");
				loadApplicationProperties = false;
			}
		}
		if (loadApplicationProperties) {
			try {
				nodeProperties.applicationProperties.load(in);
			}
			catch (IOException e) {
				throw new NodePropertiesInitializationException(InitializationError.APPLICATION_PROPERTY_FILE_READ_ERROR, "An error occured while reading application properties.", e);
			}
			finally {
				if (in != null) try { in.close (); } catch (Throwable ignore) {}	
			}
		
		}
		
		
		return nodeProperties;
		
	}
	
	public static FileNodePropertiesReader loadPropertiesFromFile(String propertiesFilePath) throws NodePropertiesInitializationException {
		return loadPropertiesFromFile(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, propertiesFilePath, DEFAULT_TRIM_PROP_VALUES);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromFile(String propertiesFilePath, boolean trimValues) throws NodePropertiesInitializationException {
		return loadPropertiesFromFile(GlobalConstants.DEFAULT_PROPERTIES_FILE_NAME, propertiesFilePath, trimValues);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromFile(String defaultPropertiesFileName, String propertiesFilePath) throws NodePropertiesInitializationException {
		return loadPropertiesFromFile(defaultPropertiesFileName, propertiesFilePath, DEFAULT_TRIM_PROP_VALUES);
	}
	
	public static FileNodePropertiesReader loadPropertiesFromFile(String defaultPropertiesFileName, String propertiesFilePath, boolean trimValues) throws NodePropertiesInitializationException {
		
		userLog.debug("Loading properties. Default properties file name: " + defaultPropertiesFileName + ". Application properties file path: " + propertiesFilePath);
		devLog.debug("Loading properties. Default properties file name: " + defaultPropertiesFileName + ". Application properties file path: " + propertiesFilePath);
		
		ClassLoader loader = ClassLoader.getSystemClassLoader();
		
		FileNodePropertiesReader nodeProperties = new FileNodePropertiesReader();
		
		nodeProperties.trimValues = trimValues;
		
		// create and load default properties
		nodeProperties.defaultProperties = new Properties();
		InputStream in = loader.getResourceAsStream(defaultPropertiesFileName);
		if (in == null) {
			throw new NodePropertiesInitializationException(InitializationError.DEFAULT_PROPERTY_FILE_READ_ERROR, "Default properties file not found.");			
		}
		try {
			nodeProperties.defaultProperties.load(in);
		}
		catch (IOException e) {
			throw new NodePropertiesInitializationException(InitializationError.DEFAULT_PROPERTY_FILE_READ_ERROR, "An error occured while reading default properties.", e);
		}
		finally {
            if (in != null) try { in.close (); } catch (Throwable ignore) {}
        }
		
		// create application properties with default values and load application properties:
		boolean loadApplicationProperties = true;
		nodeProperties.applicationProperties = new Properties(nodeProperties.defaultProperties);
		try {
			in = new FileInputStream(propertiesFilePath);
		}
		catch (FileNotFoundException e) {
			userLog.warn("Application properties file not found. Using default values");
			devLog.warn("Application properties file not found. Using default values", e);
			loadApplicationProperties = false;
		}
		finally {
			if (in != null) try { in.close (); } catch (Throwable ignore) {}	
		}
		if (loadApplicationProperties) {
			try {
				nodeProperties.applicationProperties.load(in);
			}
			catch (IOException e) {
				throw new NodePropertiesInitializationException(InitializationError.APPLICATION_PROPERTY_FILE_READ_ERROR, "An error occured while reading application properties.", e);
			}
			finally {
				if (in != null) try { in.close (); } catch (Throwable ignore) {}	
			}
		}
		
		
		return nodeProperties;
		
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#saveProperties(java.lang.String)
	 */
	@Override
	public void saveProperties(String fileName) {
		FileOutputStream out = null;
		try {
			out = new FileOutputStream(fileName);
		} catch (FileNotFoundException e) {
			
		}
		try {
			applicationProperties.store(out, null);
		} catch (IOException e) {
			
		}
		try {
			out.close();
		} catch (IOException e) {
			
		}
	}
	
	
	protected String _getProperty(String key) {
		return applicationProperties.getProperty(key);
	}
	
	protected Object _setProperty(String key, String value) {
		return applicationProperties.setProperty(key, value);
	}
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#containsKey(java.lang.String)
	 */
	@Override
	public boolean containsKey(String key) {
		return (applicationProperties.getProperty(key) != null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getProperty(java.lang.String)
	 */
	@Override
	public String getProperty(String key) {
		String propValue = _getProperty(key);
		if (trimValues && propValue != null) propValue = propValue.trim();
		return propValue; 
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#propertyNames()
	 */
	@Override
	public Enumeration<?> propertyNames() {
		return applicationProperties.propertyNames();
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#size()
	 */
	@Override
	public int size() {
		return applicationProperties.size();
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public Object setProperty(String key, String value) {
		if (trimValues && value != null) value = value.trim();
		return _setProperty(key, value);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#remove(java.lang.String)
	 */
	@Override
	public Object remove (String key) {
		return applicationProperties.remove(key);
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setProperty(java.lang.String, java.lang.Object, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public Object setProperty(String key, Object object, MappedType type) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propString = null;
		try {
			propString = converter.convertObjectToString(object, type, false);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, null, object, type, "A conversion error occured while converting a property to its string representation. Key: " + key + ", object: " + object + ", type: " + type + ".", e);
		}
		if (trimValues && propString != null) propString = propString.trim();
		return _setProperty(key, propString);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setListProperty(java.lang.String, java.util.List, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public Object setListProperty(String key, List<Object> list, MappedType type) throws NodePropertiesConversionException {
		return setListProperty(key, list, type, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setListProperty(java.lang.String, java.util.List, net.hycube.utils.ObjectToStringConverter.MappedType, java.lang.String)
	 */
	@Override
	public Object setListProperty(String key, List<Object> list, MappedType type, String delimiter) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propString = null;
		try {
			propString = converter.convertObjectListToString(list, type, delimiter, trimValues);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, null, list, type, "A conversion error occured while converting a property to its string representation. Key: " + key + ", list: " + list + ", type: " + type + ".", e);
		}
		return _setProperty(key, propString);
	}

	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setProperty(java.lang.String, java.lang.Object)
	 */
	@Override
	public Object setProperty(String key, Object object) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propString = null;
		try {
			propString = converter.convertObjectToString(object);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, null, object, null, "A conversion error occured while converting a property to its string representation. Key: " + key + ", object: " + object + ".", e);
		}
		if (trimValues && propString != null) propString = propString.trim();
		return _setProperty(key, propString);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setListProperty(java.lang.String, java.util.List)
	 */
	@Override
	public Object setListProperty(String key, List<Object> list) throws NodePropertiesConversionException {
		return setListProperty(key, list, (String)null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#setListProperty(java.lang.String, java.util.List, java.lang.String)
	 */
	@Override
	public Object setListProperty(String key, List<Object> list, String delimiter) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propString = null;
		try {
			propString = converter.convertObjectListToString(list, delimiter, trimValues);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, null, list, null, "A conversion error occured while converting a property to its string representation. Key: " + key + ", list: " + list + ".", e);
		}
		return _setProperty(key, propString);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public Object getProperty(String key, MappedType type) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propertyStr = _getProperty(key);
		if (trimValues && propertyStr != null) propertyStr = propertyStr.trim();
		Object propertyObj = null;
		try {
			propertyObj = converter.parseValue(propertyStr, type);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, propertyStr, null, type, "A conversion error occured while converting a string property to its object representation. Key: " + key + ", value: " + propertyStr + ", type: " + type + ".", e);
		}
		return propertyObj;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getEnumProperty(java.lang.String, java.lang.Class)
	 */
	@Override
	public Object getEnumProperty(String key, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propertyStr = _getProperty(key);
		if (trimValues && propertyStr != null) propertyStr = propertyStr.trim();
		Object propertyObj = null;
		try {
			propertyObj = converter.parseEnumValue(propertyStr, enumClass);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, propertyStr, null, null, "A conversion error occured while converting a string property to its enum representation. Key: " + key + ", value: " + propertyStr + ", enum: " + enumClass.getName() + ".", e);
		}
		return propertyObj;
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getListProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public List<Object> getListProperty(String key, MappedType type) throws NodePropertiesConversionException {
		return getListProperty(key, type, null);
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getListProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType, java.lang.String)
	 */
	@Override
	public List<Object> getListProperty(String key, MappedType type, String delimiter) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propertyStr = _getProperty(key);
		List<Object> propertyObj = null;
		try {
			propertyObj = converter.parseListValue(propertyStr, type, delimiter, trimValues);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, propertyStr, null, type, "A conversion error occured while converting a string property to its object representation. Key: " + key + ", value: " + propertyStr + ", type: " + type + ".", e);
		}
		return propertyObj;
	}

	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getStringListProperty(java.lang.String)
	 */
	@Override
	public List<String> getStringListProperty(String key) throws NodePropertiesConversionException {
		return getStringListProperty(key, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getStringListProperty(java.lang.String)
	 */
	@Override
	public List<String> getStringListProperty(String key, String delimiter) throws NodePropertiesConversionException {
		String propertyStr = _getProperty(key);
		if (propertyStr == null) return null;
		if (propertyStr.trim().isEmpty()) return new ArrayList<String>(0);
		if (delimiter == null) delimiter = ObjectToStringConverter.DEFAULT_LIST_DELIMIER;
		String[] stringElements = propertyStr.split(delimiter);
		List<String> valueList = new ArrayList<String>();
		for (String strElem : stringElements) {
			if (trimValues && strElem != null) strElem = strElem.trim();
			valueList.add(strElem);
		}
		return valueList;
	}

	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getEnumListProperty(java.lang.String, java.lang.Class)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		return getEnumListProperty(key, enumClass, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getEnumListProperty(java.lang.String, java.lang.Class, java.lang.String)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, Class<? extends Enum<?>> enumClass, String delimiter) throws NodePropertiesConversionException {
		ObjectToStringConverter converter = ObjectToStringConverter.getInstance();
		String propertyStr = _getProperty(key);
		List<Enum<?>> propertyObj = null;
		try {
			propertyObj = converter.parseEnumListValue(propertyStr, enumClass, delimiter, trimValues);
		} catch (ConversionException e) {
			throw new NodePropertiesConversionException(e.getDirection(), key, propertyStr, null, null, "A conversion error occured while converting a string property to its enum representation. Key: " + key + ", value: " + propertyStr + ", enum: " + enumClass.getName() + ".", e);
		}
		return propertyObj;
	}

	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#toString()
	 */
	@Override
	public String toString() {
		return applicationProperties.toString();
	}
	
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getNodeProperties()
	 */
	@Override
	public NodeProperties getNodeProperties() {
		return new ReaderNodeProperties(this);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getNodeProperties(java.lang.String)
	 */
	@Override
	public NodeProperties getNodeProperties(String configurationNS) {
		return new ReaderNodeProperties(this, configurationNS);
	}
	

	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesReaderI#getProperties()
	 */
	@Override
	public Properties getProperties() {
		return applicationProperties;
	}
	
}
