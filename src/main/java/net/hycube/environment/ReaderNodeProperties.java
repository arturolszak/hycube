package net.hycube.environment;

import java.util.HashSet;
import java.util.List;

import net.hycube.utils.ObjectToStringConverter.MappedType;

/**
 * 
 * @author Artur Olszak
 *
 */
public class ReaderNodeProperties implements NodeProperties {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -3181329914364320434L;
	
	
	protected static final String DEFAULT_CONFIGURATION_NS = "configuration";
	protected static final char CONFIGURATION_NS_SEPARATOR = '.';

	
	protected NodePropertiesReader propertiesReader;
	protected StringBuilder currentNode;

	public ReaderNodeProperties(NodePropertiesReader propertiesReader) {
		
		this.propertiesReader = propertiesReader;
		
		String configurationNS = propertiesReader.getProperty(DEFAULT_CONFIGURATION_NS);
		currentNode = new StringBuilder(configurationNS);
		if (!configurationNS.isEmpty()) currentNode.append(CONFIGURATION_NS_SEPARATOR);
		
	}

	public ReaderNodeProperties(NodePropertiesReader propertiesReader, String configurationNS) {	
		this(propertiesReader, configurationNS, true);
		
	}

	protected ReaderNodeProperties(NodePropertiesReader propertiesReader, String nodeKey, boolean isNamespace) {
		if  (isNamespace) {
			this.propertiesReader = propertiesReader;
			this.currentNode = new StringBuilder(nodeKey);
			if (!nodeKey.isEmpty()) currentNode.append(CONFIGURATION_NS_SEPARATOR);
		}
		else {
			this.propertiesReader = propertiesReader;
			this.currentNode = new StringBuilder(nodeKey);
		}
		
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#containsKey(java.lang.String)
	 */
	@Override
	public boolean containsKey(String key) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return containsKey(key, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#containsKey(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean containsKey(String key, String elem) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.containsKey(nestedPropKey);
		
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getProperty(java.lang.String)
	 */
	@Override
	public String getProperty(String key) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getProperty(key, (String)null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public String getProperty(String key, String elem) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getProperty(nestedPropKey);
	}
	
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public Object getProperty(String key, MappedType type) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getProperty(key, (String)null, type);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getProperty(java.lang.String, java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public Object getProperty(String key, String elem, MappedType type) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getProperty(nestedPropKey, type);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumProperty(java.lang.String, java.lang.Class)
	 */
	@Override
	public Object getEnumProperty(String key, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getEnumProperty(key, (String)null, enumClass);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumProperty(java.lang.String, java.lang.String, java.lang.Class)
	 */
	@Override
	public Object getEnumProperty(String key, String elem, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getEnumProperty(nestedPropKey, enumClass);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getStringListProperty(java.lang.String)
	 */
	@Override
	public List<String> getStringListProperty(String key) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getStringListProperty(key, (String)null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getListProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public List<Object> getListProperty(String key, MappedType type) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getListProperty(key, (String)null, type);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getStringListProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> getStringListProperty(String key, String elem) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
//		return getStringListProperty(key, elem);
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getStringListProperty(nestedPropKey);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getListProperty(java.lang.String, java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType)
	 */
	@Override
	public List<Object> getListProperty(String key, String elem, MappedType type) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getListProperty(nestedPropKey, type);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getStringListPropertyDel(java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> getStringListPropertyDel(String key, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getStringListProperty(key, delimiter);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getListProperty(java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType, java.lang.String)
	 */
	@Override
	public List<Object> getListProperty(String key, MappedType type, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getListProperty(key, (String)null, type, delimiter);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getStringListProperty(java.lang.String, java.lang.String, java.lang.String)
	 */
	@Override
	public List<String> getStringListProperty(String key, String elem, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		//return getListProperty(key, elem, MappedType.STRING, delimiter);
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getStringListProperty(nestedPropKey, delimiter);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getListProperty(java.lang.String, java.lang.String, net.hycube.utils.ObjectToStringConverter.MappedType, java.lang.String)
	 */
	@Override
	public List<Object> getListProperty(String key, String elem, MappedType type, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getListProperty(nestedPropKey, type, delimiter);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumListProperty(java.lang.String, java.lang.Class)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getEnumListProperty(key, (String)null, enumClass);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumListProperty(java.lang.String, java.lang.String, java.lang.Class)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, String elem, Class<? extends Enum<?>> enumClass) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getEnumListProperty(nestedPropKey, enumClass);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumListProperty(java.lang.String, java.lang.Class, java.lang.String)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, Class<? extends Enum<?>> enumClass, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getEnumListProperty(key, (String)null, enumClass, delimiter);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getEnumListProperty(java.lang.String, java.lang.String, java.lang.Class, java.lang.String)
	 */
	@Override
	public List<Enum<?>> getEnumListProperty(String key, String elem, Class<? extends Enum<?>> enumClass, String delimiter) throws NodePropertiesConversionException {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return propertiesReader.getEnumListProperty(nestedPropKey, enumClass, delimiter);
	}

	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getNestedProperty(java.lang.String)
	 */
	@Override
	public NodeProperties getNestedProperty(String key) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		return getNestedProperty(key, (String) null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getNestedProperty(java.lang.String, java.lang.String)
	 */
	@Override
	public NodeProperties getNestedProperty(String key, String elem) {
		if (key == null || key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
		String nestedPropKey = getSubPropertyKey(key, elem).toString();
		return new ReaderNodeProperties(this.propertiesReader, nestedPropKey, false);
	
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getAbsoluteKey()
	 */
	@Override
	public String getAbsoluteKey() {
		return getAbsoluteKey(null, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getAbsoluteKey(java.lang.String)
	 */
	@Override
	public String getAbsoluteKey(String key) {
		return getAbsoluteKey(key, null);
	}
	
	/* (non-Javadoc)
	 * @see net.hycube.environment.NodePropertiesI#getAbsoluteKey(java.lang.String, java.lang.String)
	 */
	@Override
	public String getAbsoluteKey(String key, String elem) {
		if (key == null) return currentNode.toString();
		else {
			if (key.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
			StringBuilder sb = new StringBuilder(currentNode);
			if (currentNode.length() != 0 && currentNode.charAt(currentNode.length()-1) != CONFIGURATION_NS_SEPARATOR ) sb.append('.');
			sb.append(key);
			if (elem != null) {
				if (elem.trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key));
				sb.append('[').append(elem).append(']');
			}
			return sb.toString();
		}
		
	}
	
	
	//helpers:
	
	protected String getSubPropertyKey(String key) {
		return getSubPropertyKey(key, null);
	}
	
	protected String getSubPropertyKey(String key, CharSequence elem) {
		
		if (key == null || key.toString().trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty key specified is null or empty: " + getAbsoluteKey());
		if (elem != null && elem.toString().trim().isEmpty()) throw new NodePropertiesRuntimeException("The subproperty element key specified is an empty string: " + getAbsoluteKey(key.toString()));
		
		StringBuilder sb = new StringBuilder(currentNode);
		if (currentNode.length() != 0 && currentNode.charAt(currentNode.length()-1) != CONFIGURATION_NS_SEPARATOR ) sb.append('.');
		sb.append(key);
		if (elem != null) {
			sb.append('[').append(elem).append(']');
		}
		
		//if the value of the property is a pointer, return where it points:
		HashSet<String> references = new HashSet<String>();
		String subPropKey = sb.toString();
		String subPropValue = propertiesReader.getProperty(subPropKey);
		while (subPropValue != null && subPropValue.length() > 0 && subPropValue.charAt(0) == '@') {
			
			if (references.contains(subPropKey)) {
				//references loop:
				throw new NodePropertiesRuntimeException("Reference loop detected while trying to determine the property key: " + sb.toString() + ".");
			}
			references.add(subPropKey);
		
			subPropKey = subPropValue.substring(1);
			subPropValue = getProperty(subPropKey);

		}
		
		return subPropKey;
		
	}
	
	
}
