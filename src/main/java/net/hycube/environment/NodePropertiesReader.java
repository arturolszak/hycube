package net.hycube.environment;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

import net.hycube.utils.ObjectToStringConverter.MappedType;

public interface NodePropertiesReader extends Serializable {

	public abstract void saveProperties(String fileName);

	public abstract boolean containsKey(String key);

	public abstract String getProperty(String key);

	public abstract Enumeration<?> propertyNames();

	public abstract int size();

	public abstract Object setProperty(String key, String value);

	public abstract Object remove(String key);

	public abstract Object setProperty(String key, Object object,
			MappedType type) throws NodePropertiesConversionException;

	public abstract Object setListProperty(String key, List<Object> list,
			MappedType type) throws NodePropertiesConversionException;

	public abstract Object setListProperty(String key, List<Object> list,
			MappedType type, String delimiter)
			throws NodePropertiesConversionException;

	public abstract Object setProperty(String key, Object object)
			throws NodePropertiesConversionException;

	public abstract Object setListProperty(String key, List<Object> list)
			throws NodePropertiesConversionException;

	public abstract Object setListProperty(String key, List<Object> list,
			String delimiter) throws NodePropertiesConversionException;

	public abstract Object getProperty(String key, MappedType type)
			throws NodePropertiesConversionException;

	public abstract Object getEnumProperty(String key,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public abstract List<Object> getListProperty(String key, MappedType type)
			throws NodePropertiesConversionException;

	public abstract List<Object> getListProperty(String key, MappedType type,
			String delimiter) throws NodePropertiesConversionException;

	public abstract List<String> getStringListProperty(String key)
			throws NodePropertiesConversionException;
	
	public abstract List<String> getStringListProperty(String key,
			String delimiter) throws NodePropertiesConversionException;
	
	public abstract List<Enum<?>> getEnumListProperty(String key,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public abstract List<Enum<?>> getEnumListProperty(String key,
			Class<? extends Enum<?>> enumClass, String delimiter)
			throws NodePropertiesConversionException;

	public abstract String toString();

	public abstract NodeProperties getNodeProperties();

	public abstract NodeProperties getNodeProperties(String configurationNS);

	public abstract Properties getProperties();

}