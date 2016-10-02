package net.hycube.environment;

import java.io.Serializable;
import java.util.List;

import net.hycube.utils.ObjectToStringConverter.MappedType;

public interface NodeProperties extends Serializable {

	public boolean containsKey(String key);

	public boolean containsKey(String key, String elem);

	public String getProperty(String key);

	public String getProperty(String key, String elem);

	public Object getProperty(String key, MappedType type)
			throws NodePropertiesConversionException;

	public Object getProperty(String key, String elem, MappedType type)
			throws NodePropertiesConversionException;

	public Object getEnumProperty(String key,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public Object getEnumProperty(String key, String elem,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public List<String> getStringListProperty(String key)
			throws NodePropertiesConversionException;

	public List<Object> getListProperty(String key, MappedType type)
			throws NodePropertiesConversionException;

	public List<String> getStringListProperty(String key, String elem)
			throws NodePropertiesConversionException;

	public List<Object> getListProperty(String key, String elem,
			MappedType type) throws NodePropertiesConversionException;

	public List<String> getStringListPropertyDel(String key,
			String delimiter) throws NodePropertiesConversionException;

	public List<Object> getListProperty(String key, MappedType type,
			String delimiter) throws NodePropertiesConversionException;

	public List<String> getStringListProperty(String key, String elem,
			String delimiter) throws NodePropertiesConversionException;

	public List<Object> getListProperty(String key, String elem,
			MappedType type, String delimiter)
			throws NodePropertiesConversionException;

	public List<Enum<?>> getEnumListProperty(String key,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public List<Enum<?>> getEnumListProperty(String key, String elem,
			Class<? extends Enum<?>> enumClass)
			throws NodePropertiesConversionException;

	public List<Enum<?>> getEnumListProperty(String key,
			Class<? extends Enum<?>> enumClass, String delimiter)
			throws NodePropertiesConversionException;

	public List<Enum<?>> getEnumListProperty(String key, String elem,
			Class<? extends Enum<?>> enumClass, String delimiter)
			throws NodePropertiesConversionException;

	public NodeProperties getNestedProperty(String key);

	public NodeProperties getNestedProperty(String key, String elem);

	public String getAbsoluteKey();

	public String getAbsoluteKey(String key);

	public String getAbsoluteKey(String key, String elem);

}

