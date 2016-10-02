package net.hycube.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @author Artur Olszak
 *
 */
public class ObjectToStringConverter {
	
	protected ObjectToStringConverter() {
	}
	
	protected static ObjectToStringConverter instance;
	
	public static final String DEFAULT_LIST_DELIMIER = ",";
	public static String DATE_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss.SSS";
	
	public static ObjectToStringConverter getInstance() {
		if (instance == null) {
			instance = new ObjectToStringConverter();
		}
		return instance;
	}
	
	
	public static enum Direction {
		STRING_TO_OBJECT,
		OBJECT_TO_STRING,
	}
	
	/**
	 * @author Artur Olszak
	 * Exception thrown when the conversion error occurs. 
	 */
	public static class ConversionException extends Exception {
		private static final long serialVersionUID = 4125590545292795727L;

		private Direction direction;
		
		public Direction getDirection() { return direction; }
		
		public ConversionException(Direction direction) {
			super();
			this.direction = direction;
		}
		public ConversionException(Direction direction, String msg, Throwable e) {
			super(msg, e);
			this.direction = direction;
		}
		public ConversionException(Direction direction, String msg) {
			super(msg);
			this.direction = direction;
		}
		public ConversionException(Direction direction, Throwable e) {
			super(e);
			this.direction = direction;
		}
		
	}
	
	
	public static enum MappedType {
		INT("int", java.lang.Integer.class),
		SHORT("short", java.lang.Short.class),
		LONG("long", java.lang.Long.class),
		BOOLEAN("boolean", java.lang.Boolean.class),
		FLOAT("float", java.lang.Float.class),
		DOUBLE("double", java.lang.Double.class),
		BIGINTEGER("big-integer", java.math.BigInteger.class),
		DECIMAL("decimal", java.math.BigDecimal.class),
		DATE("date", java.util.Date.class),
		STRING("string", java.lang.String.class),
		LIST("list", java.util.List.class);
		
		private MappedType(String code, Class<?> mappedClass) {
			this.code = code;
			this.mappedClass = mappedClass;
		}

		
		private static Map<String, MappedType> typesByCodes = createTypesByCodes();
		private static Map<Class<?>, MappedType> typesByClasses = createTypesByClasses();
		
		private static Map<String, MappedType> createTypesByCodes() {
			Map<String, MappedType> map = new HashMap<String, MappedType>();
			for (MappedType type : values()) {
				map.put(type.code, type);
			}
			return map;
		}
		
		private static Map<Class<?>, MappedType> createTypesByClasses() {
			Map<Class<?>, MappedType> map = new HashMap<Class<?>, MappedType>();
			for (MappedType type : values()) {
				if (type.mappedClass != null) map.put(type.mappedClass, type);
			}
			return map;
		}
		
		
		
		public static MappedType fromCode(String code) {
			return typesByCodes.get(code);
		}
		
		public static MappedType fromClass(Class<? extends Object> mappedClass) {
			return typesByClasses.get(mappedClass);
		}

		
		
		private String code;
		private Class<?> mappedClass;
		
		public String getCode() {
			return code;
		}
		
		public Class<?> getMappedClass() {
			return mappedClass;
		}
		
	}

	
	

	public String convertObjectToString(Object object) throws ConversionException {
		return convertObjectToString(object, false);
	}
	
	public String convertObjectToString(Object object, boolean trimValue) throws ConversionException {
		String res = _convertObjectToString(object);
		if (trimValue && res != null) res = res.trim();
		return res;
	}

	public String convertEnumObjectToString(Enum<?> object) {
		return convertEnumObjectToString(object, false);
	}
	
	public String convertEnumObjectToString(Enum<?> object, boolean trimValue) {
		String res = _convertEnumObjectToString(object);
		if (trimValue && res != null) res = res.trim();
		return res;
	}
	
	
	public String convertObjectToString(Object object, MappedType type) throws ConversionException {
		return convertObjectToString(object, type, false);
	}
	
	public String convertObjectToString(Object object, MappedType type, boolean trimValue) throws ConversionException {
		String res = _convertObjectToString(object, type);
		if (trimValue && res != null) res = res.trim();
		return res;
	}
	
	

	
	public String convertObjectListToString(List<?> list, MappedType type) throws ConversionException {
		return convertObjectListToString(list, type, false);
	}
	
	public String convertObjectListToString(List<?> list, MappedType type, boolean trimValues) throws ConversionException {
		return convertObjectListToString(list, type, DEFAULT_LIST_DELIMIER, trimValues);
	}
	
	
	public String convertObjectListToString(List<?> list, MappedType type, String delimiter) throws ConversionException {
		return convertObjectListToString(list, type, delimiter, false);
	}
	
	public String convertObjectListToString(List<?> list, MappedType type, String delimiter, boolean trimValues) throws ConversionException {
		if (delimiter == null) delimiter = DEFAULT_LIST_DELIMIER;
		List<String> stringList = new ArrayList<String>();
		for (Object elemObject : list) {
			String elemString = _convertObjectToString(elemObject, type);
			if (trimValues && elemString != null) elemString = elemString.trim();
			stringList.add(elemString);
		}
		return StringUtils.join(stringList, delimiter, true);
	}

	public String convertObjectListToString(List<?> list) throws ConversionException {
		return convertObjectListToString(list, DEFAULT_LIST_DELIMIER, false);
	}
	
	public String convertObjectListToString(List<?> list, boolean trimValues) throws ConversionException {
		return convertObjectListToString(list, DEFAULT_LIST_DELIMIER, trimValues);
	}
	
	public String convertObjectListToString(List<?> list, String delimiter) throws ConversionException {
		return convertObjectListToString(list, delimiter, false);
	}
	
	public String convertObjectListToString(List<?> list, String delimiter, boolean trimValues) throws ConversionException {
		if (delimiter == null) delimiter = DEFAULT_LIST_DELIMIER;
		List<String> stringList = new ArrayList<String>();
		for (Object elemObject : list) {
			String elemString = _convertObjectToString(elemObject);
			if (trimValues && elemString != null) elemString = elemString.trim();
			stringList.add(elemString);
		}
		return StringUtils.join(stringList, delimiter, true);
	}

	
	
	
	public Object parseValue(String str, MappedType type) throws ConversionException {
		return parseValue(str, type, false);
	}
	
	public Object parseValue(String str, MappedType type, boolean trimInput) throws ConversionException {
		if (trimInput && str != null) str = str.trim();
		return _parseValue(str, type);
	}
	
	
	@SuppressWarnings({ "rawtypes" })
	public Enum<?> parseEnumValue(String str, Class<? extends Enum> enumClass) throws ConversionException {
		return parseEnumValue(str, enumClass, false);
	}
	
	@SuppressWarnings({ "rawtypes" })
	public Enum<?> parseEnumValue(String str, Class<? extends Enum> enumClass, boolean trimInput) throws ConversionException {
		if (trimInput && str != null) str = str.trim();
		return _parseEnumValue(str, enumClass);
	}
	

	public List<Object> parseListValue(String str, MappedType type) throws ConversionException {
		return parseListValue(str, type, false);
	}
	
	public List<Object> parseListValue(String str, MappedType type, boolean trimInput) throws ConversionException {
		return parseListValue(str, type, DEFAULT_LIST_DELIMIER, false);
	}
	
	public List<Object> parseListValue(String str, MappedType type, String delimiter) throws ConversionException {
		return parseListValue(str, type, delimiter, false);
	}
	
	public List<Object> parseListValue(String str, MappedType type, String delimiter, boolean trimInput) throws ConversionException {
		if (str == null) return null;
		if (str.trim().isEmpty()) return new ArrayList<Object>(0);
		if (delimiter == null) delimiter = DEFAULT_LIST_DELIMIER;
		String[] stringElements = str.split(delimiter);
		List<Object> valueList = new ArrayList<Object>();
		for (String strElem : stringElements) {
			if (trimInput && strElem != null) strElem = strElem.trim();
			Object value = _parseValue(strElem, type);
			valueList.add(value);
		}
		return valueList;
	}
	
	@SuppressWarnings("rawtypes")
	public List<Enum<?>> parseEnumListValue(String str, Class<? extends Enum> enumClass) throws ConversionException {
		return parseEnumListValue(str, enumClass, false);
	}
	
	@SuppressWarnings("rawtypes")
	public List<Enum<?>> parseEnumListValue(String str, Class<? extends Enum> enumClass, boolean trimInput) throws ConversionException {
		return parseEnumListValue(str, enumClass, DEFAULT_LIST_DELIMIER, trimInput);
	}
	
	@SuppressWarnings("rawtypes")
	public List<Enum<?>> parseEnumListValue(String str, Class<? extends Enum> enumClass, String delimiter) throws ConversionException {
		return parseEnumListValue(str, enumClass, delimiter, false);
	}
	
	@SuppressWarnings("rawtypes")
	public List<Enum<?>> parseEnumListValue(String str, Class<? extends Enum> enumClass, String delimiter, boolean trimInput) throws ConversionException {
		if (str == null) return null;
		if (str.trim().isEmpty()) return new ArrayList<Enum<?>>(0);
		if (delimiter == null) delimiter = DEFAULT_LIST_DELIMIER;
		String[] stringElements = str.split(delimiter);
		List<Enum<?>> valueList = new ArrayList<Enum<?>>();
		for (String strElem : stringElements) {
			if (trimInput && strElem != null) strElem = strElem.trim();
			Enum<?> enumValue = _parseEnumValue(strElem, enumClass);
			valueList.add(enumValue);
		}
		return valueList;
	}
	

	
	
	
	
	//protected methods (they don't trim the input/output strings):
	
	
	protected String _convertEnumObjectToString(Enum<?> object) {
		return object.toString();
	}
	
	protected String _convertObjectToString(Object object) throws ConversionException {
		if (object.getClass().isEnum()) {
			return _convertEnumObjectToString((Enum<?>) object);
		}
		MappedType type = MappedType.fromClass(object.getClass());
		if (type != null) {
			String res = _convertObjectToString(object, type); 
			return res; 
		}
		else {
			throw new ConversionException(Direction.OBJECT_TO_STRING, "An error occured while converting an object to its string representation. Object class not supported. Object: " + object.toString() + ", type: " + object.getClass().getName());
		}
		
	}
	
	protected String _convertObjectToString(Object object, MappedType type) throws ConversionException {
		
		String formattedValue = null;
		
		Class<?> inputClass = type.getMappedClass();
		
		if (inputClass == null) {
			return "";
		}
		
		try {
			if (inputClass.equals(String.class)) {
				formattedValue = (String) object;
			}
			else if (inputClass.equals(Integer.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printInt((Integer) object);
				formattedValue = ((Integer)object).toString();
			}
			else if (inputClass.equals(Short.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printShort((Short) object);
				formattedValue = ((Short)object).toString();
			}
			else if (inputClass.equals(Long.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printLong((Long) object);
				formattedValue = ((Long)object).toString();
			}
			else if (inputClass.equals(Boolean.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printBoolean((Boolean) object);
				formattedValue = ((Boolean)object).toString();
			}
			else if (inputClass.equals(Float.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printFloat((Float) object);
				formattedValue = ((Float)object).toString();
			}
			else if (inputClass.equals(Double.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printDouble((Double) object);
				formattedValue = ((Double)object).toString();
			}
			else if (inputClass.equals(BigInteger.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printInteger((BigInteger) object);
				formattedValue = ((BigInteger) object).toString();
			}
			else if (inputClass.equals(BigDecimal.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printDecimal((BigDecimal) object);
				formattedValue = ((BigDecimal) object).toString();
			}
			else if (inputClass.equals(Date.class)) {
				Calendar cal = Calendar.getInstance();
				cal.setTime((Date)object);
				//formattedValue = javax.xml.bind.DatatypeConverter.printDateTime(cal);
				SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT);  
				formattedValue = formatter.format(cal);  
			}
			else if (inputClass.equals(Calendar.class)) {
				//formattedValue = javax.xml.bind.DatatypeConverter.printDateTime((Calendar) object);
				SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT);  
				formattedValue = formatter.format((Calendar)object);
			}
			//else should not be needed, as enum is passed to the method
			//PrintWriter a; 
		}
		catch (Exception e) {
			throw new ConversionException(Direction.OBJECT_TO_STRING, "An error occured while converting an object to its string representation. Object: " + object.toString() + ", type: " + type.getCode(), e);
		}
		
		return formattedValue;
	}

	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected Enum<?> _parseEnumValue(String str, Class<? extends Enum> enumClass) throws ConversionException {
		if (str == null) return null;
		Enum<?> enumValue;
		try {
			enumValue = Enum.valueOf(enumClass, str);
		}
		catch (Exception e) {
			throw new ConversionException(Direction.STRING_TO_OBJECT, "An error occured while converting a string object representation to an enumeration. Input string: " + str + ", enum: " + enumClass.getName(), e);
		}
		return enumValue;
	}
	
	
	protected Object _parseValue(String str, MappedType type) throws ConversionException {
		
		if (type == null) return null;
		
		//str = str.trim();
				
		Class<?> resultClass = type.getMappedClass();
		Object result = null;
		
		if (resultClass == null) {
			return null;
		}
		
		try {
			if (resultClass.equals(String.class)) {
				result = str;
			}
			else {
				if (str.isEmpty()) return null;
				
				if (resultClass.equals(Integer.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseInt(str);
					result = Integer.valueOf(str);
				}
				else if (resultClass.equals(Short.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseShort(str);
					result = Short.valueOf(str);
				}
				else if (resultClass.equals(Long.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseLong(str);
					result = Long.valueOf(str); 
				}
				else if (resultClass.equals(Boolean.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseBoolean(str);
					result = Boolean.valueOf(str);
				}
				else if (resultClass.equals(Float.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseFloat(str);
					result = Float.valueOf(str);
				}
				else if (resultClass.equals(Double.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseDouble(str);
					result = Double.valueOf(str);
				}
				else if (resultClass.equals(BigInteger.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseInteger(str);
					result = new BigInteger(str);
				}
				else if (resultClass.equals(BigDecimal.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseDecimal(str);
					result = new BigDecimal(str);
				}
				else if (resultClass.equals(Date.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseDateTime(str).getTime();
					SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT);
					result = formatter.parse(str);
				}
				else if (resultClass.equals(Calendar.class)) {
					//result = javax.xml.bind.DatatypeConverter.parseDateTime(str);
					SimpleDateFormat formatter = new SimpleDateFormat(DATE_TIME_FORMAT);
					Date date = formatter.parse(str);
					Calendar cal = Calendar.getInstance();
					cal.setTime(date);
					result = cal;
				}
				//else should not be needed, as enum is passed to the method
			}
		}
		catch (Exception e) {
			throw new ConversionException(Direction.STRING_TO_OBJECT, "An error occured while converting a string object representation to an object. Input string: " + str + ", type: " + type.getCode(), e);
		}
		
		return result;
	}

	
	

}
