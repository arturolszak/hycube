package net.hycube.environment;

import net.hycube.utils.ObjectToStringConverter.Direction;
import net.hycube.utils.ObjectToStringConverter.MappedType;

public class NodePropertiesConversionException extends Exception {
	private static final long serialVersionUID = -7773483745565985587L;
	
	protected Direction direction;
	protected String key;
	protected String stringValue;
	protected Object object;
	protected MappedType type;
	
	public Direction getDirection() { return direction; }
	public String getKey() { return key; }
	public String getStringValue() { return stringValue; }
	public Object getObject() { return object; }
	public MappedType getType() { return type; }
	
	public NodePropertiesConversionException(Direction direction, String key, String stringValue, Object object, MappedType type) {
		super();
		this.key = key;
		this.stringValue = stringValue;
		this.object = object;
		this.direction = direction;
	}
	public NodePropertiesConversionException(Direction direction, String key, String stringValue, Object object, MappedType type, String msg, Throwable e) {
		super(msg, e);
		this.key = key;
		this.stringValue = stringValue;
		this.object = object;
		this.direction = direction;
	}
	public NodePropertiesConversionException(Direction direction, String key, String stringValue, Object object, MappedType type, String msg) {
		super(msg);
		this.key = key;
		this.stringValue = stringValue;
		this.object = object;
		this.direction = direction;
	}
	public NodePropertiesConversionException(Direction direction, String key, String stringValue, Object object, MappedType type, Throwable e) {
		super(e);
		this.key = key;
		this.stringValue = stringValue;
		this.object = object;
		this.direction = direction;
	}


}
