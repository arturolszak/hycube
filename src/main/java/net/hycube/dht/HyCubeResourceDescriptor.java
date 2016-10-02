package net.hycube.dht;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HyCubeResourceDescriptor implements Serializable {

	private static final long serialVersionUID = 4616757095926318618L;
	
	public static final String OPEN_BRACKET = "<";
	public static final String CLOSE_BRACKET = ">";
	public static final String EQUALS = "=";
	
//	protected static final String OPEN_VERBATIM = "$[";
//	protected static final String CLOSE_VERBATIM = "]";
//	protected static final String ESCAPE_VERBATIM = "$";
	
	
	public static final String KEY_RESOURCE_ID = "resourceId";
	public static final String KEY_RESOURCE_NAME = "resourceName";
	public static final String KEY_RESOURCE_TYPE = "resourceType";
	public static final String KEY_RESOURCE_URL = "resourceUrl";
	
	
	public static final String KEY_ATTRIBUTES = "attributes";
	
	public static final String KEY_ATTRIBUTE_SIZE = "size";
	
	
	protected String resourceId;
	protected String resourceName;
	protected String resourceType;
	protected String resourceUrl;
	
	protected Map<String, String> attributes;
	
	
	
	public String getResourceId() {
		return resourceId;
	}


	public void setResourceId(String resourceId) {
		this.resourceId = resourceId;
	}


	public String getResourceName() {
		return resourceName;
	}


	public void setResourceName(String resourceName) {
		this.resourceName = resourceName;
	}


	public String getResourceType() {
		return resourceType;
	}


	public void setResourceType(String resourceType) {
		this.resourceType = resourceType;
	}


	public String getResourceUrl() {
		return resourceUrl;
	}


	public void setResourceUrl(String resourceUrl) {
		this.resourceUrl = resourceUrl;
	}

	
	
	
	
	protected HyCubeResourceDescriptor() {
		this.attributes = new HashMap<String, String>();
		
	}
	
	
	public HyCubeResourceDescriptor(String resourceId, String resourceName, String resourceType, String resourceUrl) {
		
		this();
		
		this.resourceId = resourceId;
		this.resourceName = resourceName;
		this.resourceType = resourceType;
		this.resourceUrl = resourceUrl;
		
		
	}

	
	
	public HyCubeResourceDescriptor(String resourceDescriptorString) {
		
		this();
		
		parseDescriptor(resourceDescriptorString, this);
		
	}
	
	
	@Override
	public String toString() {
		return getDescriptorString();
		
		
	}
	
	
	public void setAttribute(String key, String value) {
		if (key.equals(KEY_RESOURCE_ID)) resourceId = value;
		else if (key.equals(KEY_RESOURCE_NAME)) resourceName = value;
		else if (key.equals(KEY_RESOURCE_TYPE)) resourceType = value;
		else if (key.equals(KEY_RESOURCE_URL)) resourceUrl = value;
		else {
			if (value != null) attributes.put(key, value);
			else attributes.remove(key);
		}
	}
	
	public String getAttribute(String key) {
		if (key.equals(KEY_RESOURCE_ID)) return resourceId;
		else if (key.equals(KEY_RESOURCE_NAME)) return resourceName;
		else if (key.equals(KEY_RESOURCE_TYPE)) return resourceType;
		else if (key.equals(KEY_RESOURCE_URL)) return resourceUrl;
		else {
			return attributes.get(key);
		}
	}
	
	public void removeAttribute(String key) {
		if (key.equals(KEY_RESOURCE_ID)) resourceId = null;
		else if (key.equals(KEY_RESOURCE_NAME)) resourceName = null;
		else if (key.equals(KEY_RESOURCE_TYPE)) resourceType = null;
		else if (key.equals(KEY_RESOURCE_URL)) resourceUrl = null;
		else {
			attributes.remove(key);
		}
	}
	
	
	public String getDescriptorString() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(OPEN_BRACKET).append(KEY_RESOURCE_ID).append(EQUALS).append((resourceId != null ? resourceId.trim() : "")).append(CLOSE_BRACKET);
		sb.append(OPEN_BRACKET).append(KEY_RESOURCE_NAME).append(EQUALS).append((resourceName != null ? resourceName.trim() : "")).append(CLOSE_BRACKET);
		sb.append(OPEN_BRACKET).append(KEY_RESOURCE_TYPE).append(EQUALS).append((resourceType != null ? resourceType.trim() : "")).append(CLOSE_BRACKET);
		sb.append(OPEN_BRACKET).append(KEY_RESOURCE_URL).append(EQUALS).append((resourceUrl != null ? resourceUrl.trim() : "")).append(CLOSE_BRACKET);
		
		
		//attributes
		
		for (Entry<String, String> attr : attributes.entrySet()) {
			if (! attr.getKey().isEmpty()) {
				sb.append(OPEN_BRACKET).append(attr.getKey()).append(EQUALS).append((attr.getValue() != null ? attr.getValue() : "")).append(CLOSE_BRACKET);
			}
		}
		
		
		return sb.toString();
		
	}
	
	
	public static HyCubeResourceDescriptor parseDescriptor(String descriptor) {
		return parseDescriptor(descriptor, null);
	}
	
	public static HyCubeResourceDescriptor parseDescriptor(String descriptor, HyCubeResourceDescriptor resourceDescriptor) {
	
		HyCubeResourceDescriptor rd;
		if (resourceDescriptor != null) rd = resourceDescriptor;
		else rd = new HyCubeResourceDescriptor();
		
		boolean parse = true;
		int pos = 0;
		while (parse && pos < descriptor.length()) {
			pos = parseKeyValuePair(rd, descriptor, pos);
			if (pos == -1) parse = false;
		}
		
		return rd;
		
	}

	
	protected static int parseKeyValuePair(HyCubeResourceDescriptor rd, String descriptor, int pos) {
		
		pos = descriptor.indexOf(OPEN_BRACKET, pos);
		if (pos == -1) return pos;
		pos++;
		int start = pos;
		
		pos = descriptor.indexOf(CLOSE_BRACKET, pos);
		if (pos == -1) return pos;
		int end = pos;
		pos++;
		
		Pattern pattern = Pattern.compile("\\s*(\\S+)\\s*" + EQUALS + "\\s*(.*\\S+)\\s*");
		Matcher matcher = pattern.matcher(descriptor.substring(start, end));

		if (matcher.find()) {
			String key = matcher.group(1);
			String value = matcher.group(2);
			
			rd.setAttribute(key, value);
			
		}
	
		return pos;
	}
	

	
	public boolean matches(HyCubeResourceDescriptor criteria) {
		
		boolean matches;
		
		if (  (criteria.resourceId == null || criteria.resourceId.isEmpty() || criteria.resourceId.equals(this.resourceId))
				&& (criteria.resourceName == null || criteria.resourceName.isEmpty() || criteria.resourceName.equals(this.resourceName))
				&& (criteria.resourceType == null || criteria.resourceType.isEmpty() || criteria.resourceType.equals(this.resourceType))
				&& (criteria.resourceUrl == null || criteria.resourceUrl.isEmpty() || criteria.resourceUrl.equals(this.resourceUrl))
			) {
			matches = true;
		}
		else {
			matches = false;
		}
			
		if (matches) {
			for (Entry<String, String> attribute : criteria.attributes.entrySet()) {
				if ((! attribute.getKey().isEmpty()) && (attribute.getValue() != null) && (! attribute.getValue().isEmpty()) 
						&& attributes.get(attribute.getKey()) != null && (! attributes.get(attribute.getKey()).isEmpty())
						&& (! attribute.getValue().equals(attributes.get(attribute.getKey())))) 
				{
					matches = false;
					break;
				}
			}
		}
		
		
		return matches;
		
	}
	
	
	
}
