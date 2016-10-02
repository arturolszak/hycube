package net.hycube.messaging.messages;

import java.util.HashMap;
import java.util.Map;


public enum HyCubeMessageType {
	
	/*
	 * Not set
	 */
    DEFAULT(HyCubeMessage.MSG_CODE_DEFAULT, false),

    
    /*
	 * Extended type - another message field stores the application/extension specific message type
	 */
    EXT(HyCubeMessage.MSG_CODE_EXT, false),
    
    
    
    
    /*
     * Data packet
     */
    DATA(HyCubeMessage.MSG_CODE_DATA, false),
    
    /*
     * Data packet acknowledgment
     */
    DATA_ACK(HyCubeMessage.MSG_CODE_DATA_ACK, true),

    /*
     * Lookup message
     */
    LOOKUP(HyCubeMessage.MSG_CODE_LOOKUP, false),
    
    /*
     * Lookup reply message
     */
    LOOKUP_REPLY(HyCubeMessage.MSG_CODE_LOOKUP_REPLY, false),
    
    /*
     * Search message
     */
    SEARCH(HyCubeMessage.MSG_CODE_SEARCH, false),
    
    /*
     * Search reply message
     */
    SEARCH_REPLY(HyCubeMessage.MSG_CODE_SEARCH_REPLY, false),
    
    /*
     * Recovery message
     */
    RECOVERY(HyCubeMessage.MSG_CODE_RECOVERY, true),

    /*
     * Recovery reply message
     */
    RECOVERY_REPLY(HyCubeMessage.MSG_CODE_RECOVERY_REPLY, true),

    /*
     * Join message
     */
    JOIN(HyCubeMessage.MSG_CODE_JOIN, true),

    /*
     * Join reply message
     */
    JOIN_REPLY(HyCubeMessage.MSG_CODE_JOIN_REPLY, true),

    /*
     * Leave message
     */
    LEAVE(HyCubeMessage.MSG_CODE_LEAVE, true),
    
    /*
     * Notify message
     */
    NOTIFY(HyCubeMessage.MSG_CODE_NOTIFY, true),
    
    /*
     * Ping message
     */
    PING(HyCubeMessage.MSG_CODE_PING, true),
    
    /*
     * Ping reply message
     */
    PONG(HyCubeMessage.MSG_CODE_PONG, true),
    
    
    /*
     * Put message
     */
    PUT(HyCubeMessage.MSG_CODE_PUT, true),
    
    /*
     * Put reply message
     */
    PUT_REPLY(HyCubeMessage.MSG_CODE_PUT_REPLY, true),
    
    /*
     * Get message
     */
    GET(HyCubeMessage.MSG_CODE_GET, true),
    
    /*
     * Get reply message
     */
    GET_REPLY(HyCubeMessage.MSG_CODE_GET_REPLY, true),
    
    /*
     * Delete
     */
    DELETE(HyCubeMessage.MSG_CODE_DELETE, true),
    
    /*
     * Delete reply
     */
    DELETE_REPLY(HyCubeMessage.MSG_CODE_DELETE_REPLY, true),
    
    /*
     * Refresh put
     */
    REFRESH_PUT(HyCubeMessage.MSG_CODE_REFRESH_PUT, true),
    
    /*
     * Refresh put reply
     */
    REFRESH_PUT_REPLY(HyCubeMessage.MSG_CODE_REFRESH_PUT_REPLY, true),
    
    /*
     * Replicate
     */
    REPLICATE(HyCubeMessage.MSG_CODE_REPLICATE, true),
    
    ;
    


    
    private short code;
    private boolean systemMessage;
    
    private static Map<Short, HyCubeMessageType> typesByCodes = createTypesByCodes();
    
    private HyCubeMessageType(short code, boolean systemMessage) {
		this.code = code;
		this.systemMessage = true;
	}
    
    private static Map<Short, HyCubeMessageType> createTypesByCodes() {
    	
		Map<Short, HyCubeMessageType> map = new HashMap<Short, HyCubeMessageType>();
		for (HyCubeMessageType type : HyCubeMessageType.values()) {
			map.put(type.code, type);
		}
		return map;
	}
    
    
    public static HyCubeMessageType fromCode(short code) {
    	return typesByCodes.get(code);
    }
    
    
    public short getCode() {
		return code;
	}
    
    
    public boolean isSystemMessage() {
    	return systemMessage;
    }
    
    
} 	

