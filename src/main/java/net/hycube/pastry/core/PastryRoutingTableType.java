package net.hycube.pastry.core;

import java.util.HashMap;
import java.util.Map;

public enum PastryRoutingTableType {
	
	//Regular(1<<HyCubeRoutingTableType.BIT_REGULAR),
	RT(1<<PastryRoutingTableType.BIT_RT | 1<<PastryRoutingTableType.BIT_REGULAR),
	LS(1<<PastryRoutingTableType.BIT_LS | 1<<PastryRoutingTableType.BIT_REGULAR),
	//Secure(1<<HyCubeRoutingTableType.BIT_SECURE),
	SecureRT(1<<PastryRoutingTableType.BIT_RT | 1<<PastryRoutingTableType.BIT_SECURE);

	private static final int BIT_RT = 0;		//bit 0 set -> rt
	private static final int BIT_LS = 2;		//bit 2 set -> ls
	private static final int BIT_REGULAR = 16;	//bit 16 set -> regular
	private static final int BIT_SECURE = 17;	//bit 17 set -> secure


	
	private int code;
    
    private static Map<Integer, PastryRoutingTableType> typesByCodes = createTypesByCodes();
    
    private PastryRoutingTableType(int code) {
		this.code = code;
	}
    
    private static Map<Integer, PastryRoutingTableType> createTypesByCodes() {
    	
		Map<Integer, PastryRoutingTableType> map = new HashMap<Integer, PastryRoutingTableType>();
		for (PastryRoutingTableType type : PastryRoutingTableType.values()) {
			map.put(type.code, type);
		}
		return map;
	}
    
    
    public static PastryRoutingTableType fromCode(int code) {
    	return typesByCodes.get(code);
    }
    
    
    public int getCode() {
		return code;
	}
    
    
    
    public boolean isRegular() {
    	if ((getCode() & 1<<PastryRoutingTableType.BIT_REGULAR) != 0) return true;
    	else return false;
    }
    
    public boolean isRegularRT() {
    	if ((getCode() & 1<<PastryRoutingTableType.BIT_REGULAR) != 0 && (getCode() & 1<<PastryRoutingTableType.BIT_RT) != 0) return true;
    	else return false;
    }

    public boolean isSecure() {
    	if ((getCode() & 1<<PastryRoutingTableType.BIT_SECURE) != 0) return true;
    	else return false;
    }
    
    public boolean isSecureRT() {
    	if ((getCode() & 1<<PastryRoutingTableType.BIT_SECURE) != 0 && (getCode() & 1<<PastryRoutingTableType.BIT_RT) != 0) return true;
    	else return false;
    }
    
    public boolean isLS() {
    	if ((getCode() & 1<<PastryRoutingTableType.BIT_REGULAR) != 0 && (getCode() & 1<<PastryRoutingTableType.BIT_LS) != 0) return true;
    	else return false;
    }
    
}
