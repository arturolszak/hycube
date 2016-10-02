package net.hycube.core;

import java.util.HashMap;
import java.util.Map;

public enum HyCubeRoutingTableType {
	
	//Regular(1<<HyCubeRoutingTableType.BIT_REGULAR),
	RT1(1<<HyCubeRoutingTableType.BIT_RT1 | 1<<HyCubeRoutingTableType.BIT_REGULAR),
	RT2(1<<HyCubeRoutingTableType.BIT_RT2 | 1<<HyCubeRoutingTableType.BIT_REGULAR),
	NS(1<<HyCubeRoutingTableType.BIT_NS | 1<<HyCubeRoutingTableType.BIT_REGULAR),
	//Secure(1<<HyCubeRoutingTableType.BIT_SECURE),
	SecureRT1(1<<HyCubeRoutingTableType.BIT_RT1 | 1<<HyCubeRoutingTableType.BIT_SECURE),
	SecureRT2(1<<HyCubeRoutingTableType.BIT_RT2 | 1<<HyCubeRoutingTableType.BIT_SECURE);

	private static final int BIT_RT1 = 0;		//bit 0 set -> rt1
	private static final int BIT_RT2 = 1;		//bit 1 set -> rt2
	private static final int BIT_NS = 2;		//bit 2 set -> ns
	private static final int BIT_REGULAR = 16;	//bit 16 set -> regular
	private static final int BIT_SECURE = 17;	//bit 17 set -> secure


	
	private int code;
    
    private static Map<Integer, HyCubeRoutingTableType> typesByCodes = createTypesByCodes();
    
    private HyCubeRoutingTableType(int code) {
		this.code = code;
	}
    
    private static Map<Integer, HyCubeRoutingTableType> createTypesByCodes() {
    	
		Map<Integer, HyCubeRoutingTableType> map = new HashMap<Integer, HyCubeRoutingTableType>();
		for (HyCubeRoutingTableType type : HyCubeRoutingTableType.values()) {
			map.put(type.code, type);
		}
		return map;
	}
    
    
    public static HyCubeRoutingTableType fromCode(int code) {
    	return typesByCodes.get(code);
    }
    
    
    public int getCode() {
		return code;
	}
    
    
    
    public boolean isRegular() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_REGULAR) != 0) return true;
    	else return false;
    }
    
    public boolean isRegularRT1() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_REGULAR) != 0 && (getCode() & 1<<HyCubeRoutingTableType.BIT_RT1) != 0) return true;
    	else return false;
    }

    public boolean isRegularRT2() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_REGULAR) != 0 && (getCode() & 1<<HyCubeRoutingTableType.BIT_RT2) != 0) return true;
    	else return false;
    }
    
    public boolean isSecure() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_SECURE) != 0) return true;
    	else return false;
    }
    
    public boolean isSecureRT1() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_SECURE) != 0 && (getCode() & 1<<HyCubeRoutingTableType.BIT_RT1) != 0) return true;
    	else return false;
    }
    
    public boolean isSecureRT2() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_SECURE) != 0 && (getCode() & 1<<HyCubeRoutingTableType.BIT_RT2) != 0) return true;
    	else return false;
    }
    
    public boolean isNS() {
    	if ((getCode() & 1<<HyCubeRoutingTableType.BIT_REGULAR) != 0 && (getCode() & 1<<HyCubeRoutingTableType.BIT_NS) != 0) return true;
    	else return false;
    }
    
}
