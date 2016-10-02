package net.hycube.logging;

import net.hycube.configuration.GlobalConstants;


public final class LogHelper {

	protected static final String USER_LOG_SUFFIX = "log.user";
	protected static final String MESSAGES_LOG_SUFFIX = "log.messages";
	protected static final String DEV_LOG_SUFFIX = "log.dev";
	
	protected static org.apache.commons.logging.Log userLog = org.apache.commons.logging.LogFactory.getLog(GlobalConstants.HYCUBE_PACKAGE + "." + USER_LOG_SUFFIX);
	protected static org.apache.commons.logging.Log msgLog = org.apache.commons.logging.LogFactory.getLog(GlobalConstants.HYCUBE_PACKAGE + "." + MESSAGES_LOG_SUFFIX);
	protected static org.apache.commons.logging.Log devLog = org.apache.commons.logging.LogFactory.getLog(GlobalConstants.HYCUBE_PACKAGE + "." + DEV_LOG_SUFFIX);
	
	protected LogHelper() {}

	public static org.apache.commons.logging.Log getUserLog() {
		return userLog;
	}
	
	public static org.apache.commons.logging.Log getMessagesLog() {
		return msgLog;
	}
	
	public static org.apache.commons.logging.Log getDevLog() {
		return devLog;
	}
	
	public static org.apache.commons.logging.Log getDevLog(Class<?> clazz) {
		return org.apache.commons.logging.LogFactory.getLog(GlobalConstants.HYCUBE_PACKAGE + "." + DEV_LOG_SUFFIX + "." + clazz.getName());
	
	}
	
}
