package net.hycube.maintenance;

import java.util.Arrays;

import net.hycube.backgroundprocessing.AbstractBackgroundProcess;
import net.hycube.backgroundprocessing.BackgroundProcessEntryPoint;
import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.environment.NodePropertiesConversionException;
import net.hycube.logging.LogHelper;

public class HyCubeRecoveryBackgroundProcess extends AbstractBackgroundProcess {

	public static enum EntryPointType {
		SET_RECOVERY_PLAN_AND_INTERVAL,
		SET_RECOVERY_PLAN,
		SET_RECOVERY_SCHEDULE_INTERVAL,
		
	}
	
	public class HyCubeRecoveryBackgroundProcessEntryPointImpl implements HyCubeRecoveryBackgroundProcessEntryPoint {
		
		@Override
		public Object call() {
			throw new UnsupportedOperationException("The entry point method with 0 arguments is not implemented.");
		}
		
		
		@Override
		public Object call(Object arg) {
			return call(new Object[] {arg});
		}


		@Override
		public Object call(Object[] args) {
			if (args.length < 1) throw new IllegalArgumentException("Invalid arguments specified.");
			return call(args[0], Arrays.copyOfRange(args, 1, args.length));		
		}


		@Override
		public Object call(Object entryPoint, Object[] args) {
			if ( ! (entryPoint instanceof EntryPointType)) throw new IllegalArgumentException("Invalid arguments specified.");
			
			EntryPointType ept = (EntryPointType) entryPoint;
			
			switch (ept) {
				case SET_RECOVERY_PLAN_AND_INTERVAL:
					if (!(args[0] instanceof HyCubeRecoveryType[])) throw new IllegalArgumentException("Invalid argument [0].");
					if (!(args[1] instanceof Integer)) throw new IllegalArgumentException("Invalid argument [1].");
					setRecoveryPlan((HyCubeRecoveryType[])args[0], (Integer)args[1]);
					return null;
				case SET_RECOVERY_PLAN:
					if (!(args[0] instanceof HyCubeRecoveryType[])) throw new IllegalArgumentException("Invalid argument [0].");
					setRecoveryPlan((HyCubeRecoveryType[])args[0]);
					break;
				case SET_RECOVERY_SCHEDULE_INTERVAL:
					if (!(args[0] instanceof Integer)) throw new IllegalArgumentException("Invalid argument [0].");
					setRecoveryScheduleInterval((Integer) args[0]);
					break;
				default:
					break;
			}
			
			return null;
		}

		
		
		@Override
		public void setRecoveryPlan(HyCubeRecoveryType[] recoveryPlan, int scheduleInterval) {
			synchronized(this) {
				HyCubeRecoveryBackgroundProcess.this.recoveryPlan = Arrays.copyOf(recoveryPlan, recoveryPlan.length);
				HyCubeRecoveryBackgroundProcess.this.currRecoveryPlanIndex = 0;
				HyCubeRecoveryBackgroundProcess.this.scheduleInterval = scheduleInterval;
				
			}
		}
		
		@Override
		public void setRecoveryPlan(HyCubeRecoveryType[] recoveryPlan) {
			setRecoveryPlan(recoveryPlan, HyCubeRecoveryBackgroundProcess.this.scheduleInterval);
		}
		
		@Override
		public void setRecoveryScheduleInterval(int scheduleInterval) {
			synchronized(this) {
				HyCubeRecoveryBackgroundProcess.this.scheduleInterval = scheduleInterval;
			}
		}
		
		
		@Override
		public boolean isRunning() {
			return HyCubeRecoveryBackgroundProcess.this.isRunning();
		}
		
		@Override
		public void start() {
			HyCubeRecoveryBackgroundProcess.this.start();
		}
		
		@Override
		public void stop() {
			HyCubeRecoveryBackgroundProcess.this.stop();
		}
		
		@Override
		public void processOnce() {
			HyCubeRecoveryBackgroundProcess.this.doProcess();
		}
		
		

	}
	
	
	
	
	private static org.apache.commons.logging.Log userLog = LogHelper.getUserLog();
	private static org.apache.commons.logging.Log devLog = LogHelper.getDevLog(HyCubeRecoveryBackgroundProcess.class); 
	
	
	protected static final String PROP_KEY_RECOVERY_EXTENSION_KEY = "RecoveryExtensionKey";
	protected static final String PROP_KEY_SCHEDULE_PLAN = "SchedulePlan";
	
	protected String recoveryExtensionKey;
	
	
	protected HyCubeRecoveryType[] recoveryPlan;
	protected int currRecoveryPlanIndex;
	
		
	protected HyCubeRecoveryManager recoveryManager;
	
	protected BackgroundProcessEntryPoint entryPoint;
	
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		if (userLog.isDebugEnabled()) {
			userLog.debug("Initializing recovery backround process...");
		}
		if (devLog.isDebugEnabled()) {
			devLog.debug("Initializing recovery backround process...");
		}
		

		super.initialize(nodeAccessor, properties);
		
		
		String recoveryExtensionKey = properties.getProperty(PROP_KEY_RECOVERY_EXTENSION_KEY);
		if (recoveryExtensionKey == null || recoveryExtensionKey.isEmpty()) {
			throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY), "Invalid property value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_EXTENSION_KEY));
		}
		
		try {
			this.recoveryManager = ((HyCubeRecoveryExtension) (nodeAccessor.getExtension(recoveryExtensionKey))).getRecoveryManager();
			if (this.recoveryManager == null) throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, null, "The Recovery Manager is not set.");
		} catch (ClassCastException e) {
			throw new InitializationException(InitializationException.Error.MISSING_EXTENSION_ERROR, null, "The Recovery Manager is expected to be an instance of: " + HyCubeRecoveryExtension.class.getName());
		}
		
		
		try {
			
			this.recoveryPlan = (HyCubeRecoveryType[]) properties.getEnumListProperty(PROP_KEY_SCHEDULE_PLAN, HyCubeRecoveryType.class).toArray(new HyCubeRecoveryType[0]);
			
			
		} catch (NodePropertiesConversionException e) {
			throw new InitializationException(InitializationException.Error.NODE_INITIALIZATION_ERROR, null, "Unable to initialize a background process instance. Invalid parameter value: " + e.getKey() + ".", e);
		}
		
				
		currRecoveryPlanIndex = 0;
		
		
		this.entryPoint = new HyCubeRecoveryBackgroundProcessEntryPointImpl();
		
		
	}

	
	@Override
	public void doProcess() {
		
		processRecovery();
		
	}
	
	
	public void processRecovery() {
		
		if (devLog.isDebugEnabled()) {
			devLog.debug("processDHT called");
		}
		
		
		HyCubeRecoveryType rt;
		synchronized (this) {
			rt = recoveryPlan[currRecoveryPlanIndex];
			currRecoveryPlanIndex = (currRecoveryPlanIndex + 1) % recoveryPlan.length;
		}
		
		switch (rt) {
			case FULL_RECOVERY:
				recoveryManager.recover();
				break;
			case RECOVERY_NS:
				recoveryManager.recoverNS();
				break;
			default:
				throw new UnsupportedOperationException("The recovery type is not supported by the recovery background process.");
		}
		
		
		long currTime = nodeAccessor.getEnvironment().getTimeProvider().getCurrentTime();
    	long nextSchedTime = currTime + scheduleInterval; 
    	setNextSchedTime(nextSchedTime);
		
	}




	public static Object[] createRecoveryBackgroundProcessEntryPointArgsForSetRecoveryPlanAndInterval(HyCubeRecoveryType[] recoveryPlan, Integer interval) {
		Object[] parameters = new Object[] {EntryPointType.SET_RECOVERY_PLAN_AND_INTERVAL, recoveryPlan, interval};
		return parameters;
	}
	
	public static Object[] createRecoveryBackgroundProcessEntryPointArgsForSetRecoveryPlan(HyCubeRecoveryType[] recoveryPlan) {
		Object[] parameters = new Object[] {EntryPointType.SET_RECOVERY_PLAN, recoveryPlan};
		return parameters;
	}
	
	public static Object[] createRecoveryBackgroundProcessEntryPointArgsForSetRecoveryScheduleInterval(Integer interval) {
		Object[] parameters = new Object[] {EntryPointType.SET_RECOVERY_SCHEDULE_INTERVAL, interval};
		return parameters;
	}
	
	
	
}
