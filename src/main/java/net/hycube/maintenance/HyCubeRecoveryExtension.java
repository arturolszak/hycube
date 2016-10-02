package net.hycube.maintenance;

import java.util.Arrays;

import net.hycube.core.InitializationException;
import net.hycube.core.NodeAccessor;
import net.hycube.environment.NodeProperties;
import net.hycube.extensions.Extension;

public class HyCubeRecoveryExtension implements Extension, HyCubeRecoveryExtensionEntryPoint {

	
	public enum RecoveryExtensionEntryPointType {
		RECOVER,
		RECOVER_NS,
		
	}
	
	
	
	protected static final String PROP_KEY_RECOVERY_MANAGER = "RecoveryManager";
	
	
	protected NodeAccessor nodeAccessor;
	protected NodeProperties properties;
	protected NodeProperties recoveryManagerProperties;
	
	protected HyCubeRecoveryManager recoveryManager;
	
	
	
	public HyCubeRecoveryManager getRecoveryManager() {
		return recoveryManager;
	}
	
	
	@Override
	public void initialize(NodeAccessor nodeAccessor, NodeProperties properties) throws InitializationException {
		
		this.nodeAccessor = nodeAccessor;
		this.properties = properties;
		
		String recoveryManagerKey = properties.getProperty(PROP_KEY_RECOVERY_MANAGER);
		if (recoveryManagerKey == null || recoveryManagerKey.trim().isEmpty()) throw new InitializationException(InitializationException.Error.INVALID_PARAMETER_VALUE, properties.getAbsoluteKey(PROP_KEY_RECOVERY_MANAGER), "Invalid parameter value: " + properties.getAbsoluteKey(PROP_KEY_RECOVERY_MANAGER));
		recoveryManagerProperties = properties.getNestedProperty(PROP_KEY_RECOVERY_MANAGER, recoveryManagerKey);
		recoveryManager = new HyCubeRecoveryManager();
		
		
		
	}
	
	
	@Override
	public void postInitialize() throws InitializationException {
		
		recoveryManager.initialize(this.nodeAccessor, this.recoveryManagerProperties);
		
		
	}

	
	@Override
	public HyCubeRecoveryExtensionEntryPoint getExtensionEntryPoint() {
		return (HyCubeRecoveryExtensionEntryPoint)this;
	}
	
	@Override
	public Object call() {
		throw new UnsupportedOperationException("The entry point method with 0 arguments is not implemented.");
	}
	
	@Override
	public Object call(Object arg) {
		return call(new Object[] {arg});
	}

	@Override
	public Object  call(Object entryPoint, Object[] args) {
		if (entryPoint instanceof RecoveryExtensionEntryPointType) return callExtension((RecoveryExtensionEntryPointType)entryPoint, args);
		else throw new IllegalArgumentException("Invalid argument. Cannot cast args[0] to RecoveryExtensionEntryPointType");
	}
	
	@Override
	public Object call(Object[] args) {
		if (args == null || args.length < 1) throw new IllegalArgumentException("The argument specified is null or empty.");
		if (!(args[0] instanceof RecoveryExtensionEntryPointType)) throw new IllegalArgumentException("Invalid argument. Cannot cast args[0] to RecoveryExtensionEntryPointType");
		RecoveryExtensionEntryPointType entryPoint = (RecoveryExtensionEntryPointType) args[0];
		return callExtension(entryPoint, Arrays.copyOfRange(args, 1, args.length));
	}
	
	public Object callExtension(RecoveryExtensionEntryPointType entryPoint, Object[] args) {
		switch (entryPoint) {
			case RECOVER:
				callRecover();
				break;
			case RECOVER_NS:
				callRecoverNS();
				break;
		}
		return null;
	}
	
	
	@Override
	public void callRecover() {
		recover();
	}
	
	
	protected void recover() {
		recoveryManager.recover();
		
	}
	
	
	@Override
	public void callRecoverNS() {
		recoverNS();
	}
	
	
	protected void recoverNS() {
		recoveryManager.recoverNS();
	}
	

	
	
	
	
	@Override
	public void discard() {
		recoveryManager.discard();
		
	}



	
	public static Object[] createRecoveryExtensionEntryPointArgs(RecoveryExtensionEntryPointType entryPoint) {
		Object[] parameters = new Object[] {entryPoint};
		return parameters;
	}
	
	
	

}
