package net.hycube.dht;

public class HyCubePutRequestData {

	protected int commandId;
	
	protected PutCallback putCallback;
	protected Object putCallbackArg;
	
	
	
	public int getCommandId() {
		return commandId;
	}
	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	public PutCallback getPutCallback() {
		return putCallback;
	}
	public void setPutCallback(PutCallback putCallback) {
		this.putCallback = putCallback;
	}
	public Object getPutCallbackArg() {
		return putCallbackArg;
	}
	public void setPutCallbackArg(Object putCallbackArg) {
		this.putCallbackArg = putCallbackArg;
	}
	
	
	
	
}
