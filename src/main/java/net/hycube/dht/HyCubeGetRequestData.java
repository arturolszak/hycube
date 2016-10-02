package net.hycube.dht;

public class HyCubeGetRequestData {

	protected int commandId;
	
	protected GetCallback getCallback;
	protected Object getCallbackArg;
	
	
	
	public int getCommandId() {
		return commandId;
	}
	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	public GetCallback getGetCallback() {
		return getCallback;
	}
	public void setGetCallback(GetCallback getCallback) {
		this.getCallback = getCallback;
	}
	public Object getGetCallbackArg() {
		return getCallbackArg;
	}
	public void setGetCallbackArg(Object getCallbackArg) {
		this.getCallbackArg = getCallbackArg;
	}
	
	
	
}
