package net.hycube.dht;

public class HyCubeRefreshPutRequestData {

	protected int commandId;
	
	protected RefreshPutCallback refreshPutCallback;
	protected Object refreshPutCallbackArg;
	
	
	
	public int getCommandId() {
		return commandId;
	}
	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	public RefreshPutCallback getRefreshPutCallback() {
		return refreshPutCallback;
	}
	public void setRefreshPutCallback(RefreshPutCallback refreshPutCallback) {
		this.refreshPutCallback = refreshPutCallback;
	}
	public Object getRefreshPutCallbackArg() {
		return refreshPutCallbackArg;
	}
	public void setRefreshPutCallbackArg(Object refreshPutCallbackArg) {
		this.refreshPutCallbackArg = refreshPutCallbackArg;
	}
	
	
	
	
}
