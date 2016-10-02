package net.hycube.dht;

public class HyCubeDeleteRequestData {

	protected int commandId;
	
	protected DeleteCallback deleteCallback;
	protected Object deleteCallbackArg;
	
	
	
	public int getCommandId() {
		return commandId;
	}
	public void setCommandId(int commandId) {
		this.commandId = commandId;
	}
	public DeleteCallback getDeleteCallback() {
		return deleteCallback;
	}
	public void setDeleteCallback(DeleteCallback deleteCallback) {
		this.deleteCallback = deleteCallback;
	}
	public Object getDeleteCallbackArg() {
		return deleteCallbackArg;
	}
	public void setDeleteCallbackArg(Object deleteCallbackArg) {
		this.deleteCallbackArg = deleteCallbackArg;
	}
	
	
	
	
}
