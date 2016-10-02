package net.hycube;

import java.math.BigInteger;

import net.hycube.core.NodePointer;
import net.hycube.dht.DeleteCallback;
import net.hycube.dht.GetCallback;
import net.hycube.dht.HyCubeResource;
import net.hycube.dht.HyCubeResourceDescriptor;
import net.hycube.dht.PutCallback;
import net.hycube.dht.RefreshPutCallback;


public interface HyCubeNodeService extends NodeService {

	//HyCubeDHTManager methods:
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg);
	
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg);
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg);
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg);
	
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg);
	
	public PutCallback put(NodePointer recipient, BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public PutCallback put(BigInteger key, HyCubeResource resource, PutCallback putCallback, Object putCallbackArg, Object[] parameters);
	public RefreshPutCallback refreshPut(BigInteger key, HyCubeResourceDescriptor resourceDescriptor, RefreshPutCallback refreshPutCallback, Object refreshPutCallbackArg, Object[] parameters);
	public GetCallback get(BigInteger key, HyCubeResourceDescriptor criteria, GetCallback getCallback, Object getCallbackArg, Object[] parameters);
	
	public DeleteCallback delete(NodePointer recipient, BigInteger key, HyCubeResourceDescriptor criteria, DeleteCallback deleteCallback, Object deleteCallbackArg, Object[] parameters);
	
		
	

	//HyCubeRecoveryExtension method:
	public void recover();
	public void recoverNS();
	

	
	

}