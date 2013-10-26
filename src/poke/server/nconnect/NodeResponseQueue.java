package poke.server.nconnect;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import eye.Comm.Response;

public class NodeResponseQueue {
	
	static private ConcurrentHashMap<String, NodeClient> activeNodeMap = new ConcurrentHashMap<String, NodeClient>();
	
	static private ConcurrentHashMap<String, Response> docQueryResponseQueue =  new ConcurrentHashMap<String, Response>();
	
	public static void addActiveNode(String nodeId , NodeClient node){
		
		activeNodeMap.put(nodeId, node);
	}
	
	public static void removeInactiveNode(String nodeId){
		
		activeNodeMap.remove(nodeId);
	}
	
	public static void addDocQueryReponse(String docNameSpace , Response docQueryResponse){
		
		docQueryResponseQueue.put(docNameSpace, docQueryResponse);
	}
	
	public static boolean docQueryCheck(String docNameSpace){
		
		return docQueryResponseQueue.contains(docNameSpace);
	}
	
	public static boolean nodeExistCheck(String nodeId){
		
		return activeNodeMap.containsKey(nodeId);
	}
	
	public static int activeNodesCount(){
		
		return activeNodeMap.size();
	}
	
	public static Collection<NodeClient> getActiveNodeInterface(){
		
		return activeNodeMap.values();
		
	}
	
	public static void addNewActiveNode(String host , int port , String nodeId){
		
	}

}
