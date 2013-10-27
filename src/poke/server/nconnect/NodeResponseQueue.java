package poke.server.nconnect;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Response;

public class NodeResponseQueue {
	
	static private volatile ConcurrentHashMap<String, NodeClient> activeNodeMap = new ConcurrentHashMap<String, NodeClient>();
	
	//static private volatile ConcurrentHashMap<String, Response> docQueryResponseQueue =  new ConcurrentHashMap<String, Response>();
	
	protected static Logger logger = LoggerFactory.getLogger("NodeResponseQueue");
	
	public static void addActiveNode(String nodeId , NodeClient node){
		
		activeNodeMap.put(nodeId, node);
	}
	
	public static NodeClient getActiveNode(String nodeId){
		
		return activeNodeMap.get(nodeId);
	}
	
	public static void removeInactiveNode(String nodeId){
		
		activeNodeMap.remove(nodeId);
	}
	
//	public static void addDocQueryReponse(String docNameSpace , Response docQueryResponse){
//		
//		docQueryResponseQueue.put(docNameSpace, docQueryResponse);
//	}
//	
//	public static boolean docQueryCheck(String docNameSpace){
//		
//		return docQueryResponseQueue.contains(docNameSpace);
//	}
	
	public static boolean nodeExistCheck(String nodeId){
		
		return activeNodeMap.containsKey(nodeId);
	}
	
	public static int activeNodesCount(){
		
		return activeNodeMap.size();
	}
	
	public static NodeClient[] getActiveNodeInterface(){
		
		Collection<NodeClient> activeNodes = activeNodeMap.values();

		NodeClient[] activeNodeArray = new NodeClient[activeNodes.size()];

		activeNodes.toArray(activeNodeArray);
		
		return activeNodeArray;
		
	}
	
	public static void broadcastDocQuery(String nameSpace , String fileName){
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();

		for(NodeClient nc: activeNodeArray){

			nc.queryFile(nameSpace, fileName);
		}
	}
	
	public static void broadcastDocRemoveQuery(String nameSpace , String fileName){
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();

		for(NodeClient nc: activeNodeArray){
			
			logger.info("Forwarding docRemove request to "+nc.getNodeId());

			nc.removeDoc(nameSpace, fileName);
		}
	}
	
	public static void multicastReplicaRemoveQuery(String nameSpace , String fileName){
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();
		
		// int nodeId[] = fetch the node id list where this file has been replicated
		
//		for (int id : nodeId){
//			
//			NodeClient nc = getActiveNode(id);
//			
//			nc.removeReplica(nameSpace, fileName);
//			
//		}

		for(NodeClient nc: activeNodeArray){

			nc.removeReplica(nameSpace, fileName);
		}
	}
	

	public static boolean fetchDocQueryResult( String nameSpace , String fileName){
		
		boolean queryResult = true;
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();
		
		for(NodeClient nc: activeNodeArray){

			String result = nc.checkDocQueryResponse(nameSpace, fileName);
			
			if(result.equalsIgnoreCase("Failure")){
				logger.info("Document upload validation failed for "+nameSpace+"/"+fileName);
				return false;
			}else if(result.equalsIgnoreCase("NA"))
				logger.warn("No response from node "+nc.getNodeId()+"for document upload validation of "+nameSpace+"/"+fileName);
				
		}
		
			return queryResult;
	}
	
	public static boolean fetchDocRemoveResult( String nameSpace , String fileName){
		
		boolean queryResult = false;
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();
		
		for(NodeClient nc: activeNodeArray){

			String result = nc.checkDocRemoveResponse(nameSpace, fileName);
			
			if(result.equalsIgnoreCase("Success")){
				logger.info("Document remove successful for "+nameSpace+"/"+fileName+" by nodeId "+nc.getNodeId());
				return true;
			}else if(result.equalsIgnoreCase("NA")){
				logger.warn("No response from node "+nc.getNodeId()+"for document upload validation of "+nameSpace+"/"+fileName);
			}else if(result.equalsIgnoreCase("Failure"))
				logger.warn("DocRemoveResponse: Node "+nc.getNodeId()+" does not have "+nameSpace+"/"+fileName);
		}
		
			return queryResult;
	}
	
	public static boolean fetchReplicaRemoveResult(String nameSpace , String fileName){
		
		boolean queryResult = true;
		
// int nodeId[] = fetch the node id list from DB where this file has been replicated
		
//		for (int id : nodeId){
//			
//			NodeClient nc = getActiveNode(id);
//			
//			nc.removeReplica(nameSpace, fileName);
//			
//		}
		
		NodeClient[] activeNodeArray = getActiveNodeInterface();
		
		for(NodeClient nc: activeNodeArray){

			String result = nc.checkReplicaRemoveResponse(nameSpace, fileName);
			
			if(result.equalsIgnoreCase("Failure")){
				logger.info("Document Replica remove failed for "+nameSpace+"/"+fileName);
				return false;
			}else if(result.equalsIgnoreCase("NA")){
				logger.warn("No response from node "+nc.getNodeId()+"for document remove of "+nameSpace+"/"+fileName);
			}else if(result.equalsIgnoreCase("Success"))
				logger.info("Document Replica remove successful for "+nc.getNodeId()+" of "+nameSpace+"/"+fileName);
				
		}
		
			return queryResult;
	}
	
}
