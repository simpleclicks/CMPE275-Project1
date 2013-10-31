package poke.server.nconnect;

import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Header;
import eye.Comm.Payload;
import eye.Comm.Request;
import eye.Comm.Response;

public class NodeResponseQueue {

	static private volatile ConcurrentHashMap<String, NodeClient> activeNodeMap = new ConcurrentHashMap<String, NodeClient>();
	
	static private volatile ConcurrentHashMap<String, NodeClient> activeExternalNodeMap = new ConcurrentHashMap<String, NodeClient>();

	static private volatile  ConcurrentHashMap<String, String> docAddHSResponseQueue =  new ConcurrentHashMap<String, String>();

	private static final int MAXWAITFORRESPONSE = 5000;

	protected static Logger logger = LoggerFactory.getLogger("NodeResponseQueue");

	//private static Random nodeIdSelector = new Random();

	public static void addActiveNode(String nodeId , NodeClient node){

		activeNodeMap.put(nodeId, node);
	}

	public static void addExternalNode(String nodeId , NodeClient node){

		activeExternalNodeMap.put(nodeId, node);
	}

	public static NodeClient getActiveNode(String nodeId){

		return activeNodeMap.get(nodeId);
	}
	
	public static NodeClient getExternalNode(String nodeId){

		return activeExternalNodeMap.get(nodeId);
	}

	public static void removeInactiveNode(String nodeId){

		activeNodeMap.remove(nodeId);
	}
	
	public static void removeExternalNode(String nodeId){

		activeExternalNodeMap.remove(nodeId);
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
	
	public static boolean externalNodeExistCheck(String nodeId){

		return activeExternalNodeMap.containsKey(nodeId);
	}

	public static int externalNodesCount(){

		return activeExternalNodeMap.size();
	}

	public static NodeClient[] getActiveNodeInterface(boolean internal){

		Collection<NodeClient> activeNodes = null;
		
		if(internal)
			activeNodes = activeNodeMap.values();
		else
			activeNodes =  activeExternalNodeMap.values();

		NodeClient[] activeNodeArray = new NodeClient[activeNodes.size()];

		activeNodes.toArray(activeNodeArray);

		return activeNodeArray;
}

	public static void multicastDocAddHandshake(String  nameSpace, String fileName , long reqFileSize){

		final String directory  = nameSpace;

		final String fName  = fileName;

		final long fileSize  = reqFileSize;

		new Thread(){

			@Override
			public void run(){
				int activeNodeCount = activeNodesCount();

				for(int i=0 ; i < activeNodeCount ; i++){

					//int index = nodeIdSelector.nextInt(activeNodeCount);

					NodeClient[] nc = getActiveNodeInterface(true);

					NodeClient selectedNode = nc[i];

					selectedNode.docAddHandshake(directory, fName, fileSize);

					logger.info(" DocADDHS thread sleeping for 3000ms! waiting for response for docAddHandshake from "+selectedNode.getNodeId());

					try {

						Thread.sleep(MAXWAITFORRESPONSE);

					} catch (InterruptedException e) {

						e.printStackTrace();
					}

					String addHSResponse = selectedNode.checkDocADDHSResponse(directory, fName);
					
					logger.info(" Response for docAddHandshake from "+selectedNode.getNodeId()+" as "+addHSResponse);

					if(addHSResponse.equalsIgnoreCase("Success")){
						docAddHSResponseQueue.put(directory+fName, selectedNode.getNodeId());
						logger.info(" inserting entry into docAddHSQueue with key as "+directory+fName);
						break;
					}
				}
			}
		}.start();

	}

	public static void forwardDocADDRequest(String nodeId, Header docAddHeader , Payload docAddBody){
		
		if(activeNodeMap.containsKey(nodeId)){
		
			NodeClient node = activeNodeMap.get(nodeId);
		
			node.docAdd(docAddHeader, docAddBody);
		}else{
			
			logger.error(" forwardDocADDRequest: Node "+nodeId+" does not exist in the network: Validate documentResource,nconnect");
		}
	
		System.gc();
	}
	
	public static boolean  fetchDocAddResult( String nodeId ,String nameSpace , String fileName){

		System.gc();
		
		String response = null;
		
		if(activeNodeMap.containsKey(nodeId)){
			
			NodeClient node = activeNodeMap.get(nodeId);
			
			response = node.checkDocAddResponse(nameSpace, fileName);
			
			logger.info(" Response for docAdd for "+nameSpace+"/"+fileName+" from "+nodeId+" as "+response);
			
		}else{
			
			logger.error(" fetchDocAddResult: Node "+nodeId+" does not exist in the network: Validate documentResource,nconnect");
		}
		
		if(response.equalsIgnoreCase("Success"))
			return true;
		else
			return false;
	}
	
	public static void broadcastDocQuery(String nameSpace , String fileName , boolean internal){

		NodeClient[] activeNodeArray = getActiveNodeInterface(internal);

		for(NodeClient nc: activeNodeArray){

			nc.queryFile(nameSpace, fileName);
		}
	}

	public static void broadcastDocRemoveQuery(String nameSpace , String fileName){

		NodeClient[] activeNodeArray = getActiveNodeInterface(true);

		for(NodeClient nc: activeNodeArray){

			logger.info("Forwarding docRemove request to "+nc.getNodeId());

			nc.removeDoc(nameSpace, fileName);
		}
	}

	public static void multicastReplicaRemoveQuery(String nameSpace , String fileName){

		NodeClient[] activeNodeArray = getActiveNodeInterface(true);

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


	public static String  fetchDocAddHSResult( String nameSpace , String fileName){

		String key = nameSpace+fileName;
		
		logger.info(" fetchDocAddHSResult for key "+key);

		if(docAddHSResponseQueue.containsKey(key)){
			return docAddHSResponseQueue.get(key);
		}
		else{
			logger.info("Key not found in docAddHSResponseQueue");
			logger.info("size of docAddHSResponseQueue "+docAddHSResponseQueue.size());
			return "NA";
		}
	}


	public static boolean fetchDocQueryResult( String nameSpace , String fileName , boolean internal){

		NodeClient[] activeNodeArray = getActiveNodeInterface(internal);

		for(NodeClient nc: activeNodeArray){

			String result = nc.checkDocQueryResponse(nameSpace, fileName);

			if(result.equalsIgnoreCase("Success")){
				logger.info("Document "+nameSpace+"/"+fileName+" exists with "+nc.getNodeId());
				return true;
			}else if(result.equalsIgnoreCase("NA"))
				logger.warn("No response from node "+nc.getNodeId()+"for document query of "+nameSpace+"/"+fileName);

		}

		return false;
	}

	public static boolean fetchDocRemoveResult( String nameSpace , String fileName){

		boolean queryResult = false;

		NodeClient[] activeNodeArray = getActiveNodeInterface(true);

		for(NodeClient nc: activeNodeArray){

			String result = nc.checkDocRemoveResponse(nameSpace, fileName);

			if(result.equalsIgnoreCase("Success")){
				logger.info("Document remove successful for "+nameSpace+"/"+fileName+" by nodeId "+nc.getNodeId());
				return true;
			}else if(result.equalsIgnoreCase("NA")){
				logger.warn("No response from node "+nc.getNodeId()+"for document remove of "+nameSpace+"/"+fileName);
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

		NodeClient[] activeNodeArray = getActiveNodeInterface(true);

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
