package poke.server.nconnect;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.server.management.HeartbeatData;
import poke.server.management.HeartbeatManager;
import poke.server.storage.jdbc.DatabaseStorage;
import eye.Comm.Header;
import eye.Comm.Payload;
import eye.Comm.Document;

/**
 * @author Kaustubh
 * @version 2.3
 * {@code : Manages the public TCP connections with the active nodes}
 * {@code : Decides the eligible nodes for request forwarding and deals with the responses of forwarded requests}
 *
 */
public class NodeResponseQueue {

        static private volatile ConcurrentHashMap<String, NodeClient> activeNodeMap = new ConcurrentHashMap<String, NodeClient>();

        static private volatile ConcurrentHashMap<String, NodeClient> activeExternalNodeMap = new ConcurrentHashMap<String, NodeClient>();

        static private volatile  ConcurrentHashMap<String, String> docAddHSResponseQueue =  new ConcurrentHashMap<String, String>();

        protected static Logger logger = LoggerFactory.getLogger("NodeResponseQueue ");

        static final private DatabaseStorage dbAct = DatabaseStorage.getInstance();

        static final int RESPONSE_WAIT_MAX_RETRY = 5;

        static final int RESPONSE_WAIT_MAX_TIME = 5000;

        static final int MAX_CHUNK_SIZE = 26214400;
        static final int MAX_ATTEMPT = 5;
        

       // static private HashMap<String, String> responseToChunkMap = new HashMap<String, String>();

        public static void addActiveNode(String nodeId, NodeClient node) {


                activeNodeMap.put(nodeId, node);
        }

        public static void addExternalNode(String nodeId , NodeClient node){

                activeExternalNodeMap.put(nodeId, node);
        }

        public static NodeClient getActiveNode(String nodeId){

                return activeNodeMap.get(nodeId);
        }
        
        public static NodeClient getActiveExternalNode(String nodeId){

            return activeExternalNodeMap.get(nodeId);
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

        public static boolean nodeExistCheck(String nodeId) {

                return activeNodeMap.containsKey(nodeId);
        }

        public static int activeNodesCount() {

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
                                NodeClient[] nc = getActiveNodeInterface(true);
                                boolean docAddHSSuccess = false;
                                String finalizedNodeId = null;
outer:                                for(int i=0 ; i < activeNodeCount ; i++){

                                        NodeClient selectedNode = nc[i];

                                        selectedNode.docAddHandshake(directory, fName, fileSize);

                                        int attempt = 0;
                                
                                        try {

inner:                                                do{
                                                        logger.info("Sleeping for configured wait time !!! Waiting for response for docAddHandshake from node "+selectedNode.getNodeId()+" attempt no "+(attempt+1));        
                                                        Thread.sleep(RESPONSE_WAIT_MAX_TIME);
                                                        String addHSResponse = selectedNode.checkDocADDHSResponse(directory, fName);
                                                        logger.info(" Response for docAddHandshake from "+selectedNode.getNodeId()+" as "+addHSResponse);
                                                        if(addHSResponse.equals("NA")){
                                                                attempt++;
                                                                continue inner;
                                                        }
                                                        else if(addHSResponse.equalsIgnoreCase("Failure"))
                                                                continue outer;
                                                        else if(addHSResponse.equalsIgnoreCase("Success")){
                                                                docAddHSResponseQueue.put(directory+fName, selectedNode.getNodeId());
                                                                docAddHSSuccess = true;
                                                                finalizedNodeId = selectedNode.getNodeId();
                                                                logger.info(" inserting entry into docAddHSQueue with key as "+directory+fName);
                                                                break outer;
                                                        }
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);


                                        } catch (InterruptedException e) {

                                                e.printStackTrace();
                                        }
                                }
                                
                                if(docAddHSSuccess){
                                        logger.info("Doc add handshake successful for "+directory+"\\"+fName+" with "+finalizedNodeId);
                                }else{
                                        logger.info("Doc add handshake failed for "+directory+"\\"+fName+" no node is available/ready for handshake");
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

                for (NodeClient nc : activeNodeArray) {

                        nc.queryFile(nameSpace, fileName);
                }
        }
        
        public static void broadcastReplicaQuery(String filePath){


                NodeClient[] activeNodeArray = getActiveNodeInterface(true);
                
                String path =         FilenameUtils.getPath(filePath);
                String trimmedPath = path.substring(path.indexOf(File.separator)+1);
                String fileName = FilenameUtils.getName(filePath);

                for(NodeClient nc: activeNodeArray){

                        nc.queryReplica(trimmedPath, fileName);
                }
        }
        
        public static void broadcastDocFind(String nameSpace, String fileName , boolean internal) {

                NodeClient[] activeNodeArray = getActiveNodeInterface(internal);

                for (NodeClient nc : activeNodeArray) {

                        nc.findFile(nameSpace, fileName);
                }
        }

        public static void broadcastNamespaceQuery(String nameSpace){

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);

                for(NodeClient nc: activeNodeArray){

                        nc.queryNamespace(nameSpace);
                }
        }

        public static void broadcastNamespaceListQuery(String nameSpace) {
                // TODO Auto-generated method stub

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);

                for(NodeClient nc: activeNodeArray){

                        nc.queryNamespaceList(nameSpace);
                }

        }


        /*public static boolean fetchDocQueryResult( String nameSpace , String fileName){

                boolean queryResult = true;

                NodeClient[] activeNodeArray = getActiveNodeInterface();

                for (NodeClient nc : activeNodeArray) {*/

        public static void broadcastDocRemoveQuery(String nameSpace , String fileName){

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);

                for(NodeClient nc: activeNodeArray){

                        logger.info("Forwarding docRemove request to "+nc.getNodeId());

                        nc.removeDoc(nameSpace, fileName);
                }
        }

        public static String multicastReplicaRemoveQuery(String nameSpace , String fileName){
        	
        		String trimmedPath = nameSpace.substring(nameSpace.indexOf(File.separator)+1);

                String replicatedNode = dbAct.getReplicatedNode(nameSpace, fileName);

                if(replicatedNode.equals("DNE")){
                        logger.warn("multicastReplicaRemoveQuery: Document "+nameSpace+fileName+" does not exists in database: verify the doccAdd and/or supplied params");
                        return replicatedNode ;
                }else if (replicatedNode.equals("NR")){
                        logger.warn("multicastReplicaRemoveQuery: Document "+nameSpace+fileName+" has not been replicated yet: Safe to delete instanstly");
                        return replicatedNode;
                } else{

                        NodeClient replicatedNC = getActiveNode(replicatedNode);
                        replicatedNC.removeReplica(trimmedPath, fileName);
                        return "wait";
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

outer:                for(NodeClient nc: activeNodeArray){
                        int attempt = 0;
                        
inner:                        do{
                logger.info("Sleeping for configured wait time !!! Waiting for response for docQuery from node "+nc.getNodeId()+" attempt no "+(attempt+1));
                        try {
                                Thread.sleep(RESPONSE_WAIT_MAX_TIME);
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                        String result = nc.checkDocQueryResponse(nameSpace, fileName);

                        if(result.equalsIgnoreCase("Success")){
                                logger.info("Document "+nameSpace+"/"+fileName+" exists with "+nc.getNodeId());
                                return true;
                        }else if(result.equalsIgnoreCase("NA")){
                                logger.warn("No response from node "+nc.getNodeId()+"for document query of "+nameSpace+"/"+fileName);
                                continue inner;
                        }else if(result.equalsIgnoreCase("Failure")){
                                logger.warn("No response from node "+nc.getNodeId()+"for document query of "+nameSpace+"/"+fileName);
                                continue outer;
                        }
                        
                 }while (attempt < RESPONSE_WAIT_MAX_RETRY);
                }
                return false;
        }
        
        public static boolean fetchReplicaQueryResult(String filePath){

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);
                
                String path =         FilenameUtils.getPath(filePath);
                String trimmedPath = path.substring(path.indexOf(File.separator)+1);
                String fileName = FilenameUtils.getName(filePath);

outer:                for(NodeClient nc: activeNodeArray){
                        int attempt = 0;
                        
inner:                        do{
                
                logger.info("Sleeping for configured wait time !!! Waiting for response for replicaQuery from node "+nc.getNodeId()+" attempt no "+(attempt+1));
                        try {
                                Thread.sleep(RESPONSE_WAIT_MAX_TIME);
                        } catch (InterruptedException e) {
                                e.printStackTrace();
                        }
                        String result = nc.checkReplicaQueryResponse(trimmedPath, fileName);

                        if(result.equalsIgnoreCase("Success")){
                                logger.info("Replica "+trimmedPath+fileName+" exists with "+nc.getNodeId());
                                return true;
                        }else if(result.equalsIgnoreCase("NA")){
                                logger.warn("No response from node "+nc.getNodeId()+"for replica query of "+trimmedPath+"/"+fileName);
                                attempt++;
                                continue inner;
                        }else if(result.equalsIgnoreCase("Failure")){
                                logger.warn("Replica "+trimmedPath+fileName+" does not exists at "+nc.getNodeId());
                                continue outer;
                        }
                        
                 }while (attempt < RESPONSE_WAIT_MAX_RETRY);
                }
                return false;
        }

        public static String fetchDocRemoveResult( String nameSpace , String fileName){

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);
                
                String result = "NA";

                for(NodeClient nc: activeNodeArray){

                        result = nc.checkDocRemoveResponse(nameSpace, fileName);

                        if(result.equalsIgnoreCase("Success")){
                                logger.info("Document remove successful for "+nameSpace+"/"+fileName+" by nodeId "+nc.getNodeId());
                                return result;
                        }else if(result.equalsIgnoreCase("NA")){
                                logger.warn("No response from node "+nc.getNodeId()+"for document remove of "+nameSpace+"/"+fileName);
                        }else if(result.equalsIgnoreCase("Failure"))
                                logger.warn("DocRemoveResponse: Node "+nc.getNodeId()+" does not have "+nameSpace+"/"+fileName);
                }

                return result;
        }

        public static boolean fetchReplicaRemoveResult(String nameSpace , String fileName){

                String replicatedNode = dbAct.getReplicatedNode(nameSpace, fileName);
                String trimmedPath = nameSpace.substring(nameSpace.indexOf(File.separator)+1);
                logger.info(" fetchReplicaRemoveResult: Replica node for "+nameSpace+fileName+"is "+replicatedNode);
                String result = "";
                NodeClient replNode =  null;
                try{

                        replNode = getActiveNode(replicatedNode);
                        
                        if(replNode == null){
                        	logger.error(" Node not found in active node map "+replicatedNode);
                        	return false;
                        }
                        result = replNode.checkReplicaRemoveResponse(trimmedPath, fileName);

                }catch(Exception e){
                        logger.error("fetchReplicaRemoveResult: Encountered Exception "+e.getMessage());
                        e.printStackTrace();
                        return false;
                }

                if(result.equalsIgnoreCase("Failure")){
                        logger.error("Document Replica remove failed for "+nameSpace+"\\"+fileName);
                        return false;
                }else if(result.equalsIgnoreCase("NA")){
                        logger.warn("No response from node "+replNode.getNodeId()+"for document remove of "+nameSpace+fileName);
                        return false;
                }else if(result.equalsIgnoreCase("Success"))
                        logger.info("Document Replica remove successful for "+replNode.getNodeId()+" of "+nameSpace+fileName);
                return true;
        }

        public static void multicastDocReplication(String filePath){

                final String actFilePath = filePath;

                final NodeClient[] activeNodeArray = getActiveNodeInterface(true);

                new Thread(){

                        public void run(){

                                boolean replicated = false;
                                boolean replicaHS = false;
                                String finalNodeId = null;
                                NodeClient finalizedNode = null;
                                File unRepFile = new File(actFilePath);
                                String path = FilenameUtils.getPath(actFilePath);
                                String trimmedPath = path.substring(path.indexOf(File.separator)+1);
                                String fileName = FilenameUtils.getName(actFilePath);
                                long size = FileUtils.sizeOf(unRepFile);

outer:                                for(NodeClient nc: activeNodeArray){
                                        int attempt  = 0;
                                        if(HeartbeatManager.getInstance().checkNodeStatus(nc.getNodeId()) == HeartbeatData.BeatStatus.Active ){

                                                try{

                                                        nc.replicaHandshake(trimmedPath , fileName , size);

                                                        logger.info("Replica handshake sent to node "+nc.getNodeId()+" for file "+actFilePath);

inner:                                                do{
                                                                logger.info("Sleeping for configured wait time !!! Waiting for response for ReplicaHS from node "+nc.getNodeId()+" attempt no "+(attempt+1));        
                                                                Thread.sleep(RESPONSE_WAIT_MAX_TIME);
                                                                String replicaHSResp = nc.checkReplicaHSResponse(trimmedPath, fileName);
                                                                if(replicaHSResp.equals("NA")){
                                                                        attempt++;
                                                                        continue inner;
                                                                }
                                                                else if(replicaHSResp.equalsIgnoreCase("Failure"))
                                                                        continue outer;
                                                                else if(replicaHSResp.equalsIgnoreCase("Success")){
                                                                        finalNodeId = nc.getNodeId();
                                                                        dbAct.setReplicationInProgress(path, fileName);
                                                                        finalizedNode = nc;
                                                                        replicaHS = true;
                                                                        break outer;
                                                                }
                                                        }while(attempt < RESPONSE_WAIT_MAX_RETRY);

                                                }catch(Exception e){
                                                        logger.error("Unexpected failure during replication of "+actFilePath+" with node "+nc.getNodeId()+" "+e.getMessage());
                                                        e.printStackTrace();
                                                }

                                        }else{

                                                logger.warn("Node "+nc.getNodeId()+" selected for replication is not active: Proceeding to validate next");
                                                continue outer;
                                        }

                                }

                                if(finalNodeId != null && finalizedNode !=null){

                                        logger.info(finalNodeId+" has been finalized for the replication of "+actFilePath);
                                        int totalChunks = (int) ((size/MAX_CHUNK_SIZE)+1);
                                        logger.info("Total number of chunks to be transferred for "+actFilePath+" of size "+size+" are "+totalChunks);
                                        try{

                                                
outer:                                        for(int chunkId = 0 ; chunkId< totalChunks ; chunkId++){
        
                                                        FileInputStream fis =  new FileInputStream(unRepFile);
                                                        
                                                        byte[] chunkContents =  null;
                                                        fis.skip(chunkId*MAX_CHUNK_SIZE);

                                                        if(fis.available() > MAX_CHUNK_SIZE)
                                                                chunkContents = new byte[MAX_CHUNK_SIZE];
                                                        else
                                                                chunkContents = new byte[fis.available()];
                                                        logger.info("Chunk "+(chunkId+1)+" to be transferred for "+actFilePath+" of size "+chunkContents.length+" to "+finalNodeId);
                                                        int bytesRead  = fis.read(chunkContents, 0, chunkContents.length );
                                                        if(bytesRead==0)
                                                                logger.error("Unable to read bytes from file "+actFilePath);
                                                        finalizedNode.addReplica(trimmedPath, fileName, chunkContents, totalChunks, (chunkId+1));

                                                        logger.info("Chuck "+(chunkId+1)+"has been sent to node "+finalizedNode.getNodeId()+" for file "+actFilePath);
                                                        int attempt = 0;

inner:                                                do{
                                                                logger.info("Sleeping for configured wait time !!! Waiting for response for addReplica from node "+finalizedNode.getNodeId()+" attempt no "+(attempt+1));        
                                                                Thread.sleep(RESPONSE_WAIT_MAX_TIME);
                                                                String replicaHSResp = finalizedNode.checkAddReplicaResponse(trimmedPath, fileName);
                                                                if(replicaHSResp.equals("NA")){
                                                                        attempt++;
                                                                        continue inner;
                                                                }
                                                                else if(replicaHSResp.equalsIgnoreCase("Failure")){
                                                                        logger.error("Failure response received from node "+finalNodeId+" for addReplica of "+actFilePath+" for chunk "+(chunkId+1));
                                                                        logger.error(" Aborting the replication process for "+actFilePath);
                                                                        dbAct.resetReplicationInProgress(path, fileName);
                                                                        break outer;
                                                                }
                                                                else if(replicaHSResp.equalsIgnoreCase("Success")){
                                                                        logger.error("Success response received from node "+finalNodeId+" for addReplica of "+actFilePath+" for chunk "+(chunkId+1));
                                                                        dbAct.setReplicationInProgress(path, fileName);
                                                                        if((chunkId+1) == totalChunks){
                                                                                dbAct.updateReplicationCount(path, fileName, finalNodeId, 1);
                                                                                dbAct.resetReplicationInProgress(path, fileName);
                                                                                replicated = true;
                                                                                break outer;
                                                                        }else
                                                                        continue outer;
                                                                }
                                                        }while(attempt < RESPONSE_WAIT_MAX_RETRY);
                                                        fis.close();
                                        }

                                        }catch(IOException ioExcep){
                                                logger.error("IOException occurred while replicating "+actFilePath+" on "+finalNodeId+" reasosn: "+ioExcep.getMessage());
                                                return;
                                        } catch (InterruptedException e) {
                                                
                                                e.printStackTrace();
                                        }
                                        
                                        if(replicated && replicaHS)
                                                logger.info("Replication process completed successfully for "+actFilePath+" with node "+finalNodeId);
                                        else{
                                                dbAct.resetReplicationInProgress(path, fileName);
                                                logger.info("Replication process failed for "+actFilePath+" with node "+finalNodeId);
                                        }
                                }else{

                                        logger.warn("No network node ready/available for replication of "+actFilePath+" of size "+size);
                                        return;
                                }
                        }// run method ends


                }.start();        // thread constructor ends
        }



                        /*if (result.equalsIgnoreCase("Failure")) {
                                logger.info("Document upload validation failed for "
                                                + nameSpace + "/" + fileName);
                                return false;
                        } else if (result.equalsIgnoreCase("NA"))
                                logger.warn("No response from node " + nc.getNodeId()
                                                + "for document upload validation for " + nameSpace
                                                + "/" + fileName);

                }

                return queryResult;

        }*/

        public static String fetchDocFindResult(String nameSpace, String fileName , boolean internal) {
                String queryResult = null;

                NodeClient[] activeNodeArray = getActiveNodeInterface(internal);
                try{
                for (NodeClient nc : activeNodeArray) {
                        String result = "NA";
                        int attempt = 0;
                        do{
                                result = nc.checkDocFindResponse(nameSpace, fileName);
                                attempt++;
                                Thread.sleep(4000);
                        }while(result.equalsIgnoreCase("NA") || attempt <= 7);
                        

                        if (result.equalsIgnoreCase("Failure")) {
                                logger.info("Document with the given name "
                                                + nameSpace + "/" + fileName +" was not found.");
                                return "Failure";
                        } else if (result.equalsIgnoreCase("Success")){
                                logger.info("Document with the given name "
                                                + nameSpace + "/" + fileName +" was found.");
                                return "Success";                
                        }else if (result.equalsIgnoreCase("NA")){
                            logger.info("Document with the given name "
                                    + nameSpace + "/" + fileName +" was not found in time.");
                    return "Failure";                
            }
                        
                }
                }
                catch(InterruptedException ex){
                        ex.printStackTrace();
                }
                return queryResult;
        }

        public static String fetchNamespaceRemoveResult( String nameSpace){

                NodeClient[] activeNodeArray = getActiveNodeInterface(true);
                
                String result = "NA";

                for(NodeClient nc: activeNodeArray){

                        result = nc.checkNamespaceRemoveResponse(nameSpace);

                        if(result.equalsIgnoreCase("Success")){
                                logger.info("Namespace remove successful for "+nameSpace);
                                return result;
                        }else if(result.equalsIgnoreCase("NA")){
                                logger.warn("No response from node "+nc.getNodeId());
                        }else if(result.equalsIgnoreCase("Failure"))
                                logger.warn("namespaceRemoveResponse: Node "+nc.getNodeId()+" does not have "+nameSpace);
                        }

                return result;
        }
        
        
        public static List fetchNamespaceList(String namespace){
               // boolean queryResult = true;
                List<Document> fileList= new ArrayList<Document>();
                List<Document> newFileList = new ArrayList<Document>();
                logger.info("In fetchNameSpaceList");
                NodeClient[] activeNodeArray = getActiveNodeInterface(true);
                int attempt = 0;

                for (NodeClient nc : activeNodeArray) {
                      //  String result = "NA";

                        //  while(result.equalsIgnoreCase("NA")){
                        do{
                        
                        try {
                                Thread.sleep(8000);
                        } catch (InterruptedException e) {
                                // TODO Auto-generated catch block
                                logger.error("Thread exception in fetchNamespaceList "+e.getMessage());
                        }
                        fileList = (List<Document>) (nc.sendNamespaceList(namespace));
                        if (fileList.isEmpty()){
                                attempt++;
                                continue;
                        }else{
                                
                                newFileList.addAll(fileList);
                                attempt = MAX_ATTEMPT;
                              //  break;
                        }
                        
                        }while(attempt < MAX_ATTEMPT);
                        
                        logger.info("Files returned from fetchNameSpaceList " +newFileList);
                       // return newFileList;                

                }

                return newFileList;
        }

}