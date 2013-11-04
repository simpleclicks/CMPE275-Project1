/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.resources;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.server.management.HeartbeatManager;
import poke.server.nconnect.NodeResponseQueue;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import poke.server.storage.jdbc.DatabaseStorage;
import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;

public class DocumentResource implements Resource {

        protected static Logger logger = LoggerFactory.getLogger("DocumentResource ");

        private static final String HOMEDIR = "home";

        private static final String VISITORDIR = "away";

        private static final String FILEADDSUCCESSMSG = "File has been uploaded successfully";

        private static final String FILEADDREQMISSINGPARAMMSG = "Fail to validate document upload request : Document name/size (mandatory) has not been provided.";

        private static final String FILEREQMISSINGPARAMMSG = "Fail to validate document find/delete request : Document name (mandatory) has not been provided.";

        private static final String INTERNALSERVERERRORMSG ="Failed to serve the request: Internal Server Error";

        private static final String FILEADDREQDUPLICATEFILEMSG ="Can not upload the file: File already exists: Use docUpdate";

        private static final String FILETOOLARGETOSAVEMSG ="Can not upload the file: File is too large to save";

        private static final String FILEUPLOADREQVALIDATEDMSG ="Valid file upload request: File can be uploaded";

        private static final String NAMESPACEINEXISTENTMSG = " Supplied namespacce does not exist: Please suppy valid namespace";

        private static final String FILEINEXISTENTMSG = " Requested file does not exist: Please suppy valid filename";

        private static final String FILEDELETESUCCESSFULMSG = "Requested file has been deleted successfully";

        private static final String FILEDELETEUNSUCCESSFULMSG = "Requested file can not be deleted at this time: Please try again later";

        private static final String OPERATIONNOTALLOWEDMSG = "Requested Operation is not allowed with the 'request' type ";

        private static final String REQUESTEDFILEEXISTSMSG = "Cluster has the requested file";

        private static final String REQUESTEDFILEDNEXISTSMSG = "Cluster does not have the requested file";

        private static final int MAXWAITFORRESPONSE = 6000;
        
        private static final int MAXWAITFORADDRESPONSE = 7000;

        private static final File homeDir = new File(HOMEDIR);

        private static final File visitorDir = new File(VISITORDIR);

        private static final String EMPTY_STRING = "";
        
        private static final int bufferMemSize = 1073741824;
        
        static private volatile  ConcurrentHashMap<String, String> docAddHSResponseQueue =  new ConcurrentHashMap<String, String>();
        
        static final private String self = HeartbeatManager.getInstance().getNodeId();
        
        static final private DatabaseStorage dbAct = DatabaseStorage.getInstance();
        
        static final int RESPONSE_WAIT_MAX_RETRY = 5;
        
        @Override
        public Response process(Request request) {

                int opChoice = 0;
                
                System.gc();

                Response docOpResponse = null;

                Header docOpHeader = request.getHeader();

                Payload docOpBody =  request.getBody();

                opChoice = docOpHeader.getRoutingId().getNumber();

                switch(opChoice){

                case 24:
                        docOpResponse = docAddValidate(docOpHeader , docOpBody);
                        break;

                case 20:
                        docOpResponse = docAdd(docOpHeader , docOpBody);
                        break;

                case 21:
                        docOpResponse = docFind(docOpHeader, docOpBody);
                        break;

                case 22:
                        docOpResponse = docUpdate(docOpHeader, docOpBody);
                        break;

                case 23:
                        docOpResponse = docRemove(docOpHeader, docOpBody);
                        break;

                case 25:
                        docOpResponse = docQuery(docOpHeader, docOpBody);
                        break;
                
                default:
                        System.out.println("DpcumentResource: No matching doc op id found");


                }

                return docOpResponse;
        }

        private Response docAddValidate(Header docAddValidateHeader , Payload docAddValidateBody){

                Document repDoc = docAddValidateBody.getDoc();

                long reqFileSize = repDoc.getDocSize();

                String newFileName = repDoc.getDocName();

                String nameSpace = EMPTY_STRING;
                
                String originator = docAddValidateHeader.getOriginator();
                
                logger.info("Received docAddValidate( docAddHS) from "+originator);

                Response.Builder docAddValidateResponseBuilder = Response.newBuilder();

                if(docAddValidateBody.getSpace() !=null && docAddValidateBody.getSpace().getName().length() > 0){

                        nameSpace = docAddValidateBody.getSpace().getName();
                        
                        docAddValidateResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(repDoc).addSpaces(docAddValidateBody.getSpace()));

                }else{
                        docAddValidateResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(repDoc));
                }
                
                long spaceAvailable = 0;

                long  bufferredLimit = 0;

                if((newFileName == null || newFileName.length() ==0) || reqFileSize ==0){

                        docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQMISSINGPARAMMSG).toBuilder().setOriginator(self));

                        return docAddValidateResponseBuilder.build();
                }

                if(nameSpace != null && nameSpace.length() > 0){

                        String effNS = HOMEDIR+File.separator+nameSpace; 

                        String effVisitorNS = VISITORDIR+File.separator+nameSpace;

                        File targetNS = new File (effNS);

                        File targetVisitorNS =  new File(effVisitorNS);

                        try {

                                boolean nsCheck = FileUtils.directoryContains(homeDir, targetNS);

                                boolean nsAwayCheck = FileUtils.directoryContains(visitorDir, targetVisitorNS); // Commented temporarily to test docRemove and replica remove

                                //boolean nsAwayCheck = false;

                                if(nsCheck){

                                        File targetFileName = new File (effNS+File.separator+newFileName);

                                        boolean fileCheck = FileUtils.directoryContains(targetNS, targetFileName);

                                        if(fileCheck){

                                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(self));

                                                return docAddValidateResponseBuilder.build();

                                        }

                                }

                                if(nsAwayCheck){

                                        File targetFileName = new File (effVisitorNS+File.separator+newFileName);

                                        boolean fileCheck = FileUtils.directoryContains(targetVisitorNS, targetFileName);

                                        logger.info("Validating "+effVisitorNS+" for "+newFileName+" as "+fileCheck);

                                        if(fileCheck){

                                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(self));

                                                return docAddValidateResponseBuilder.build();

                                        }

                                }

                        } catch (IOException e) {

                                logger.error("Document Response: IO Exception while validating file add request "+e.getMessage());

                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return docAddValidateResponseBuilder.build();

                        }

                }else{

                        try {

                                boolean fileCheck = FileUtils.directoryContains(homeDir, new File(HOMEDIR+File.separator+newFileName));

                                boolean visitorFileCheck = FileUtils.directoryContains(visitorDir, new File(VISITORDIR+File.separator+newFileName));

                                logger.info("Validating "+VISITORDIR+" for "+newFileName+" as "+visitorFileCheck);

                                if(fileCheck){

                                        docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(self));

                                        return docAddValidateResponseBuilder.build();

                                }

                                if(visitorFileCheck){

                                        docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(self));

                                        return docAddValidateResponseBuilder.build();
                                }

                        } catch (IOException e) {

                                logger.error("Document Response: IO Exception while validating file add request "+e.getMessage());

                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return docAddValidateResponseBuilder.build();
                        }
                }


                if(!NodeResponseQueue.nodeExistCheck(originator)){
                
                NodeResponseQueue.broadcastDocQuery(nameSpace, newFileName , true);

                                        
                        boolean docQueryResult = NodeResponseQueue.fetchDocQueryResult(nameSpace , newFileName , true);

                        if(docQueryResult){
                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(self));

                                return docAddValidateResponseBuilder.build();
                        }
                }
                
                try {
                        spaceAvailable = FileSystemUtils.freeSpaceKb()*1024;

                        bufferredLimit = spaceAvailable - bufferMemSize; // commented temporarily for testing
                        
                        //bufferredLimit = 25;

                } catch (IOException e) {

                        logger.error("DpcumentResource:docAddValidate IOException while calculating free space "+e.getMessage());
                        docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
                        return docAddValidateResponseBuilder.build();
                }

                if(reqFileSize > bufferredLimit){
                        
                        if(!NodeResponseQueue.nodeExistCheck(originator)){
                                
                        NodeResponseQueue.multicastDocAddHandshake(nameSpace, newFileName , reqFileSize);
                        
                        boolean nodeFound = false;
                        
                        int attempt  = 0 ;
                        
                        do{
                                
                                logger.info(" DocAddHS: sleeping for 3000ms! witing for response for docAddHS");
                                
                                try {
                                        Thread.sleep(MAXWAITFORADDRESPONSE);
                                } catch (InterruptedException e) {
                                        e.printStackTrace(); 
                                }
                                
                                String nodeId = NodeResponseQueue.fetchDocAddHSResult(nameSpace, newFileName);
                                
                                if(!nodeId.equalsIgnoreCase("NA")){
                                        docAddHSResponseQueue.put(nameSpace+newFileName, nodeId);
                                        logger.info(" DocAddValidate: Received response for docAddValidate from "+nodeId+" as "+nodeId);
                                        nodeFound = true;
                                        break;
                                }else{
                                		attempt++;
                                        continue;
                                }			
                        }while (attempt < RESPONSE_WAIT_MAX_RETRY );
                        
                        if(nodeFound)
                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.SUCCESS, FILEUPLOADREQVALIDATEDMSG).toBuilder().setOriginator(self));
                        else
                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILETOOLARGETOSAVEMSG).toBuilder().setOriginator(self));        

                        }else{
                                
                                docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILETOOLARGETOSAVEMSG).toBuilder().setOriginator(self));
                        }
                
                }else{

                        docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.SUCCESS, FILEUPLOADREQVALIDATEDMSG).toBuilder().setOriginator(self));
                }

                return docAddValidateResponseBuilder.build();
        }


        private Response docAdd(Header docAddHeader , Payload docAddBody){

                String nameSpace = EMPTY_STRING; //docAddBody.getSpace().getName();

                String effNS = EMPTY_STRING; 
                
                String originator = docAddHeader.getOriginator();

                if(docAddBody.getSpace() !=null && docAddBody.getSpace().getName().length() > 0){

                        nameSpace = docAddBody.getSpace().getName();
                        
                        if(nameSpace.startsWith(File.separator))
                                effNS = HOMEDIR+nameSpace;
                        else
                        effNS = HOMEDIR+File.separator+nameSpace;
                        
                }else
                        effNS = HOMEDIR;

                String fileName = docAddBody.getDoc().getDocName();

                logger.info("DocAdd: Received file "+fileName+" from "+originator);
                
                File nameDir = new File(effNS);

                File file = new File(effNS+File.separator+fileName);

                Header.Builder docAddHeaderBuilder = Header.newBuilder(docAddHeader);

                Document receivedFile = docAddBody.getDoc();

                Document toBesent= null;
                
                String key = nameSpace+fileName;
                
                if(docAddHSResponseQueue.containsKey(key)){  // forwarding data received to eligible node
                        
                        String nodeId = docAddHSResponseQueue.get(key);
                                                
                        NodeResponseQueue.forwardDocADDRequest(nodeId , docAddHeader , docAddBody );
                        
                        logger.info("DocAdd: forwarding docAdd to "+nodeId+" due to insufficient space");
                        
                        int attempt = 0;
                        
                        boolean docAddResponse = false;
                        
                        do{
                        
                        logger.info(" DocAdd: Sleeping for 3000ms! waiting to receive response from "+nodeId+" for "+nameSpace+File.separator+fileName);
                        try {
                        
                                Thread.sleep(MAXWAITFORADDRESPONSE);
                
                        } catch (InterruptedException e) {
                                
                                e.printStackTrace();
                        }
                        
                        logger.info(" DocAdd: checking the response from "+nodeId+" for "+nameSpace+""+fileName);
                        
                        docAddResponse = NodeResponseQueue.fetchDocAddResult(nodeId, nameSpace, fileName);
                        
                        logger.info(" DocAdd: Response from "+nodeId+" for "+nameSpace+File.separator+fileName+" is "+docAddResponse);
                        
                        if(docAddResponse)
                                break;
                        else{
                        		attempt++;
                                continue;
                        }
                }while(attempt < RESPONSE_WAIT_MAX_RETRY);
                        
                        if(docAddResponse){
                                docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);
                                docAddHeaderBuilder.setReplyMsg("File(chunk) Uploaded Successfully");
                                
                                //if(receivedFile.getChunkId() == receivedFile.getTotalChunk())
                                                //dbAct.addDocumentInDatabase(nameSpace, fileName);
                                
                        }
                        else{
                                docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);
                                docAddHeaderBuilder.setReplyMsg("File could not be  Uploaded due to internal issue");
                        }
                        
                        
                }else{        // Storing contents locally

                try {

                        if(!nameDir.exists()){

                                //logger.info("Creating directory with name "+nameSpace );

                                FileUtils.forceMkdir(nameDir);

                        }

                        logger.info("DocAdd: Creating file with name "+effNS+File.separator+fileName+" and writing the content sent by client to it" );

                        FileUtils.writeByteArrayToFile(file, receivedFile.getChunkContent().toByteArray(), true);

                        docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);
                        
                        if(receivedFile.getChunkId() == receivedFile.getTotalChunk()){
                         
                                if(effNS.endsWith(File.separator))
                                        dbAct.addDocumentInDatabase(effNS, fileName);
                                else
                                        dbAct.addDocumentInDatabase(effNS+File.separator, fileName);
                        }
                         
                        docAddHeaderBuilder.setReplyMsg("File (Chunk) Uploaded Successfully");
                        
                        

                } catch (IOException e) {

                        logger.info("Exception while creating the file and/or writing the content to it "+e.getMessage());

                        docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);

                        docAddHeaderBuilder.setReplyMsg("Server Exception while uploading a file");

                        e.printStackTrace();
                }

        }        // request-forwarding if-else ends
                ///////
                
                System.gc();
                
                toBesent = receivedFile.toBuilder().clearChunkContent().build();

                Response.Builder docAddRespBuilder = Response.newBuilder();

                docAddRespBuilder.setHeader(docAddHeaderBuilder);

                System.gc();

                if(nameSpace != null && nameSpace.length() > 0)
                        docAddRespBuilder.setBody(PayloadReply.newBuilder().addDocs(toBesent).addSpaces(docAddBody.getSpace()));
                else
                        docAddRespBuilder.setBody(PayloadReply.newBuilder().addDocs(toBesent));

                System.gc();

                return docAddRespBuilder.build();
        }

        private Response docFind(Header docFindHeader , Payload docFindBody){

                // implemented in chunkeddocument resource

                return null;
        }

        private Response docUpdate(Header docUpdateHeader , Payload docUpdateBody){

                return null;
        }

        private Response docRemove(Header docRemoveHeader , Payload docRemoveBody){

                String fileToBeDeleted = docRemoveBody.getDoc().getDocName();

                String nameSpece = EMPTY_STRING; 

                Response.Builder fileRemoveResponseBuilder = Response.newBuilder();

                if(docRemoveBody.getSpace() !=null && docRemoveBody.getSpace().getName().length() >0 ){
                        nameSpece =        docRemoveBody.getSpace().getName();
                        logger.info(" DocRemove: nameSpace received "+nameSpece);
                        fileRemoveResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(docRemoveBody.getDoc()).addSpaces(docRemoveBody.getSpace()));
                }else{
                        fileRemoveResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(docRemoveBody.getDoc()));
                }

                String originator = docRemoveHeader.getOriginator();

                logger.info("docRemove Client data file to be delted: "+fileToBeDeleted+" namespace: "+nameSpece);

                File targetFile = null;
                if(fileToBeDeleted == null || fileToBeDeleted.length() ==0){

                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEREQMISSINGPARAMMSG).toBuilder().setOriginator(self));

                        return fileRemoveResponseBuilder.build();
                }

                if(nameSpece != null && nameSpece.length() > 0){

                        String effNS = HOMEDIR+File.separator+nameSpece;

                        File targetNS = new File (effNS);

                        targetFile = new File(effNS+File.separator+fileToBeDeleted);

                        try {

                                boolean nsCheck = FileUtils.directoryContains(homeDir, targetNS);

                                if(nsCheck){

                                        boolean fileCheck = FileUtils.directoryContains(targetNS, targetFile);

                                        if(fileCheck){

                                                if(targetFile.isDirectory()){

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, OPERATIONNOTALLOWEDMSG+"Supplied file is directory").toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();
                                                }

                                                logger.info(" docRemove: Requested file found: Forwarding requets to other nodes to delete replicas");
                                                String response = NodeResponseQueue.multicastReplicaRemoveQuery(HOMEDIR+File.separator+nameSpece+File.separator, fileToBeDeleted);
                                                
                                                boolean replicasDeleted = false;
                                                
                                                if(response.equals("DNE")){
                                                        
                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));
                                                        return fileRemoveResponseBuilder.build();

                                                }else if (response.equals("NR"))
                                                         replicasDeleted = true;
                                                else{
                                                int attempt = 0;                        
                                                                                                
                                                do{
                                                
                                                logger.info(" docRemove: Sleeping for 3000ms... Witing for responses from other nodes regarding replica removal");
                                                Thread.sleep(MAXWAITFORRESPONSE);
                                                replicasDeleted = NodeResponseQueue.fetchReplicaRemoveResult(nameSpece, fileToBeDeleted);
                                                
                                                if(replicasDeleted)
                                                        break;
                                                else{
                                                        attempt++;
                                                        continue;
                                                }
                                                
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);
                                                
                                                }

                                                if(replicasDeleted){

                                                        FileUtils.forceDelete(targetFile);
                                                        
                                                        logger.info("DocRemove: Deleteing document from DB "+effNS+File.separator+fileToBeDeleted);
                                                        
                                                        dbAct.deleteDocumentInDatabase(effNS+File.separator, fileToBeDeleted);

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG).toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();

                                                }else{

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();

                                                }

                                        }else{

                                                if(!HeartbeatManager.getInstance().checkNearest(originator)){

                                                        logger.info(" docRemove: Requested file not available locally: Forwarding requets to other nodes");
                                                        
                                                        boolean docRemoveBCastResult = false;
                                                        
                                                        NodeResponseQueue.broadcastDocRemoveQuery(nameSpece, fileToBeDeleted);
                                                        
                                                        int attempt = 0;

                                                        do{
                                                        
                                                        logger.info(" docRemove: Sleeping for 3000ms...Witing for responses from other nodes");

                                                        Thread.sleep(MAXWAITFORRESPONSE);

                                                        String docRemoveBroadcastResult = NodeResponseQueue.fetchDocRemoveResult(nameSpece, fileToBeDeleted);
                                                        
                                                        if(docRemoveBroadcastResult.equalsIgnoreCase("Success")){
                                                                docRemoveBCastResult = true;
                                                                break;
                                                        }else if(docRemoveBroadcastResult.equalsIgnoreCase("NA")){
                                                                logger.warn("No response from all the network nodes for document remove of "+nameSpece+"/"+fileToBeDeleted);
                                                                attempt++;
                                                                continue;
                                                        }
                                                        
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);

                                                        if(docRemoveBCastResult){

                                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG).toBuilder().setOriginator(self));

                                                                return fileRemoveResponseBuilder.build();

                                                        }else{

                                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));

                                                                return fileRemoveResponseBuilder.build();

                                                        }

                                                }else{

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEINEXISTENTMSG).toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();
                                                }
                                        }

                                }else{

                                        if(!HeartbeatManager.getInstance().checkNearest(originator)){
                                                
                                                logger.info(" docRemove: Requested namespace not available locally: Forwarding requets to other nodes");
                                                NodeResponseQueue.broadcastDocRemoveQuery(nameSpece, fileToBeDeleted);
                                                
                                                boolean docRemoveBCastResult = false;
                                                
                                                int attempt = 0;

                                                do{
                                                
                                                logger.info(" docRemove: Sleeping for 3000ms...Witing for responses from other nodes");

                                                Thread.sleep(MAXWAITFORRESPONSE);

                                                String docRemoveBroadcastResult = NodeResponseQueue.fetchDocRemoveResult(nameSpece, fileToBeDeleted);
                                                
                                                if(docRemoveBroadcastResult.equalsIgnoreCase("Success")){
                                                        docRemoveBCastResult = true;
                                                        break;
                                                }else if(docRemoveBroadcastResult.equalsIgnoreCase("NA")){
                                                        logger.warn("No response from all the network nodes for document remove of "+nameSpece+"/"+fileToBeDeleted);
                                                        attempt++;
                                                        continue;
                                                }
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);                                                

                                                if(docRemoveBCastResult)
                                                {
                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG).toBuilder().setOriginator(self));
                                                        return fileRemoveResponseBuilder.build();
                                                }else{

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));
                                                        return fileRemoveResponseBuilder.build();
                                                }
                                        }else{

                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, NAMESPACEINEXISTENTMSG).toBuilder().setOriginator(self));
                                                return fileRemoveResponseBuilder.build();
                                        }
                                }
                        } catch (IOException e) {

                                logger.error("Document Response: IO Exception while processing file delete request "+e.getMessage());

                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return fileRemoveResponseBuilder.build();

                        } catch (InterruptedException e) {

                                logger.info(" DocRemove: encountered interrupted exception ");

                                e.printStackTrace();

                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return fileRemoveResponseBuilder.build();
                        }

                } else{

                        try {

                                targetFile = new File(HOMEDIR+File.separator+fileToBeDeleted);

                                boolean fileCheck = FileUtils.directoryContains(homeDir, targetFile);

                                if(fileCheck){

                                        if(targetFile.isDirectory()){

                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, OPERATIONNOTALLOWEDMSG+"Requested file is directory").toBuilder().setOriginator(self));

                                                return fileRemoveResponseBuilder.build();
                                        }

                                        logger.info(" docRemove: Requested file found: Forwarding requets to other nodes to delete replicas");
                                        String response = NodeResponseQueue.multicastReplicaRemoveQuery(HOMEDIR+File.separator, fileToBeDeleted);
                                        
                                        boolean replicasDeleted = false;
                                        
                                        if(response.equals("DNE")){
                                                
                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));
                                                return fileRemoveResponseBuilder.build();

                                        }else if (response.equals("NR"))
                                                 replicasDeleted = true;
                                        else{
                                                                                
                                                int attempt = 0;                        
                                                                                
                                                do{
                                                
                                                logger.info(" docRemove: Sleeping for 3000ms... Witing for responses from other nodes regarding replica removal");
                                                Thread.sleep(MAXWAITFORRESPONSE);
                                                replicasDeleted = NodeResponseQueue.fetchReplicaRemoveResult(nameSpece, fileToBeDeleted);
                                                
                                                if(replicasDeleted)
                                                        break;
                                                else{
                                                        attempt++;
                                                        continue;
                                                }
                                                
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);
                                        }

                                        if(replicasDeleted){

                                                FileUtils.forceDelete(targetFile);
                                                
                                                dbAct.deleteDocumentInDatabase(HOMEDIR+File.separator, fileToBeDeleted);

                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG).toBuilder().setOriginator(self));

                                                return fileRemoveResponseBuilder.build();

                                        }else{

                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));

                                                return fileRemoveResponseBuilder.build();
                                        }

                                }else{

                                        if(!HeartbeatManager.getInstance().checkNearest(originator)){

                                                logger.info(" docRemove: Requested file not available locally: Forwarding requets to other nodes");

                                                NodeResponseQueue.broadcastDocRemoveQuery(nameSpece, fileToBeDeleted);

                                                boolean docRemoveBCastResult = false;
                                                
                                                int attempt = 0;

                                                do{
                                                
                                                logger.info(" docRemove: Sleeping for 3000ms...Witing for responses from other nodes");

                                                Thread.sleep(MAXWAITFORRESPONSE);

                                                String docRemoveBroadcastResult = NodeResponseQueue.fetchDocRemoveResult(nameSpece, fileToBeDeleted);
                                                
                                                if(docRemoveBroadcastResult.equalsIgnoreCase("Success")){
                                                        docRemoveBCastResult = true;
                                                        break;
                                                }else if(docRemoveBroadcastResult.equalsIgnoreCase("NA")){
                                                        logger.warn("No response from all the network nodes for document remove of "+nameSpece+"/"+fileToBeDeleted);
                                                        attempt++;
                                                        continue;
                                                }
                                                }while(attempt < RESPONSE_WAIT_MAX_RETRY);        

                                                if(docRemoveBCastResult){

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG).toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();

                                                }else{

                                                        fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEDELETEUNSUCCESSFULMSG).toBuilder().setOriginator(self));

                                                        return fileRemoveResponseBuilder.build();

                                                }


                                        }else{

                                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEINEXISTENTMSG).toBuilder().setOriginator(self));

                                                return fileRemoveResponseBuilder.build();
                                        }

                                }

                        } catch (IOException e) {

                                logger.error("Document Response: IO Exception while processing file delete request w/o namespace "+e.getMessage());

                                e.printStackTrace();

                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return fileRemoveResponseBuilder.build();

                        } catch (InterruptedException e) {

                                logger.info(" DocRemove: encountered interrupted exception ");

                                e.printStackTrace();

                                fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));

                                return fileRemoveResponseBuilder.build();
                        }

                }
        }

        private Response docQuery(Header docQueryHeader , Payload docQueryBody){

                String originator = docQueryHeader.getOriginator();

                logger.info(" Received doc query request from "+originator);

                Response.Builder docQueryResponseBuilder = Response.newBuilder();

                Document queryDoc = docQueryBody.getDoc();

                NameSpace space = docQueryBody.getSpace();

                String fileName = queryDoc.getDocName();

                String nameSpace =  EMPTY_STRING;

                String effHomeNS = HOMEDIR;

                String effAwayNS = VISITORDIR;

                if(space !=null && space.getName().length() >0){
                        nameSpace  = space.getName();
                        docQueryResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(queryDoc).addSpaces(space));
                }else{
                        docQueryResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(queryDoc));
                }

                if(nameSpace !=null && nameSpace.length() >0){
                        effHomeNS= effHomeNS+File.separator+nameSpace;
                        effAwayNS = effAwayNS+File.separator+nameSpace;
                }

                File targetFile = null;

                File parentHomeDir = new File(effHomeNS);

                File parentAwayDir = new File(effAwayNS);

                boolean fileHome = false;

                boolean fileAway = false;

                try {

                        if(parentHomeDir.exists()){
                                targetFile = new File(effHomeNS+File.separator+fileName);
                                fileHome =        FileUtils.directoryContains(parentHomeDir, targetFile);
                                logger.info(" DocQuery: validating "+effHomeNS+" for "+fileName+" as "+fileHome);
                        }

                        if(!fileHome){
                                if(parentAwayDir.exists()){

                                        targetFile = new File(effAwayNS+File.separator+fileName);
                                        fileAway =        FileUtils.directoryContains(parentAwayDir, targetFile);
                                        logger.info(" DocQuery: validating "+effAwayNS+" for "+fileName+" as "+fileAway);
                                }
                        }

                } catch (IOException e) {

                        logger.error("DocQuery: IOException while validating file existence");
                        e.printStackTrace();
                        docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
                        return docQueryResponseBuilder.build();
                }catch (Exception e) {

                        logger.error("DocQuery: Exception while validating file existence "+e.getMessage());
                        e.printStackTrace();
                        docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
                        return docQueryResponseBuilder.build();
                }


                if(fileHome || fileAway)
                        docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.SUCCESS, REQUESTEDFILEEXISTSMSG+" "+nameSpace+File.separator+fileName).toBuilder().setOriginator(self));
                else{

                        if(!NodeResponseQueue.nodeExistCheck(originator)){

                                logger.info(" DocQuery: Requested file "+fileName+" does not exist locally...Broadcasting DOCQuery to active nodes ");

                                NodeResponseQueue.broadcastDocQuery(nameSpace, fileName , true);

                                try {
                                        boolean docQueryResult = false;
                                        int attempt = 0;

                                        do{
                                        
                                        logger.info(" DocQuery: sleeping for 3000ms! Waiting for responses from the other nodes for DOCQUERY ");

                                        Thread.sleep(MAXWAITFORRESPONSE);

                                        docQueryResult  = NodeResponseQueue.fetchDocQueryResult(nameSpace , fileName , true);

                                        if(docQueryResult){

                                                docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.SUCCESS, REQUESTEDFILEEXISTSMSG+" "+nameSpace+File.separator+fileName).toBuilder().setOriginator(self));
                                                break;

                                        }else{

                                                docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.FAILURE, REQUESTEDFILEDNEXISTSMSG).toBuilder().setOriginator(self));
                                                attempt++;
                                                continue;
                                        }
                                
                                        }while(attempt < 2);

                                } catch (InterruptedException e1) {

                                        e1.printStackTrace();
                                }

                        }else{

                                docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.FAILURE, REQUESTEDFILEDNEXISTSMSG).toBuilder().setOriginator(self));
                        }

                }

                return docQueryResponseBuilder.build();
        }
}