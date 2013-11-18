package poke.resources;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.Header.ReplyStatus;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import poke.server.management.HeartbeatManager;
import poke.server.nconnect.NodeResponseQueue;
import poke.server.resources.ChunkedResource;
import poke.server.resources.ResourceUtil;


public class DocumentChunkResource implements ChunkedResource {
        
         protected static Logger logger = LoggerFactory.getLogger("DocumentChunkResource");

     private static final String HOMEDIR = "home";

     private static final String VISITORDIR = "away";
     
     private static final String DOCFOUNDSUCCESS = "Requested document was found successfully";

     private static final File homeDir = new File(HOMEDIR);
     
     private static final File visitorDir = new File(VISITORDIR);     

         private static final int MAX_UNCHUNKED_FILE_SIZE = 26214400;

        private static final long MAXWAITFORRESPONSE = 8000;

        @Override
        public List<Response> process(Request request) {

                List<Response> responses = new ArrayList<Response>();
                int opChoice = 0;
                Header docOpHeader = request.getHeader();
                Payload docOpBody = request.getBody();
                opChoice = docOpHeader.getRoutingId().getNumber();

                switch (opChoice) {
                
                 case 21:
                                 responses = docFind(docOpHeader, docOpBody); 
                                 break;
             
                        default:
                                System.out
                                                .println("DocumentChunkResource: No matching doc op id found");

                }

                return responses;
        }

        private List<Response> docFind(Header docFindHeader, Payload docFindBody){
                
                List<Response> responses = new ArrayList<Response>();
                String fileName = HOMEDIR + File.separator
                                + docFindBody.getSpace().getName() + File.separator
                                + docFindBody.getDoc().getDocName();
                String fileNameAway = VISITORDIR + File.separator
                                + docFindBody.getSpace().getName() + File.separator
                                + docFindBody.getDoc().getDocName();
                boolean fileExists = false;
                boolean awayExists= false;
                Response.Builder docFindResponse = Response.newBuilder();
                PayloadReply.Builder docFindRespPayload = PayloadReply.newBuilder();
                Header.Builder docFindRespHeader = Header.newBuilder();
                String nameSpace = docFindBody.getSpace().getName();
                String self = HeartbeatManager.getInstance().getNodeId();
                docFindRespPayload.addDocsBuilder();
                docFindRespPayload.addSpacesBuilder();
                
                docFindRespPayload.setDocs(0, Document.newBuilder().setDocName(fileName));
                //if(nameSpace == null)
                docFindRespPayload.setSpaces(0, NameSpace.newBuilder().setName(nameSpace));
                
                try {
                        fileExists = FileUtils.directoryContains(homeDir,
                                        new File(fileName));
                        awayExists = FileUtils.directoryContains(visitorDir, new File(fileNameAway));
                        if(awayExists){
                                fileName = fileNameAway;
                        }
                        if (fileExists || awayExists) {
                                logger.info("Document found at " + HeartbeatManager.getInstance().getNodeId());
                                responses = docFindClient(docFindHeader, responses, fileName,
                                                docFindResponse, docFindRespPayload, docFindRespHeader,
                                                nameSpace);

                        } else if(!fileExists && docFindHeader.getOriginator().contains("Client")){
                                logger.info("Document not found, broadcasting request to all nodes.");
                                NodeResponseQueue.broadcastDocFind(nameSpace, docFindBody.getDoc().getDocName(), true);
                
                try {

                                        logger.info(" DocFind: sleeping. Waiting for responses from the other nodes for DOCFIND ");

                                        //Thread.sleep(MAXWAITFORRESPONSE);
                                        String docFindResult = null;
                                        
                                        do{
                                                docFindResult = NodeResponseQueue.fetchDocFindResult(nameSpace , docFindBody.getDoc().getDocName(),true);
                                                Thread.sleep(2000);
                                        }while(docFindResult == null);

                                        if(docFindResult=="Success"){
                                                logger.info("Document found at external node..");
                                                String tempfname = "temp" + File.separator
                                                + docFindBody.getSpace().getName() + File.separator
                                                + docFindBody.getDoc().getDocName();
                                                responses = docFindClient(docFindHeader, responses, tempfname,
                                                                docFindResponse, docFindRespPayload, docFindRespHeader,
                                                                nameSpace);
                                                //docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.SUCCESS, "Document created in temp").toBuilder().setOriginator(self));

                                        }else{
                                        	logger.info("Document not found at node " + HeartbeatManager.getInstance().getNodeId());
                                            logger.info("Docuemnt not found in internal network");
                                            logger.info("Broadcasting docQuery to external network");
                                            NodeResponseQueue.broadcastDocQuery(nameSpace, fileName, false);
                                            logger.info("Sleeping for configured time!!! Waiting for response from External Node for DOCQUERY");
                                            try {
            									Thread.sleep(MAXWAITFORRESPONSE);
            								} catch (InterruptedException e) {
            									// TODO Auto-generated catch block
            									e.printStackTrace();
            								}
                                            
                                            boolean docExistsAtExternal = NodeResponseQueue.fetchDocQueryResult(nameSpace, fileName, false);
                                            if(docExistsAtExternal){
                                            	logger.info("Document exists at external network...broadcasting docFind to external network");
                                            	 NodeResponseQueue.broadcastDocFind(nameSpace, fileName, false);
                                            	 String externalDocFindResponse = NodeResponseQueue.fetchDocFindResult(nameSpace, fileName, false);
                                            	 if(externalDocFindResponse=="Success"){
                                                     logger.info("Document found at external node..");
                                                     String tempfname = "temp" + File.separator
                                                     + docFindBody.getSpace().getName() + File.separator
                                                     + docFindBody.getDoc().getDocName();
                                                     responses = docFindClient(docFindHeader, responses, tempfname,
                                                                     docFindResponse, docFindRespPayload, docFindRespHeader,
                                                                     nameSpace);
                                                     //docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.SUCCESS, "Document created in temp").toBuilder().setOriginator(self));

                                             }else{

                                                     docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.FAILURE, "Document not found").toBuilder().setOriginator(self));
                                                      docFindResponse.setBody(docFindRespPayload.build());
                                           
                                          responses.add(docFindResponse.build());
                                             }
                                            	 
                                            }
                                            docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

                                docFindRespHeader.setReplyMsg("Server could not find the file.");
                                docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.FAILURE, "Document not Found").toBuilder().setOriginator(self));
                                //docFindRespPayload.addSpacesBuilder();
                                docFindResponse.setBody(docFindRespPayload.build());
                                responses.add(docFindResponse.build());
                                        		
                                                /*docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.FAILURE, "Document not found").toBuilder().setOriginator(self));
                                                 docFindResponse.setBody(docFindRespPayload.build());
                                      
                                     responses.add(docFindResponse.build());*/
                                        }

                                } catch (InterruptedException e1) {

                                        e1.printStackTrace();
                                }
                        } else if(!fileExists && !awayExists && !docFindHeader.getOriginator().contains("Client")){
                                logger.info("Document not found at node " + HeartbeatManager.getInstance().getNodeId());
                                logger.info("Docuemnt not found in internal network");
                                logger.info("Broadcasting docQuery to external network");
                                NodeResponseQueue.broadcastDocQuery(nameSpace, fileName, false);
                                logger.info("Sleeping for configured time!!! Waiting for response from External Node for DOCQUERY");
                                try {
									Thread.sleep(MAXWAITFORRESPONSE);
								} catch (InterruptedException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
                                
                                boolean docExistsAtExternal = NodeResponseQueue.fetchDocQueryResult(nameSpace, fileName, false);
                                if(docExistsAtExternal){
                                	logger.info("Document exists at external network...broadcasting docFind to external network");
                                	 NodeResponseQueue.broadcastDocFind(nameSpace, fileName, false);
                                	 String externalDocFindResponse = NodeResponseQueue.fetchDocFindResult(nameSpace, fileName, false);
                                	 if(externalDocFindResponse=="Success"){
                                         logger.info("Document found at external node..");
                                         String tempfname = "temp" + File.separator
                                         + docFindBody.getSpace().getName() + File.separator
                                         + docFindBody.getDoc().getDocName();
                                         responses = docFindClient(docFindHeader, responses, tempfname,
                                                         docFindResponse, docFindRespPayload, docFindRespHeader,
                                                         nameSpace);
                                         //docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.SUCCESS, "Document created in temp").toBuilder().setOriginator(self));

                                 }else{

                                         docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.FAILURE, "Document not found").toBuilder().setOriginator(self));
                                          docFindResponse.setBody(docFindRespPayload.build());
                               
                              responses.add(docFindResponse.build());
                                 }
                                	 
                                }
                                docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

                    docFindRespHeader.setReplyMsg("Server could not find the file.");
                    docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.FAILURE, "Document not Found").toBuilder().setOriginator(self));
                    //docFindRespPayload.addSpacesBuilder();
                    docFindResponse.setBody(docFindRespPayload.build());
                    responses.add(docFindResponse.build());
                        }
                        
                } catch (IOException e) {
                          logger.info("IO Exception while creating the file and/or writing the content to it "+e.getMessage());

              docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

              docFindRespHeader.setReplyMsg("Server Exception while downloading the requested file.");
              docFindResponse.setHeader(docFindRespHeader.build());
              docFindResponse.setBody(docFindRespPayload.build());
              responses.add(docFindResponse.build());

              e.printStackTrace();
                }

                return responses;
        }

        private List<Response> docFindClient(Header docFindHeader, List<Response> responses,
                        String fileName, Response.Builder docFindResponse,
                        PayloadReply.Builder docFindRespPayload,
                        Header.Builder docFindRespHeader, String nameSpace) {
                
                String fileExt = FilenameUtils.getExtension(fileName);

                java.io.File file = FileUtils.getFile(fileName);

                long fileSize = FileUtils.sizeOf(file);
                
                logger.info("Writing the document to the temp folder.");

                logger.info("Size of the file to be sent " + fileSize);
                String self = HeartbeatManager.getInstance().getNodeId();

                long totalChunk = ((fileSize / MAX_UNCHUNKED_FILE_SIZE)) + 1;

                if (fileSize < MAX_UNCHUNKED_FILE_SIZE) {

                        logger.info(" DocFind: Sending the complete file in unchunked mode");

                        logger.info("Total number of chunks " + totalChunk);

                        byte[] fileContents = null;

                        try {

                                fileContents = FileUtils.readFileToByteArray(file);

                        } catch (IOException e) {

                                logger.error("Error while reading the specified file "
                                                + e.getMessage());
                                docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

                                docFindRespHeader
                                                .setReplyMsg("Server Exception while downloading the file found.");
                                docFindResponse.setBody(docFindRespPayload.build());
                                docFindResponse.setHeader(docFindHeader);
                                responses.add(docFindResponse.build());
                                return responses;
                        }
                        
                        docFindRespPayload.addDocsBuilder();

                        docFindRespPayload.setDocs(0, Document.newBuilder().setDocName(fileName)
                        .setDocExtension(fileExt)
                        .setChunkContent(ByteString.copyFrom(fileContents))
                        .setDocSize(fileSize).setTotalChunk(totalChunk)
                        .setChunkId(1));

                        docFindResponse.setBody(docFindRespPayload.build());
                        
                        docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.SUCCESS, DOCFOUNDSUCCESS).toBuilder().setOriginator(self));
                        responses.add(docFindResponse.build());
                        return responses;

                } else {

                        logger.info(" DocFind: Uploading the file in chunked mode");

                        logger.info("Total number of chunks " + totalChunk);

                        try {

                                int bytesRead = 0;

                                int chunkId = 1;

                                FileInputStream chunkeFIS = new FileInputStream(file);

                                do {

                                        byte[] chunckContents = new byte[26214400];
                                     
                                        logger.info("Contents of the chunk "+chunkId+" : "+chunckContents);
                                        
                                        docFindRespPayload.addDocsBuilder();

                                        docFindRespPayload.setDocs(0,Document
                                                        .newBuilder()
                                                        .setDocName(fileName)
                                                        .setDocExtension(fileExt)
                                                        .setDocSize(fileSize).setTotalChunk(totalChunk)
                                                        .setChunkId(chunkId));

                                        docFindResponse.setBody(docFindRespPayload.build());

                                        docFindResponse.setHeader(ResourceUtil.buildHeaderFrom(docFindHeader, ReplyStatus.SUCCESS, DOCFOUNDSUCCESS).toBuilder().setOriginator(self));

                                        //chunckContents = null;
                                        responses.add(docFindResponse.build());
                                        System.gc();

                                        chunkId++;

                                } while (chunkId <= totalChunk);

                                logger.info("Out of chunked write while loop");

                        } catch (FileNotFoundException e) {

                                logger.info("IO Exception while creating the file and/or writing the content to it "+e.getMessage());
                                
                                  docFindResponse.setBody(docFindRespPayload.build());
                          docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

                          docFindRespHeader.setReplyMsg("Server Exception while downloading the requested file.");
                          
                          responses.add(docFindResponse.build());

                          e.printStackTrace();

                        } catch (IOException e) {

                                logger.info("IO Exception while creating the file and/or writing the content to it "+e.getMessage());
                                
                                docFindResponse.setBody(docFindRespPayload.build());

                          docFindRespHeader.setReplyCode(Header.ReplyStatus.FAILURE);

                          docFindRespHeader.setReplyMsg("Server Exception while downloading the requested file.");
                          
                          responses.add(docFindResponse.build());

                          e.printStackTrace();
                        }
                        return responses;
                }
        }

}