package poke.server.replication;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;
import poke.server.management.HeartbeatManager;
import poke.server.nconnect.NodeResponseQueue;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import poke.server.storage.jdbc.DatabaseStorage;
/**
 * @author Kaustubh
 * @version 2.3
 * {@code : Handles all the replication related requests}
 * 
 */
public class ReplicationResource implements Resource {

	private static final String EMPTY_STRING = "";
	
	private static Logger logger = LoggerFactory.getLogger("ReplicationResource ");
	
	private static final String VISITORDIR = "away";
	
	static final private String self = HeartbeatManager.getInstance().getNodeId();

	private static final String REPLICAEXISTS = "Replica already exists. Checkout other another network node";

	private static final String REPLHSREQMISSINGPARAMMSG = "Insufficient parameters to validate the replication handshake";
	
	private static final long bufferMemSize = 4294967296L;

	private static final String INTERNALSERVERERRORMSG = "Request can not be fulfilled due to internal server error";
	
	private static final String FILETOOLARGETOSAVEMSG ="Can not upload the file: File is too large to save";

	private static final String REPLICAHSVALIDATED = "Replica can be uploaded: Handshake successful";
	
	private static final String REPLICAFOUND = "Replica for specified file exists";
	
	private static final String REPLICADNE = "Replica does not exists";
	
	private static final String REPLICADELETESUCCESSFULMSG = "Requested file replica has been deleted successfully";
	
	private static final String REPLICAINEXISTENTMSG = " Requested replica does not exist: Validate the replication module/ Supplied parameters";
	
	static final private DatabaseStorage dbAct = DatabaseStorage.getInstance();

	@Override
	public Response process(Request request) {
		
		int opChoice = 0;
		
		System.gc();

		Response repliOpResponse = null;

		Header repliOpHeader = request.getHeader();

		Payload  repliOpBody =  request.getBody();

		opChoice =  repliOpHeader.getRoutingId().getNumber();
		
		switch(opChoice){
		
		case 26:
			repliOpResponse = replicaRemove(repliOpHeader, repliOpBody);
			break;	

		case 27:
			repliOpResponse = replicaHandshake(repliOpHeader , repliOpBody);
			break;

		case 28:
			repliOpResponse = addReplica(repliOpHeader , repliOpBody);
			break;

		case 29:
			repliOpResponse = replicaQuery(repliOpHeader, repliOpBody);
			break;

		default:
			System.out.println("DpcumentResource: No matching doc op id found");


		}

		return repliOpResponse;
	}
	
	private Response replicaHandshake(Header repliOpHeader , Payload repliOpBody){
		
		Response.Builder replicaHSRespBuilder = Response.newBuilder();
		
		Document repDoc = repliOpBody.getDoc();

		long reqFileSize = repDoc.getDocSize();

		String fileName = repDoc.getDocName();
		
		long spaceAvailable = 0;
		long bufferredLimit = 0;
				
		if((fileName == null || fileName.length() ==0) || reqFileSize ==0){

			replicaHSRespBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, REPLHSREQMISSINGPARAMMSG).toBuilder().setOriginator(self));

			return replicaHSRespBuilder.build();
		}
		
		String nameSpace = EMPTY_STRING;
		String effNS = VISITORDIR;
		String originator = repliOpHeader.getOriginator();
		String effFileName = null;
		
		logger.info("Received replicahandshake from "+originator);

		if(repliOpBody.getSpace() !=null && repliOpBody.getSpace().getName().length() > 0){

			nameSpace = repliOpBody.getSpace().getName();
			effNS = effNS +File.separator+nameSpace;
			replicaHSRespBuilder.setBody(PayloadReply.newBuilder().addDocs(repDoc).addSpaces(repliOpBody.getSpace()));
			effFileName = effNS+fileName;
		}else{
			replicaHSRespBuilder.setBody(PayloadReply.newBuilder().addDocs(repDoc));
			effFileName = effNS+File.separator+fileName;
		}
		
		logger.info("replicaHandshake: effective namespace "+effNS);
		logger.info("replicaHandshake: effective fileName "+effFileName);
		
		File file = new File(effFileName);
		
		if(file.exists()){
			
			replicaHSRespBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, REPLICAEXISTS).toBuilder().setOriginator(self));
			return replicaHSRespBuilder.build();
		}
		
		try {
			spaceAvailable = FileSystemUtils.freeSpaceKb()*1024;
			bufferredLimit = spaceAvailable - bufferMemSize;
			
		} catch (IOException e) {

			logger.error(" replicaHandshake IOException while calculating free space "+e.getMessage());
			replicaHSRespBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
			return replicaHSRespBuilder.build();
		}

		if(reqFileSize > bufferredLimit){
			replicaHSRespBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, FILETOOLARGETOSAVEMSG).toBuilder().setOriginator(self));
			return replicaHSRespBuilder.build();
		}
		replicaHSRespBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.SUCCESS, REPLICAHSVALIDATED).toBuilder().setOriginator(self));
		return replicaHSRespBuilder.build();
	}
	
	private Response addReplica(Header repliOpHeader , Payload repliOpBody){
		
		Response.Builder addReplicaRespBuilder = Response.newBuilder();
		
		String nameSpace = EMPTY_STRING; //docAddBody.getSpace().getName();

		String effNS = EMPTY_STRING; 
		
		String originator = repliOpHeader.getOriginator();
		
		Header.Builder replicaAddHeaderBuilder = Header.newBuilder(repliOpHeader);

		if(repliOpBody.getSpace() !=null && repliOpBody.getSpace().getName().length() > 0){

			nameSpace = repliOpBody.getSpace().getName();
			
			if(nameSpace.startsWith(File.separator))
				effNS = VISITORDIR+nameSpace;
			else
			effNS = VISITORDIR+File.separator+nameSpace;
			
		}else
			effNS = VISITORDIR+File.separator;

		if(repliOpBody.getDoc() == null || repliOpBody.getDoc().getDocName().length() == 0 ){
			
			replicaAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);
			replicaAddHeaderBuilder.setReplyMsg("Server Exception while uploading a file");
			logger.error(" addReplica: "+originator+" did not send the document details");
			return addReplicaRespBuilder.setHeader(replicaAddHeaderBuilder).build();
		}
		
		String fileName = repliOpBody.getDoc().getDocName();

		logger.info("addReplica: Received file "+nameSpace+fileName+" from "+originator);
		
		File file = new File(effNS+fileName);
	
		Document receivedFile = repliOpBody.getDoc();

		Document toBesent= null;
		
		try {

			logger.info("addReplica: Creating file with name "+effNS+fileName+" and writing the content sent by "+originator+" to it" );

			FileUtils.writeByteArrayToFile(file, receivedFile.getChunkContent().toByteArray(), true);

			replicaAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);
			
			if(receivedFile.getChunkId() == receivedFile.getTotalChunk()){
			 
				if(effNS.endsWith(File.separator))
					dbAct.addReplicaInDatabase(effNS, fileName, originator, self);
				else
					dbAct.addReplicaInDatabase(effNS+File.separator, fileName, originator, self);
			}
			 
			replicaAddHeaderBuilder.setReplyMsg("File (Chunk) Uploaded Successfully");
			
		} catch (IOException e) {

			logger.info("Exception while creating the file and/or writing the content to it "+e.getMessage());

			replicaAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);

			replicaAddHeaderBuilder.setReplyMsg("Server Exception while uploading a file");

			e.printStackTrace();
		}

		System.gc();
		
		toBesent = receivedFile.toBuilder().clearChunkContent().build();

		addReplicaRespBuilder.setHeader(replicaAddHeaderBuilder);

		System.gc();

		if(nameSpace != null && nameSpace.length() > 0)
			addReplicaRespBuilder.setBody(PayloadReply.newBuilder().addDocs(toBesent).addSpaces(repliOpBody.getSpace()));
		else
			addReplicaRespBuilder.setBody(PayloadReply.newBuilder().addDocs(toBesent));

		System.gc();
	
		return addReplicaRespBuilder.build();
	}
	
	private Response replicaQuery(Header repliOpHeader , Payload repliOpBody){
		
		String originator = repliOpHeader.getOriginator();

		Response.Builder replicaQueryResponseBuilder = Response.newBuilder();

		Document queryDoc = repliOpBody.getDoc();

		NameSpace space = repliOpBody.getSpace();

		String fileName = queryDoc.getDocName();

		String nameSpace =  EMPTY_STRING;

		String effNS = VISITORDIR;

		if(space !=null && space.getName().length() >0){
			nameSpace  = space.getName();
			replicaQueryResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(queryDoc).addSpaces(space));
		}else{
			replicaQueryResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(queryDoc));
		}

		File file = null;
		
		if(nameSpace !=null && nameSpace.length() >0){
			effNS = effNS+File.separator+nameSpace;
			file = new File(effNS+fileName);
		}else{
			
			file = new File(effNS+File.separator+fileName);
		}
		
		logger.info(" Received replica query request from "+originator+" for "+effNS+" "+fileName);

		
		
		try {

			if(file.exists()){
				
				logger.info("replica found for "+effNS+" "+fileName);
				replicaQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.SUCCESS, REPLICAFOUND).toBuilder().setOriginator(self));
				return replicaQueryResponseBuilder.build();
			}else{
				
				logger.info("replica does not exists for "+effNS+" "+fileName);
				replicaQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, REPLICADNE).toBuilder().setOriginator(self));
				return replicaQueryResponseBuilder.build();
			}

		} catch (Exception e) {

			logger.error("ReplicaQuery: Exception while validating file existence "+e.getMessage());
			e.printStackTrace();
			replicaQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(repliOpHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
			return replicaQueryResponseBuilder.build();
		}
	}
	
	private Response replicaRemove(Header replicaRemoveHeader , Payload replicaRemoveBody){

		String nameSpace = EMPTY_STRING ;
		
		String effNS = VISITORDIR;
		
		String originator = replicaRemoveHeader.getOriginator();

		Response.Builder replicaRemoveResponseBuilder = Response.newBuilder();

		if(replicaRemoveBody.getSpace() !=null && replicaRemoveBody.getSpace().getName().length() > 0){
			nameSpace = replicaRemoveBody.getSpace().getName();
			replicaRemoveResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(replicaRemoveBody.getDoc()).addSpaces(replicaRemoveBody.getSpace()));
		}else{
			replicaRemoveResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(replicaRemoveBody.getDoc()));
		}

		String replicaName = replicaRemoveBody.getDoc().getDocName();

		File targetFile = null;

		try{

			if(nameSpace !=null && nameSpace.length() >0){
				effNS = effNS+File.separator+nameSpace;
				targetFile = new File(effNS+replicaName);
			}
			else{
				
				targetFile = new File(effNS+File.separator+replicaName);
			}

			logger.info("Received replica remove request for "+effNS+" "+replicaName+" from "+originator);
			
			if(targetFile.exists()){

					logger.info(" Replica found for "+effNS+replicaName+ " proceeding to delete it");
					dbAct.deleteDocumentInDatabase(effNS, replicaName);
					targetFile.delete();
					replicaRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(replicaRemoveHeader, ReplyStatus.SUCCESS, REPLICADELETESUCCESSFULMSG).toBuilder().setOriginator(self));
					return replicaRemoveResponseBuilder.build();

				}else{

					logger.info(" Replica not present for "+nameSpace+File.separator+replicaName+ " validate the replication module");
					replicaRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(replicaRemoveHeader, ReplyStatus.FAILURE, REPLICAINEXISTENTMSG).toBuilder().setOriginator(self));
					return replicaRemoveResponseBuilder.build();
				}

		}catch(Exception repRmExcep){

			logger.error("replicaRemove: Encountered general Exception "+repRmExcep.getMessage());
			repRmExcep.printStackTrace();
			replicaRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(replicaRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG).toBuilder().setOriginator(self));
			return replicaRemoveResponseBuilder.build();
		}
	}
}
