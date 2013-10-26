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
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.monitor.FileEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;

import poke.server.management.HeartbeatData;
import poke.server.management.HeartbeatManager;
import poke.server.nconnect.NodeClient;
import poke.server.nconnect.NodeResponseQueue;
import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;

public class DocumentResource implements Resource {

	protected static Logger logger = LoggerFactory.getLogger("DocumentResource");

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

	private static final String OPERATIONNOTALLOWEDMSG = "Requested Operation is not allowed with the 'request' type ";

	private static final File homeDir = new File(HOMEDIR);
	
	private static final File visitorDir = new File(VISITORDIR);

	@Override
	public Response process(Request request) {

		int opChoice = 0;

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

		String nameSpace = null;

		if(docAddValidateBody.getSpace() !=null){

			nameSpace = docAddValidateBody.getSpace().getName();
		}

		Response.Builder docAddValidateResponseBuilder = Response.newBuilder();

		docAddValidateResponseBuilder.setBody(PayloadReply.newBuilder().addDocs(repDoc).addSpaces(docAddValidateBody.getSpace()));

		long spacceAvailable = 0;

		long  bufferredLimit = 0;

		if((newFileName == null || newFileName.length() ==0) || reqFileSize ==0){

			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQMISSINGPARAMMSG));

			return docAddValidateResponseBuilder.build();
		}

		if(nameSpace != null && nameSpace.length() > 0){



			String effNS = HOMEDIR+File.separator+nameSpace; 
						
			String effVisitorNS = VISITORDIR+File.separator+nameSpace;
			
			logger.info("Validating "+effVisitorNS+" for "+newFileName);

			File targetNS = new File (effNS);
			
			File targetVisitorNS =  new File(effVisitorNS);
			
			try {

				boolean nsCheck = FileUtils.directoryContains(homeDir, targetNS);
				
				boolean nsAwayCheck = FileUtils.directoryContains(visitorDir, targetVisitorNS);

				if(nsCheck){

					File targetFileName = new File (effNS+File.separator+newFileName);

					boolean fileCheck = FileUtils.directoryContains(targetNS, targetFileName);

					if(fileCheck){

						docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG));

						return docAddValidateResponseBuilder.build();

					}

				}
				
				if(nsAwayCheck){
					
					File targetFileName = new File (effVisitorNS+File.separator+newFileName);

					boolean fileCheck = FileUtils.directoryContains(targetVisitorNS, targetFileName);
					
					logger.info("Validating "+effVisitorNS+" for "+newFileName+" as "+fileCheck);

					if(fileCheck){
						
						docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG));

						return docAddValidateResponseBuilder.build();

					}

				}
					
		} catch (IOException e) {

				logger.error("Document Response: IO Exception while validating file add request "+e.getMessage());

				docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG));

				return docAddValidateResponseBuilder.build();

			}

		}else{

			try {

				boolean fileCheck = FileUtils.directoryContains(homeDir, new File(HOMEDIR+File.separator+newFileName));
				
				boolean visitorFileCheck = FileUtils.directoryContains(visitorDir, new File(VISITORDIR+File.separator+newFileName));
				
				logger.info("Validating "+VISITORDIR+" for "+newFileName+" as "+visitorFileCheck);

				if(fileCheck){

					docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG));

					return docAddValidateResponseBuilder.build();
			
				}
				
				if(visitorFileCheck){

					docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG));

					return docAddValidateResponseBuilder.build();
				}

			} catch (IOException e) {

				logger.error("Document Response: IO Exception while validating file add request "+e.getMessage());

				docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG));

				return docAddValidateResponseBuilder.build();
			}
		}

		NodeResponseQueue.broadcastDocQuery(nameSpace, newFileName);
		
		try {
		
			logger.info(" Document resousrce sleeping for 2000ms! Witing for responses from the other nodes for DOCQUERY ");
		
			Thread.sleep(2000);
			
			boolean docQueryResult = NodeResponseQueue.fetchDocQueryResult(nameSpace , newFileName);
			
			if(!docQueryResult){
				docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG));

				return docAddValidateResponseBuilder.build();
				}
		
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		try {
			spacceAvailable = FileSystemUtils.freeSpaceKb()*1024;

			bufferredLimit = spacceAvailable - 10240000;

			logger.info("DocumentResource: Free Space available " + spacceAvailable);

		} catch (IOException e) {

			System.out.println("DpcumentResource:docAddValidate IOException while calculating free space");

			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG));

			return docAddValidateResponseBuilder.build();
		}

		if(reqFileSize > bufferredLimit){

			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, FILETOOLARGETOSAVEMSG));

		}else{

			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.SUCCESS, FILEUPLOADREQVALIDATEDMSG));
		}

		return docAddValidateResponseBuilder.build();
	}


	private Response docAdd(Header docAddHeader , Payload docAddBody){

		String nameSpace = docAddBody.getSpace().getName();

		String effNS = HOMEDIR+File.separator+nameSpace;

		String fileName = docAddBody.getDoc().getDocName();

		logger.info("DocAdd: Received file "+fileName);

		logger.info("effective namespace "+effNS);

		File nameDir = new File(effNS);

		File file = new File(effNS+File.separator+fileName);

		Header.Builder docAddHeaderBuilder = Header.newBuilder(docAddHeader);

		Document recivedFile = docAddBody.getDoc();

		Document toBesent= null;

		try {

			logger.info("Creating directory with name "+nameSpace );

			FileUtils.forceMkdir(nameDir);

			logger.info("Creating file with name "+fileName+" and writing the content sent by client to it" );

			FileUtils.writeByteArrayToFile(file, recivedFile.getChunkContent().toByteArray(), true);

			toBesent = recivedFile.toBuilder().clearChunkContent().build();

		} catch (IOException e) {

			logger.info("Exception while creating the file and/or writing the content to it "+e.getMessage());

			docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);

			docAddHeaderBuilder.setReplyMsg("Server Exception while uploading a file");

			e.printStackTrace();
		}

		System.gc();

		docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);

		docAddHeaderBuilder.setReplyMsg("File Uploaded Successfully");

		Response.Builder docAddRespBuilder = Response.newBuilder();

		docAddRespBuilder.setHeader(docAddHeaderBuilder);

		System.gc();

		docAddRespBuilder.setBody(PayloadReply.newBuilder().addDocs(toBesent).addSpaces(docAddBody.getSpace()));

		System.gc();

		return docAddRespBuilder.build();
	}

	private Response docFind(Header docFindHeader , Payload docFindBody){

		return null;
	}

	private Response docUpdate(Header docUpdateHeader , Payload docUpdateBody){

		return null;
	}

	private Response docRemove(Header docRemoveHeader , Payload docRemoveBody){

		String fileToBeDeleted = docRemoveBody.getDoc().getDocName();

		String nameSpece = docRemoveBody.getSpace().getName();

		logger.info("docRemove Client data file to be delted: "+fileToBeDeleted+" namespace: "+nameSpece);

		File targetFile = null;

		Response.Builder fileRemoveResponseBuilder = Response.newBuilder();

		fileRemoveResponseBuilder.setBody(PayloadReply.newBuilder().build());

		if(fileToBeDeleted == null || fileToBeDeleted.length() ==0){

			fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEREQMISSINGPARAMMSG));

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

							fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, OPERATIONNOTALLOWEDMSG+"Supplied file is directory"));

							return fileRemoveResponseBuilder.build();
						}

						FileUtils.forceDelete(targetFile);

					}else{

						fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEINEXISTENTMSG));

						return fileRemoveResponseBuilder.build();
					}

				}else{

					fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, NAMESPACEINEXISTENTMSG));

					return fileRemoveResponseBuilder.build();

				}

			} catch (IOException e) {

				logger.error("Document Response: IO Exception while processing file delete request "+e.getMessage());

				fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG));

				return fileRemoveResponseBuilder.build();

			}

		} else{

			try {

				targetFile = new File(HOMEDIR+File.separator+fileToBeDeleted);

				boolean fileCheck = FileUtils.directoryContains(homeDir, targetFile);

				if(fileCheck){

					if(targetFile.isDirectory()){

						fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, OPERATIONNOTALLOWEDMSG+"Requested file is directory"));

						return fileRemoveResponseBuilder.build();
					}

					FileUtils.forceDelete(targetFile);

				}else{

					fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, FILEINEXISTENTMSG));

					return fileRemoveResponseBuilder.build();

				}

			} catch (IOException e) {

				logger.error("Document Response: IO Exception while processing file delete request w/o namespace "+e.getMessage());

				fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.FAILURE, INTERNALSERVERERRORMSG));

				return fileRemoveResponseBuilder.build();
			}

		}

		fileRemoveResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docRemoveHeader, ReplyStatus.SUCCESS, FILEDELETESUCCESSFULMSG));

		return fileRemoveResponseBuilder.build();
	}

	private Response docQuery(Header docQueryHeader , Payload docQueryBody){

		logger.info(" Received doc query request from "+docQueryHeader.getOriginator());

		Response.Builder docQueryResponseBuilder = Response.newBuilder();

		Document queryDoc = docQueryBody.getDoc();

		NameSpace space = docQueryBody.getSpace();

		String fileName = queryDoc.getDocName();

		String nameSpace =  null;

		String effHomeNS = HOMEDIR;
		
		String effAwayNS = VISITORDIR;

		if(space !=null){
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
				fileHome =	FileUtils.directoryContains(parentHomeDir, targetFile);
				logger.info("validating "+effHomeNS+" for "+fileName+" as "+fileHome);
			}
			
			if(!fileHome){
			if(parentAwayDir.exists()){
				
			targetFile = new File(effAwayNS+File.separator+fileName);
			fileAway =	FileUtils.directoryContains(parentAwayDir, targetFile);
				logger.info("validating "+effAwayNS+" for "+fileName+" as "+fileAway);
			}
			}
			
			

		} catch (IOException e) {

			logger.error("DocQuery: IOException while validating file existence");
			e.printStackTrace();
		}

		if(fileHome || fileAway)
			docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.FAILURE, FILEADDREQDUPLICATEFILEMSG).toBuilder().setOriginator(HeartbeatManager.getInstance().getNodeId()));
		else
			docQueryResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docQueryHeader, ReplyStatus.SUCCESS, FILEUPLOADREQVALIDATEDMSG).toBuilder().setOriginator(HeartbeatManager.getInstance().getNodeId()));	

		
		return docQueryResponseBuilder.build();
	}

}
