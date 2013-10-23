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

import org.apache.commons.io.FileSystemUtils;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;
import poke.server.storage.jdbc.DatabaseStorage;
import poke.server.storage.jdbc.SpaceMapper;
import eye.Comm;
import eye.Comm.Header;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;

public class DocumentResource implements Resource {
	
	protected static Logger logger = LoggerFactory.getLogger("DocumentResource");
	protected static DatabaseStorage dbInst;

	@Override
	public Response process(Request request, DatabaseStorage dbInstance) {
		dbInst = dbInstance;
		int opChoice = 0;
		
		Response docOpResponse = null;
		
		Header docOpHeader = request.getHeader();
		
		Payload docOpBody =  request.getBody();
		
		opChoice = docOpHeader.getRoutingId().getNumber();
		
		switch(opChoice){
		
		case 19:
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
		
		default:
			System.out.println("DpcumentResource: No matching doc op id found");
		
		
		}
		
		return docOpResponse;
	}

	private Response docAddValidate(Header docAddValidateHeader , Payload docAddValidateBody){
		
		int reqFileSize = docAddValidateBody.getFile().getFileSize();
		
		Response.Builder docAddValidateResponseBuilder = Response.newBuilder();
		
		long spacceAvailable = 0;
		
		try {
			spacceAvailable = FileSystemUtils.freeSpaceKb()*1024;
			
			System.out.println("DocumentResource: Free Space available " + spacceAvailable);
		} catch (IOException e) {
			System.out.println("DpcumentResource:docAddValidate IOException while calculating free space");
			e.printStackTrace();
		}
		
		if(reqFileSize < (spacceAvailable - 102400)){
			
			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.SUCCESS, null));
						
		}else{
			
			docAddValidateResponseBuilder.setHeader(ResourceUtil.buildHeaderFrom(docAddValidateHeader, ReplyStatus.FAILURE, null));
		}
		
		docAddValidateResponseBuilder.setBody(PayloadReply.newBuilder().build());
		return docAddValidateResponseBuilder.build();
	}
	
	
	private Response docAdd(Header docAddHeader , Payload docAddBody){
		
		String nameSpace = docAddBody.getSpace().getName();
		
		String fileName = docAddBody.getFile().getFileName();
		
		logger.info("Received file "+fileName);
		
		logger.info("Creating namespace "+nameSpace);
		
		//File nameDir = new File(nameSpace);
		
		File file = new File(nameSpace+"//"+fileName);
		
		Header.Builder docAddHeaderBuilder = Header.newBuilder(docAddHeader);
		
		try {
		
			logger.info("Creating directory with name "+nameSpace );
			
			//FileUtils.forceMkdir(nameDir);
			
			logger.info("Creating file with name "+fileName+" and woritng the content sent by client to it" );
			
			FileUtils.writeByteArrayToFile(file, docAddBody.getFile().getFileData().toByteArray(), true);
			
			dbInst.addDocument(nameSpace, docAddBody.getFile());
		
		} catch (IOException e) {
			
			logger.info("Exception while creating the file and/or writing the content to it "+e.getMessage());
			
			docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);
			
			docAddHeaderBuilder.setReplyMsg("Server Exception while uploading a file");
			
			e.printStackTrace();
		}
		
		docAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);
		
		docAddHeaderBuilder.setReplyMsg("File Uploaded Successfully");
		
		Response.Builder docAddRespBuilder = Response.newBuilder();
		
		docAddRespBuilder.setHeader(docAddHeaderBuilder);
		
		docAddRespBuilder.setBody(PayloadReply.newBuilder().build());
		
		return docAddRespBuilder.build();
	}
	
	private Response docFind(Header docFindHeader , Payload docFindBody){
		
		return null;
	}
	
	private Response docUpdate(Header docUpdateHeader , Payload docUpdateBody){
		
		return null;
	}
	
	private Response docRemove(Header docRemoveHeader , Payload docRemoveBody){
		
		return null;
	}
	
}
