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

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.resources.Resource;
import poke.server.resources.ResourceUtil;
import eye.Comm.Header;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import eye.Comm.Header.ReplyStatus;

public class NameSpaceResource implements Resource {
	protected static Logger logger = LoggerFactory.getLogger("NamespaceResource");
	private static final String HOMEDIR = "home";

	private static final String VISITORDIR = "away";

	private static final String NAMESPACEEXIST = " requested namespacce already exist:";

	private static final File homeDir = new File(HOMEDIR);

	@Override
	public Response process(Request request) {
		// TODO Auto-generated method stub

		int opChoice = 0;

		Response docOpResponse = null;

		Header docOpHeader = request.getHeader();

		Payload docOpBody =  request.getBody();

		opChoice = docOpHeader.getRoutingId().getNumber();

		switch(opChoice){
		case 10:
			docOpResponse = namespaceAdd(docOpHeader, docOpBody);
			break;

		default:
			System.out.println("NamespaceResource: No matching doc op id found for "+opChoice);
		}
		return docOpResponse;
	}
	
	private Response namespaceAdd(Header namespaceAddHeader, Payload namespaceAddBody) {
		// Add new namespace
		
		String nameSpace = namespaceAddBody.getSpace().getName();
		String namespacePath = HOMEDIR+File.separator+nameSpace;

		logger.info("namespace to be created "+nameSpace);

		File namespaceDir = new File(namespacePath);
		Response.Builder namespaceAddResponse = Response.newBuilder();
		Header.Builder namespaceAddHeaderBuilder = Header.newBuilder(namespaceAddHeader);
		boolean checkNamespace = false;

		try {
			FileUtils.forceMkdir(homeDir);
			checkNamespace = FileUtils.directoryContains(homeDir, namespaceDir);
			if(checkNamespace){
				logger.info("Namespace already exists");
				namespaceAddResponse.setHeader(ResourceUtil.buildHeaderFrom(namespaceAddHeader, ReplyStatus.FAILURE, NAMESPACEEXIST));

				return namespaceAddResponse.build();
			} else {
				try{
					logger.info("Creating directory with name "+nameSpace );
					FileUtils.forceMkdir(namespaceDir);
					namespaceAddHeaderBuilder.setReplyCode(Header.ReplyStatus.SUCCESS);
					namespaceAddHeaderBuilder.setReplyMsg("namespace created  Successfully");
				} catch (Exception e) {

					logger.warn("Exception while creating namespace "+ nameSpace+ " " + e.getMessage());

					namespaceAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);
					namespaceAddHeaderBuilder.setReplyMsg("Server Exception while creating namespace");
				}
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
			namespaceAddHeaderBuilder.setReplyCode(Header.ReplyStatus.FAILURE);
			namespaceAddHeaderBuilder.setReplyMsg("Server Exception while checking for namespace existance");
		}

		namespaceAddResponse.setHeader(namespaceAddHeaderBuilder);
		namespaceAddResponse.setBody(PayloadReply.newBuilder().build());

		return namespaceAddResponse.build();

	}

}
