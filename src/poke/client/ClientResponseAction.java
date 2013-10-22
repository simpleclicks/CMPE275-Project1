package poke.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.provider.certpath.OCSPResponse.ResponseStatus;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.Request;
import eye.Comm.Header.Routing;

public class ClientResponseAction {
	
private LinkedBlockingDeque<eye.Comm.Response> inbound;
private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
private Channel serverChannel;
private InboundWorker inBoundworker;
private OutboundWorker outBoundworker;
protected static Logger logger = LoggerFactory.getLogger("ClientResponseAction");
private static final int MAX_UNCHUNKED_FILE_SIZE = 26214400;
	
public Channel getServerChannel() {
	return serverChannel;
}

public void setServerChannel(Channel serverChannel) {
	this.serverChannel = serverChannel;
}

	
	
	ClientResponseAction(Channel serverChannel){
		
		this.serverChannel = serverChannel;
		init();
	}
	
	public void init(){
		
		inbound = new LinkedBlockingDeque<eye.Comm.Response>();
		
		outbound = new LinkedBlockingDeque<GeneratedMessage>();
		
		inBoundworker = new InboundWorker(this);
		
		outBoundworker = new OutboundWorker(this);
		
		inBoundworker.start();
		
		outBoundworker.start();
	}
	
	public void enqueueRecievedResponse( eye.Comm.Response serverResponse){
		
		try {
		
			inbound.put(serverResponse);
		
		} catch (InterruptedException e) {
			
			System.out.println("Unable to put msg onto client response handler inbound queue");
			e.printStackTrace();
		}
	}
	
	protected class InboundWorker extends Thread {
		
		boolean forever = true;
		int retry = 0;
		ClientResponseAction target;

		public InboundWorker(ClientResponseAction processResponse) {
			
			target= processResponse;
			if (target.inbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
				
			while (true) {
				
				if (!forever && target.inbound.size() == 0){
					
					break;
				}

				try {
					// block until a message is enqueued
					eye.Comm.Response msg = target.inbound.take();
					
					if (msg.getHeader().getRoutingId() == Header.Routing.DOCADDHANDSHAKE) {

						System.out.println("ClientResponseHandler: Recieved the response to doccAddHandshake from the server and the response is "+msg.getHeader().getReplyCode()+" with Message fom server as "+msg.getHeader().getReplyMsg());
						
						//System.out.println("File path recived from the server "+msg.getBody().getDocs(0).getDocExtension());
					
						if(msg.getHeader().getReplyCode() == Header.ReplyStatus.SUCCESS){
								fileUpload(msg.getBody());
						}
					
					}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCADD){

						System.out.println("ClientResponseHandler: Recieved the response to doccADD from the server and the response is "+msg.getHeader().getReplyCode()+" with Message fom server as "+msg.getHeader().getReplyMsg());
						
						if(msg.getHeader().getReplyCode() == Header.ReplyStatus.SUCCESS){
							
							System.out.println("next chunk to be sent "+(msg.getBody().getDocs(0).getChunkId()+1));
					  }
						
					}
						
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ClientConnection.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ClientConnection.logger.info("connection queue closing");
			}
		}
		
		private void fileUpload(eye.Comm.PayloadReply docUpload){
			
			Header.Builder docAddReqHeader = Header.newBuilder();
			
			docAddReqHeader.setRoutingId(Routing.DOCADD);
			
			docAddReqHeader.setOriginator("Doc add test");
			
			Request.Builder docAddReqBuilder = Request.newBuilder();
			
			docAddReqBuilder.setHeader(docAddReqHeader);
			
			Payload.Builder docAddPLBuilder = Payload.newBuilder();
			
			String nameSpace = docUpload.getSpaces(0).getName();
			
			String filePath = docUpload.getDocs(0).getDocExtension();
						
			if(nameSpace != null && nameSpace.length() > 0)
				docAddPLBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace).build());
			
			String fileExt = FilenameUtils.getExtension(filePath);
			
			String fileName = FilenameUtils.getName(filePath);
					
			java.io.File file = FileUtils.getFile(filePath);
			
			long fileSize = FileUtils.sizeOf(file);
			
			logger.info("Size of the file to be sent "+fileSize);
			
			long totalChunk = ((fileSize / MAX_UNCHUNKED_FILE_SIZE))+1;
			
			if(fileSize < MAX_UNCHUNKED_FILE_SIZE ){
				
				logger.info(" DocADD: Sending the complete file in unchunked mode");
				
				logger.info("Total number of chunks "+totalChunk);
				
			byte[] fileContents = null;
			
			try {
				
				fileContents = FileUtils.readFileToByteArray(file);
				
							
			} catch (IOException e) {
				
				logger.error("Error while reading the specified file "+e.getMessage());
				return ;
	     	}
			
			docAddPLBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocExtension(fileExt).
					setChunkContent(ByteString.copyFrom(fileContents)).setDocSize(fileSize).setTotalChunk(totalChunk).setChunkId(1));
			
			docAddReqBuilder.setBody(docAddPLBuilder);
			
			try {
				// enqueue message
				outbound.put(docAddReqBuilder.build());
			} catch (InterruptedException e) {
				logger.warn("Unable to deliver doc add message, queuing "+e.getMessage());
			}
			
			}else{
				
				logger.info(" DocADD: Uploading the file in chunked mode");
				
				logger.info("Total number of chunks "+totalChunk);
				
				try {
					
					int bytesRead = 0;
					
					int chunkId = 1;
					
					FileInputStream chunkFIS = new FileInputStream(file);
					
					do{
						
					int bytesRemaining = chunkFIS.available();
						
					byte[] chunckContents = null; 
					
					if(bytesRemaining > MAX_UNCHUNKED_FILE_SIZE){
						
						chunckContents = new byte[MAX_UNCHUNKED_FILE_SIZE];
						
						bytesRead= IOUtils.read(chunkFIS, chunckContents , 0 , MAX_UNCHUNKED_FILE_SIZE);
					}
					else{
						chunckContents = new byte[bytesRemaining];
						
						//System.out.println("length of last chunk content "+chunckContents.length);
						
						bytesRead= IOUtils.read(chunkFIS, chunckContents , 0 , bytesRemaining);
						
					}
										 
					 docAddPLBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocExtension(fileExt).
								setChunkContent(ByteString.copyFrom(chunckContents)).setDocSize(fileSize).setTotalChunk(totalChunk).setChunkId(chunkId));
						
						docAddReqBuilder.setBody(docAddPLBuilder);
						
						try {
						
							
							outbound.put(docAddReqBuilder.build());
							sleep(10);
						
						} catch (InterruptedException e) {
							
							logger.warn("Unable to deliver doc add (chunked) message, queuing "+e.getMessage());
						}
						
						chunkId++;
					
					}while(chunkFIS.available() > 0  );
					
					logger.info("Out of chunked write while loop");
					
					System.gc();
			
				} catch (FileNotFoundException e) {
				
					logger.info("Requested File does not exists: File uploading Aborted "+e.getMessage());
				
					e.printStackTrace();
				
				} catch (IOException e) {
					
					logger.info("IO exception while uploading the requested file : File upload Aborted "+e.getMessage());
					
					e.printStackTrace();
				}
				
			}
			
			logger.info("DocAdd: File Send activity complete ");
			
			System.gc();
			
		}
	}
	
	protected class OutboundWorker extends Thread {
		
		ClientResponseAction conn;
		boolean forever = true;
		int retry = 0;

		public OutboundWorker(ClientResponseAction conn) {
		
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			
			Channel ch = conn.serverChannel;
			
				if (ch == null || !ch.isOpen()) {
				ClientConnection.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				
				if (!forever && conn.outbound.size() == 0){
					 break;
				}

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {

						logger.info("OoutboundWroker: sending message to the server...");
						
						ChannelFuture cf = ch.write(msg);
						
						if (cf.isDone() && !cf.isSuccess()) {
						
							conn.outbound.putFirst(msg);
						}
						
					

					} else{
						conn.outbound.putFirst(msg);
						logger.info("Channel is not writable");
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					ClientConnection.logger.error("Unexpected communcation failure", e.getCause());
					break;
				}
			}

			if (!forever) {
				ClientConnection.logger.info("connection queue closing");
			}
		}
		
	}

}
