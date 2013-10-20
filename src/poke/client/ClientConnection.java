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
package poke.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Finger;
import eye.Comm.Header;
import eye.Comm.Header.Builder;
import eye.Comm.Header.Routing;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.Request;

/**
 * provides an abstraction of the communication to the remote server.
 * 
 * @author gash
 * 
 */
public class ClientConnection {

	protected static Logger logger = LoggerFactory.getLogger("client");
	private String host;
	private int port;
	private ChannelFuture channel; // do not use directly call connect()!
	private ClientBootstrap bootstrap;
	ClientDecoderPipeline clientPipeline;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker worker;
	private static boolean firstTimeConnect = true; 

	protected ClientConnection(String host, int port) {
		this.host = host;
		this.port = port;

		init();
	}

	/**
	 * release all resources
	 */
	public void release() {
		bootstrap.releaseExternalResources();
	}

	public static ClientConnection initConnection(String host, int port) {

		ClientConnection rtn = new ClientConnection(host, port);
		return rtn;
	}

	/**
	 * add an application-level listener to receive messages from the server (as
	 * in replies to requests).
	 * 
	 * @param listener
	 */
	public void addListener(ClientListener listener) {
		try {
			if (clientPipeline != null)
				clientPipeline.addListener(listener);
		} catch (Exception e) {
			logger.error("failed to add listener", e);
		}
	}

	public void poke(String tag, int num) {
		// data to send
		Finger.Builder f = eye.Comm.Finger.newBuilder();
		f.setTag(tag);
		f.setNumber(num);

		// payload containing data
		Request.Builder r = Request.newBuilder();
		eye.Comm.Payload.Builder p = Payload.newBuilder();
		p.setFinger(f.build());
		r.setBody(p.build());

		// header with routing info
		eye.Comm.Header.Builder h = Header.newBuilder();
		h.setOriginator("client");
		h.setTag("test finger");
		h.setTime(System.currentTimeMillis());
		h.setRoutingId(eye.Comm.Header.Routing.FINGER);
		r.setHeader(h.build());

		eye.Comm.Request req = r.build();

		try {
			// enqueue message
			outbound.put(req);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver message, queuing");
		}
	}
	
	public void docAddReq(String nameSpace,String fileName,int fileSize){
		
		Header.Builder docAddReqHeader = Header.newBuilder();
		
		docAddReqHeader.setRoutingId(Routing.DOCADDHANDSHAKE);
		
		docAddReqHeader.setOriginator("Doc add test");
		
		Payload.Builder docAddBodyBuilder = Payload.newBuilder();
		
		docAddBodyBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace).build());
		
		docAddBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocSize(fileSize).build());
		
		Request.Builder docAddReqBuilder = Request.newBuilder();
		
		docAddReqBuilder.setHeader(docAddReqHeader.build());
		
		docAddReqBuilder.setBody(docAddBodyBuilder.build());
		
		Request docAddReq = docAddReqBuilder.build();
		
		try {
			// enqueue message
			outbound.put(docAddReq);
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver doc add req message, queuing");
		}
	}
	
	public void docAdd(String nameSpace , String filePath){
		
		Header.Builder docAddReqHeader = Header.newBuilder();
		
		docAddReqHeader.setRoutingId(Routing.DOCADD);
		
		docAddReqHeader.setOriginator("Doc add test");
		
		Payload.Builder docAddBodyBuilder = Payload.newBuilder();
		
		docAddBodyBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace).build());
		
		String fileExt = FilenameUtils.getExtension(filePath);
		
		String fileName = FilenameUtils.getName(filePath);
				
		java.io.File file = FileUtils.getFile(filePath);
		
		byte[] fileContents = null;
		
		try {
			
			fileContents = FileUtils.readFileToByteArray(file);
			
						
		} catch (IOException e) {
			
			logger.error("Error while reading the specified file "+e.getMessage());
			return ;
     	}
		
		docAddBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocExtension(fileExt).
				setChunkContent(ByteString.copyFrom(fileContents)).setDocSize(fileContents.length).setTotalChunk(1));
		
		Request.Builder docAddReqBuilder = Request.newBuilder();
		
		docAddReqBuilder.setHeader(docAddReqHeader);
		
		docAddReqBuilder.setBody(docAddBodyBuilder);
		
		try {
			// enqueue message
			outbound.put(docAddReqBuilder.build());
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver doc add message, queuing "+e.getMessage());
		}
}
	public void docRemove(String nameSpace , String fileName){
	
		Header.Builder docRemoveReqHeader = Header.newBuilder();
		
		docRemoveReqHeader.setRoutingId(Routing.DOCREMOVE);
		
		docRemoveReqHeader.setOriginator("Doc remove test");
		
		Payload.Builder docRemoveBodyBuilder = Payload.newBuilder();
		
		if(nameSpace !=null && nameSpace.length() > 0)
			docRemoveBodyBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace).build());
		
		docRemoveBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName));
		
		Request.Builder docRemoveReqBuilder = Request.newBuilder();
		
		docRemoveReqBuilder.setBody(docRemoveBodyBuilder.build());
		
		docRemoveReqBuilder.setHeader(docRemoveReqHeader.build());
		
		try {
			
			outbound.put(docRemoveReqBuilder.build());
		
		} catch (InterruptedException e) {
			logger.warn("Unable to deliver doc remove message, queuing "+e.getMessage());
		}
	
	}
	

	private void init() {
		// the queue to support client-side surging
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		clientPipeline = new ClientDecoderPipeline();
		bootstrap.setPipelineFactory(clientPipeline);

		// start outbound message processor
		worker = new OutboundWorker(this);
		worker.start();
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			// System.out.println("---> connecting");
			channel = bootstrap.connect(new InetSocketAddress(host, port));

			// cleanup on lost connection

		}

		// wait for the connection to establish
		channel.awaitUninterruptibly();

		if (channel.isDone() && channel.isSuccess())
			return channel.getChannel();
		else
			throw new RuntimeException("Not able to establish connection to server");
	}

	/**
	 * queues outgoing messages - this provides surge protection if the client
	 * creates large numbers of messages.
	 * 
	 * @author gash
	 * 
	 */
	protected class OutboundWorker extends Thread {
		ClientConnection conn;
		boolean forever = true;

		public OutboundWorker(ClientConnection conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			
			Channel ch = null;
			
			if(firstTimeConnect){
			
				ch = conn.connect();
				firstTimeConnect = false;
		
			}
			if (ch == null || !ch.isOpen()) {
				ClientConnection.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && conn.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {
//						ClientHandler handler = conn.connect().getPipeline().get(ClientHandler.class);
//
//						if (!handler.send(msg))
//							conn.outbound.putFirst(msg);
						logger.info("Sending message to the server...");
						ChannelFuture cf = ch.write(msg);
						if (cf.isDone() && !cf.isSuccess()) {
							logger.error("failed to poke!");
							conn.outbound.putFirst(msg);
						}
						
						

					} else
						conn.outbound.putFirst(msg);
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
	}
}
