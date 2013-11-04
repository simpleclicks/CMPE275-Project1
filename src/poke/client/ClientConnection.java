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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
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

	private ChannelFuture channeluture; // do not use directly call connect()!

	private Channel channel;

	private ClientBootstrap bootstrap;

	private ClientDecoderPipeline clientPipeline;

	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;

	private OutboundWorker worker;

	private static final long MAX_UNCHUNKED_FILE_SIZE = 26214400L;

	public ChannelFuture getChanneluture() {
		return channeluture;
	}

	public void setChanneluture(ChannelFuture channeluture) {
		this.channeluture = channeluture;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

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

	public void namespaceAdd(String namespace) {
		// request to add namespace

		Header.Builder namespaceAddHeader = Header.newBuilder();
		namespaceAddHeader.setRoutingId(Routing.NAMESPACEADD);
		namespaceAddHeader.setOriginator("Namespace Add test");

		Payload.Builder namespaceAddBodyBuilder = Payload.newBuilder();
		namespaceAddBodyBuilder.setSpace(NameSpace.newBuilder().setName(namespace));

		Request.Builder namespaceAddReqBuilder = Request.newBuilder();
		namespaceAddReqBuilder.setHeader(namespaceAddHeader);
		namespaceAddReqBuilder.setBody(namespaceAddBodyBuilder);

		try {
			//enqueue message

			outbound.put(namespaceAddReqBuilder.build());
			logger.info("put namespace add request in outbound queue for namespace "+ namespace);
		} catch (Exception e) {
			logger.warn("Failed to put namespace add request in outbound queue for namespace "+ namespace);

		}




	}


	public void docAddReq(String nameSpace, String filePath) {

		Header.Builder docAddReqHeader = Header.newBuilder();

		String fileName = FilenameUtils.getName(filePath);

		logger.info("File to be uploaded to the server "+fileName);


		logger.info("File to be uploaded to the server " + fileName);

		File fts = new File(filePath);

		long fileSize = FileUtils.sizeOf(fts);

		logger.info("Size of the file to be uploaded to the server " + fileSize);

		docAddReqHeader.setRoutingId(Routing.DOCADDHANDSHAKE);

		docAddReqHeader.setOriginator("Doc add test");

		// docAddReqHeader.setTag(filePath);

		Payload.Builder docAddBodyBuilder = Payload.newBuilder();
		
		if(nameSpace !=null && nameSpace.length()>0){
			
			if(nameSpace.contains("/") && File.separator.equals("\\"))
			{
				logger.error("Invalid namespace declaration");
				return;
			}
			docAddBodyBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace).build());
		}
		docAddBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocSize(fileSize).setDocExtension(filePath));
		

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

	public void docAdd(String nameSpace, String filePath) {

		Header.Builder docAddReqHeader = Header.newBuilder();

		docAddReqHeader.setRoutingId(Routing.DOCADD);

		docAddReqHeader.setOriginator("Doc add test");

		Request.Builder docAddReqBuilder = Request.newBuilder();

		docAddReqBuilder.setHeader(docAddReqHeader);

		Payload.Builder docAddPLBuilder = Payload.newBuilder();

		if (nameSpace != null && nameSpace.length() > 0)
			docAddPLBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace)
					.build());

		String fileExt = FilenameUtils.getExtension(filePath);

		String fileName = FilenameUtils.getName(filePath);

		java.io.File file = FileUtils.getFile(filePath);

		long fileSize = FileUtils.sizeOf(file);

		logger.info("Size of the file to be sent " + fileSize);

		long totalChunk = ((fileSize / MAX_UNCHUNKED_FILE_SIZE)) + 1;

		if (fileSize < MAX_UNCHUNKED_FILE_SIZE) {

			logger.info(" DocADD: Sending the complete file in unchunked mode");

			logger.info("Total number of chunks " + totalChunk);

			byte[] fileContents = null;

			try {

				fileContents = FileUtils.readFileToByteArray(file);

			} catch (IOException e) {

				logger.error("Error while reading the specified file "
						+ e.getMessage());
				return;
			}

			docAddPLBuilder.setDoc(Document.newBuilder().setDocName(fileName)
					.setDocExtension(fileExt)
					.setChunkContent(ByteString.copyFrom(fileContents))
					.setDocSize(fileSize).setTotalChunk(totalChunk)
					.setChunkId(1));

			docAddReqBuilder.setBody(docAddPLBuilder);

			try {
				// enqueue message
				outbound.put(docAddReqBuilder.build());
			} catch (InterruptedException e) {
				logger.warn("Unable to deliver doc add message, queuing "
						+ e.getMessage());
			}

		} else {

			logger.info(" DocADD: Uploading the file in chunked mode");

			logger.info("Total number of chunks " + totalChunk);

			try {

				int bytesRead = 0;

				int chunkId = 1;

				FileInputStream chunkeFIS = new FileInputStream(file);

				do {

					byte[] chunckContents = new byte[26214400];

					bytesRead = IOUtils.read(chunkeFIS, chunckContents, 0,
							26214400);

					logger.info("Total number of bytes read for chunk "
							+ chunkId + ": " + bytesRead);

					// logger.info("Contents of the chunk "+chunkId+" : "+chunckContents);

					docAddPLBuilder.setDoc(Document
							.newBuilder()
							.setDocName(fileName)
							.setDocExtension(fileExt)
							.setChunkContent(
									ByteString.copyFrom(chunckContents))
							.setDocSize(fileSize).setTotalChunk(totalChunk)
							.setChunkId(chunkId));

					docAddReqBuilder.setBody(docAddPLBuilder);

					try {

						outbound.put(docAddReqBuilder.build());

					} catch (InterruptedException e) {

						logger.warn("Unable to deliver doc add (chunked) message, queuing "
								+ e.getMessage());
					}

					chunckContents = null;

					System.gc();

					chunkId++;

				} while (chunkeFIS.available() > 0);

				logger.info("Out of chunked write while loop");

			} catch (FileNotFoundException e) {

				logger.info("Requested File does not exists: File uploading Aborted "
						+ e.getMessage());

				e.printStackTrace();

			} catch (IOException e) {

				logger.info("IO exception while uploading the requested file : File upload Aborted "
						+ e.getMessage());

				e.printStackTrace();
			}

		}

		logger.info("DocAdd: File Send activity complete ");

	}

	public void docRemove(String nameSpace, String fileName) {

		Header.Builder docRemoveReqHeader = Header.newBuilder();

		docRemoveReqHeader.setRoutingId(Routing.DOCREMOVE);

		docRemoveReqHeader.setOriginator("Doc remove test");

		Payload.Builder docRemoveBodyBuilder = Payload.newBuilder();

		if (nameSpace != null && nameSpace.length() > 0)
			docRemoveBodyBuilder.setSpace(NameSpace.newBuilder()
					.setName(nameSpace).build());

		docRemoveBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName));

		logger.info((docRemoveBodyBuilder.getSpace().getName().length())+"");
		
		Request.Builder docRemoveReqBuilder = Request.newBuilder();

		docRemoveReqBuilder.setBody(docRemoveBodyBuilder.build());

		docRemoveReqBuilder.setHeader(docRemoveReqHeader.build());

		try {

			outbound.put(docRemoveReqBuilder.build());

		} catch (InterruptedException e) {
			logger.warn("Unable to deliver doc remove message, queuing "
					+ e.getMessage());
		}

		System.gc();

	}

	public void docFind(String nameSpace, String fileName) {
		Header.Builder docFindReqHeader = Header.newBuilder();
		docFindReqHeader.setRoutingId(Routing.DOCFIND);
		docFindReqHeader.setOriginator("From Client");
		Payload.Builder docFindReqBody = Payload.newBuilder();

		if (nameSpace != null && nameSpace.length() > 0)
			docFindReqBody.setSpace(NameSpace.newBuilder().setName(nameSpace)
					.build());

		docFindReqBody.setDoc(Document.newBuilder().setDocName(fileName));

		Request.Builder docFindRequest = Request.newBuilder();
		docFindRequest.setBody(docFindReqBody.build());
		docFindRequest.setHeader(docFindReqHeader.build());

		try {

			outbound.put(docFindRequest.build());

		} catch (InterruptedException e) {
			logger.warn("Unable to deliver doc find message, queuing "
					+ e.getMessage());
		}

	}

	public void namespaceRemove(String namespace) {
		
		// remove namespace
		
		Header.Builder namespaceRemoveReqHeader = Header.newBuilder();

		namespaceRemoveReqHeader.setRoutingId(Routing.NAMESPACEREMOVE);

		namespaceRemoveReqHeader.setOriginator("Namespace remove test");

		Payload.Builder namespaceRemoveBodyBuilder = Payload.newBuilder();

		if(namespace !=null && namespace.length() > 0)
			namespaceRemoveBodyBuilder.setSpace(NameSpace.newBuilder().setName(namespace).build());

		//namespaceRemoveBodyBuilder.setDoc(Document.newBuilder().setDocName(fileName));

		Request.Builder namespaceRemoveRequest = Request.newBuilder();

		namespaceRemoveRequest.setHeader(namespaceRemoveReqHeader.build());
		namespaceRemoveRequest.setBody(namespaceRemoveBodyBuilder.build());


		try {

			outbound.put(namespaceRemoveRequest.build());

		} catch (InterruptedException e) {
			logger.warn("Unable to deliver namespace remove message, queuing "+e.getMessage());
		}

	}

	public void namespaceList(String nameSpace) {
		// To list the namespace and files in it
		
		Header.Builder namespaceListReqHeader = Header.newBuilder();
		namespaceListReqHeader.setRoutingId(Routing.NAMESPACELIST);
		namespaceListReqHeader.setOriginator("namespace List test");
        Payload.Builder namespaceListReqBody = Payload.newBuilder();

        if (nameSpace != null && nameSpace.length() > 0)
        	namespaceListReqBody.setSpace(NameSpace.newBuilder().setName(nameSpace)
                                .build());


        Request.Builder namespaceListRequest = Request.newBuilder();
        namespaceListRequest.setBody(namespaceListReqBody.build());
        namespaceListRequest.setHeader(namespaceListReqHeader.build());

        try {

                outbound.put(namespaceListRequest.build());

        } catch (InterruptedException e) {
                logger.warn("Unable to deliver namespace list message, queuing "
                                + e.getMessage());
        }
        
        System.gc();
		
	}
	
	private void init() {
		// the queue to support client-side surging
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		// Configure the client.
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(
				Executors.newCachedThreadPool(),
				Executors.newCachedThreadPool()));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		clientPipeline = new ClientDecoderPipeline();
		bootstrap.setPipelineFactory(clientPipeline);
		channel = connect();
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
			channeluture = bootstrap.connect(new InetSocketAddress(host, port));

			// cleanup on lost connection

		}

		// wait for the connection to establish
		channeluture.awaitUninterruptibly();

		if (channeluture.isDone() && channeluture.isSuccess())
			return channeluture.getChannel();
		else
			throw new RuntimeException(
					"Not able to establish connection to server");
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
		int retry = 0;

		public OutboundWorker(ClientConnection conn) {
			this.conn = conn;

			if (conn.outbound == null)
				throw new RuntimeException(
						"connection worker detected null queue");
		}

		@Override
		public void run() {

			Channel ch = conn.getChannel();

			if (ch == null || !ch.isOpen()) {
				ClientConnection.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {

				if (!forever && conn.outbound.size() == 0) {

					try {

						retry++;
						if (retry <= 5) {
							Thread.sleep(100);
							continue;
						} else {
							System.out.println("Closing the channel");
							ch.close();
							bootstrap.releaseExternalResources();
						}
					} catch (InterruptedException e) {

						e.printStackTrace();
					}
				}

				try {
					// block until a message is enqueued
					GeneratedMessage msg = conn.outbound.take();
					if (ch.isWritable()) {
						// ClientHandler handler =
						// conn.connect().getPipeline().get(ClientHandler.class);
						//
						// if (!handler.send(msg))
						// conn.outbound.putFirst(msg);
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
					ClientConnection.logger.error(
							"Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				ClientConnection.logger.info("connection queue closing");
			}
		}
	}


}
