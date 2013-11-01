package poke.server.nconnect;

import java.io.File;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.GeneratedMessage;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.Management;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import poke.client.ClientConnection;
import poke.client.ClientResponseAction;
import poke.server.management.HeartbeatManager;
import poke.server.management.ManagementQueue;
import poke.server.management.ManagementQueue.ManagementQueueEntry;

public class NodeClient {

	protected static Logger logger = LoggerFactory.getLogger("NodeClient");

	protected ChannelFuture channelFuture;

	protected Channel channel; 

	protected ClientBootstrap bootstrap;

	private String host;

	private int port;

	private String nodeId;

	private NodeResponseHandler handler;

	private OutboundWorker requestSender;

	private InboundWorker responseProcessor;

	private  LinkedBlockingDeque<Response> inboundResponseQueue = new LinkedBlockingDeque<Response>();

	private  LinkedBlockingDeque<Request> outboundRequestQueue = new LinkedBlockingDeque<Request>();

	private ConcurrentHashMap<String, String> docQueryResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, List<Document>> listFiles = new ConcurrentHashMap<String, List<Document>>(); 


	public NodeClient(String host, int port, String nodeId) {
		super();
		this.host = host;
		this.port = port;
		this.nodeId = nodeId;
		requestSender = new OutboundWorker(this);
		responseProcessor = new InboundWorker(this);
		initTCP();
	}

	private void initTCP() {

		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newFixedThreadPool(2)));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);


		if (handler == null) {

			handler = new NodeResponseHandler(this);
		}

		bootstrap.setPipelineFactory(new NodePipeline(handler));

		//connect();

		requestSender.start();

		responseProcessor.start();
	}

	private void connect() {

		try{

			if (channel == null) {
				logger.info("connecting to network node "+nodeId+" at " + host + ":" + port);
				channelFuture = bootstrap.connect(new InetSocketAddress(host, port));
				channelFuture.awaitUninterruptibly();

			}
			
		}catch(Exception btConnectExccep){

			logger.error(" Error while establishing public connection to node "+nodeId+" "+btConnectExccep.getMessage());
		}

		if (channelFuture.isDone() && channelFuture.isSuccess()) {

			channel = channelFuture.getChannel();
			channel.getCloseFuture().addListener(new NodeChannelClosedListener(this));
			logger.info("connected to network node "+nodeId+" at " + host + ":" + port+" successfully");

		} else {
			channel = null;
			channelFuture = null;
			//throw new RuntimeException("Not able to establish TCP connection to node "+nodeId);
		}
	}

	public boolean queryFile(String nameSpace, String fileName){

		Header.Builder docQueryHeaader = Header.newBuilder();

		docQueryHeaader.setRoutingId(Header.Routing.DOCQUERY);

		docQueryHeaader.setOriginator(HeartbeatManager.getInstance().getNodeId());

		Payload.Builder docPayloadBuilder = Payload.newBuilder();

		docPayloadBuilder.setDoc(Document.newBuilder().setDocName(fileName));

		if(nameSpace !=null && nameSpace.length() > 0)
			docPayloadBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace));

		Request.Builder docQueryReqBuilder = Request.newBuilder();

		docQueryReqBuilder.setHeader(docQueryHeaader.build());

		docQueryReqBuilder.setBody(docPayloadBuilder.build());

		return enqueueRequest(docQueryReqBuilder.build());

	}
	
	public boolean queryNamespace(String nameSpace){

		Header.Builder namespaceQueryHeader = Header.newBuilder();

		namespaceQueryHeader.setRoutingId(Header.Routing.NAMESPACEQUERY);

		namespaceQueryHeader.setOriginator(HeartbeatManager.getInstance().getNodeId());

		Payload.Builder docPayloadBuilder = Payload.newBuilder();

		//docPayloadBuilder.setDoc(Document.newBuilder().setDocName(fileName));

		if(nameSpace !=null && nameSpace.length() > 0)
			docPayloadBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace));

		Request.Builder namespaceQueryReqBuilder = Request.newBuilder();

		namespaceQueryReqBuilder.setHeader(namespaceQueryHeader.build());

		namespaceQueryReqBuilder.setBody(docPayloadBuilder.build());

		return enqueueRequest(namespaceQueryReqBuilder.build());

	}
	
	public boolean queryNamespaceList(String nameSpace) {
		// TODO Auto-generated method stub

		Header.Builder namespaceListQueryHeader = Header.newBuilder();

		namespaceListQueryHeader.setRoutingId(Header.Routing.NAMESPACELISTQUERY);

		namespaceListQueryHeader.setOriginator(HeartbeatManager.getInstance().getNodeId());

		Payload.Builder namespacePayloadBuilder = Payload.newBuilder();

		if(nameSpace !=null && nameSpace.length() > 0)
			namespacePayloadBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace));

		Request.Builder namespaceListQueryReqBuilder = Request.newBuilder();

		namespaceListQueryReqBuilder.setHeader(namespaceListQueryHeader.build());

		namespaceListQueryReqBuilder.setBody(namespacePayloadBuilder.build());

		return enqueueRequest(namespaceListQueryReqBuilder.build());
		
	}
	
	public String checkDocQueryResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(docQueryResponseQueue.containsKey(key)){
			
			return docQueryResponseQueue.get(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public List checkNamespaceList(String namespace){
		String key = namespace;

        String noResult = "NA";

        if (listFiles.containsKey(key)) {

                return listFiles.get(key);

        } else {
        		
                return null;
        }
	}
	
	public List sendNamespaceList(String namespace) {
		
		List<Document> Files = new ArrayList<Document>();
		String noResult = "NA";
		if (listFiles.containsKey(namespace)){
			Files = listFiles.get(namespace);
		}
		
    	logger.info("Files returned from sendNamespaceList " +Files);

		return Files;
		
	}
	
	
	public String getNodeId() {
		return nodeId;
	}


	private boolean enqueueRequest(Request request) {
		try {

			outboundRequestQueue.put(request);

			return true;

		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
			return false;
		}
	}

	public  void enqueueResponse(Response response) {
		try {

			inboundResponseQueue.put(response);

		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	public static class NodeChannelClosedListener implements ChannelFutureListener {


		NodeClient nodeConnect;

		public NodeChannelClosedListener(NodeClient nodeConnect) {

			this.nodeConnect = nodeConnect;

		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {

			logger.warn(" Public channel to node "+nodeConnect.nodeId+" has been closed");

		}

	}

	protected class OutboundWorker extends Thread {

		NodeClient conn;
		boolean forever = true;

		public OutboundWorker(NodeClient conn) {
			this.conn = conn;

			if (conn.outboundRequestQueue == null)
				throw new RuntimeException("OutboundWorker worker detected null request queue");
		}

		@Override
		public void run() {

			Channel ch = conn.channel;
			
			int retry = 0;
			
			while(true){

			if(ch == null || !ch.isConnected()){

				logger.info("Attempting to establish public TCP connection to "+nodeId);
				conn.connect();
				ch = conn.channel;
				
				if (ch == null || !ch.isOpen()) {
					logger.error("connection missing, no outbound public communication with node "+nodeId);
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					continue;
				}else
					break;
			}
		
		}// end of connect while-true

			while (true) {

				if (!forever && conn.outboundRequestQueue.size() == 0){

					try {

						retry++;
						if(retry <=5){
							Thread.sleep(1000);
							continue;
						}
						else{
							System.out.println("Closing the public channel for nodeId "+conn.nodeId);
							ch.close();
							bootstrap.releaseExternalResources();
						}
					} catch (InterruptedException e) {

						e.printStackTrace();
					}
				}

				try {

					Request msg = conn.outboundRequestQueue.take();
					logger.info("Message is in the outbound queue: Node client");
					if (ch.isWritable()) {
						logger.info("Sending request to the nodeId "+nodeId);
						ChannelFuture cf = ch.write(msg);
						if (cf.isDone() && !cf.isSuccess()) {
							logger.error("failed to send request to nodeId "+nodeId);
							conn.outboundRequestQueue.putFirst(msg);
						}



					} else{
						conn.outboundRequestQueue.putFirst(msg);
						logger.error("Channel to nodeId "+nodeId+" is not writable");
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					logger.error("Unexpected communcation failure with nodeId "+nodeId, e);
					break;
				}
			}

			if (!forever) {
				logger.info("Shutting down the public connection to nodeId "+nodeId);
				ch.close();
				bootstrap.releaseExternalResources();
			}
		}
	}

	protected class InboundWorker extends Thread {

		boolean forever = true;

		int retry = 0;

		NodeClient owner;

		public InboundWorker(NodeClient processResponse) {

			owner= processResponse;
			if (owner.inboundResponseQueue == null)
				throw new RuntimeException("InboundWorker worker detected null response queue");
		}

		@Override
		public void run() {

			while (true) {

				if (!forever && owner.inboundResponseQueue.size() == 0){

					break;
				}

				try {

					eye.Comm.Response msg = owner.inboundResponseQueue.take();

					if (msg.getHeader().getRoutingId() == Header.Routing.DOCQUERY) {

						logger.info("Recieved the response to DOCQUERY from the Node "+owner.nodeId);

						String msgKey = null;

						PayloadReply response =  msg.getBody();

						String nameSpace = null;

						if(response.getSpaces(0) !=null)
							nameSpace = response.getSpaces(0).getName();

						String docName = response.getDocs(0).getDocName();

						if( docName == null || docName.length() ==0 )
						{
							logger.error("Invalid DocQueryResponse from node "+owner.nodeId+" document name missing");
							continue;
						}

						if(nameSpace !=null && nameSpace.length() >0)
							msgKey = nameSpace;

						msgKey=msgKey+docName;

						owner.docQueryResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCADD){

						System.out.println("ClientResponseHandler: Recieved the response to doccADD from the server and the response is "+msg.getHeader().getReplyCode()+" with Message fom server as "+msg.getHeader().getReplyMsg());

						if(msg.getHeader().getReplyCode() == Header.ReplyStatus.SUCCESS){


						}

					}
					
					else if(msg.getHeader().getRoutingId() == Header.Routing.NAMESPACEQUERY){

						System.out.println("NodeClientResponseHandler: Recieved the response to namespaceQuery from the server and the response is "+msg.getHeader().getReplyCode()+" with Message fom server as "+msg.getHeader().getReplyMsg());

						if(msg.getHeader().getReplyCode() == Header.ReplyStatus.SUCCESS){
							logger.info("Namespace removed from node "+ owner.getNodeId());

						}

					}
					
					else if(msg.getHeader().getRoutingId() == Header.Routing.NAMESPACELISTQUERY){
						String namespace = null;
						System.out.println("NodeClientResponseHandler:");
						PayloadReply response =  msg.getBody();
						System.out.println("NodeClientResponseHandler: Recieved the response to namespaceListQuery from the server and the response is "+msg.getHeader().getReplyCode()+" with Message fom server as "+msg.getHeader().getReplyMsg());

						if(msg.getHeader().getReplyCode() == Header.ReplyStatus.SUCCESS){
							logger.info("inside if ");
							namespace = response.getSpaces(0).getName();
							logger.info("namespace " + namespace);
						//	listFiles = msg.getBody().getDocsList();
							owner.listFiles.put(namespace, msg.getBody().getDocsList());
							logger.info("Document list recieved from node "+ owner.getNodeId());
						}

					}

				} catch (InterruptedException ie) {
					logger.error("InboundWorker has been interrupted "+ie.getMessage());
					break;
				} catch (Exception e) {
					logger.error("InboundWorker has generic exception ", e.getMessage());
					e.printStackTrace();
				}
			}

			if (!forever) {
				logger.info("connection queue closing");
			}
		}
	}

	
}
