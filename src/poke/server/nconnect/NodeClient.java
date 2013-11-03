package poke.server.nconnect;

import java.net.InetSocketAddress;
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

import com.google.protobuf.ByteString;

import eye.Comm.Document;
import eye.Comm.Header;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;
import poke.server.management.HeartbeatManager;
/**
 * @author Kaustubh
 * @version 1.8
 * {@code : Establishes TCP on public port of the active nodes in the network}
 * {@code : Forwards request to corresponding node and stores with the response}
 *
 */

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
	
	private ConcurrentHashMap<String, String> docRemoveResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, String> replicaRemoveResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, String> docAddHSResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, String> docAddResponseQueue =  new ConcurrentHashMap<String, String>();

	private ConcurrentHashMap<String, String> replicaHSResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, String> addReplicaResponseQueue =  new ConcurrentHashMap<String, String>();
	
	private ConcurrentHashMap<String, String> queryReplicaResponseQueue =  new ConcurrentHashMap<String, String>();
	
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

		Request docQueryRequest = createRequest(nameSpace , fileName , Header.Routing.DOCQUERY ,0 ,0 ,0 , null);

		return enqueueRequest(docQueryRequest);

	}
	
	public boolean queryReplica(String nameSpace, String fileName){

		Request replicaQueryRequest = createRequest(nameSpace , fileName , Header.Routing.REPLICAQUERY ,0 ,0 ,0 , null);

		return enqueueRequest(replicaQueryRequest);
	}
	
	
	public boolean removeDoc(String nameSpace , String fileName){
		
		Request docRemoveRequest = createRequest(nameSpace , fileName , Header.Routing.DOCREMOVE,0,0,0 , null);
		
		return enqueueRequest(docRemoveRequest);
	}
	
	public boolean docAddHandshake(String nameSpace , String fileName , long size){
		
		Request docAddHandshakeRequest = createRequest(nameSpace , fileName , Header.Routing.DOCADDHANDSHAKE , size , 0 ,0,null);
		
		return enqueueRequest(docAddHandshakeRequest);
	}
	
	public boolean removeReplica(String nameSpace , String fileName){
		
		Request replicaRemoveRequest = createRequest(nameSpace , fileName , Header.Routing.REPLICAREMOVE , 0 ,0 ,0 , null);
		
		return enqueueRequest(replicaRemoveRequest);
	}
	
	public boolean docAdd(Header docAddHeader , Payload body){
		
		docAddHeader = docAddHeader.toBuilder().setOriginator(nodeId).build();
		
		Request docAddReq = Request.newBuilder().setBody(body).setHeader(docAddHeader).build();
		
		return enqueueRequest(docAddReq);
	}
	
	public boolean replicaHandshake(String nameSpace , String fileName , long size){
		
		Request docAddHandshakeRequest = createRequest(nameSpace , fileName , Header.Routing.REPLICAHANDSHAKE , size , 0 ,0 , null);
		
		return enqueueRequest(docAddHandshakeRequest);
	}
	
	public boolean addReplica(String nameSpace , String fileName , byte[] chunkContents , int totalChunks , int chunkId ){
		
		Request addReplicaRequest = createRequest(nameSpace , fileName , Header.Routing.ADDREPLICA , chunkContents.length , totalChunks ,chunkId , chunkContents );
		
		return enqueueRequest(addReplicaRequest);
	}
	
	private Request createRequest(String nameSpace , String fileName , Header.Routing action , long docSize , int totalChunks , int chunkId , byte[] chunkContents){
		
		Header.Builder docQueryHeaader = Header.newBuilder();

		docQueryHeaader.setRoutingId(action);

		docQueryHeaader.setOriginator(HeartbeatManager.getInstance().getNodeId()); 

		Payload.Builder docPayloadBuilder = Payload.newBuilder();

		if(chunkContents !=null && chunkContents.length > 0){
			logger.info("setting the chunk contents to be sent to "+nodeId+" as a part of "+nameSpace+fileName);
			docPayloadBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocSize(docSize).setTotalChunk(totalChunks).setChunkId(chunkId).setChunkContent(ByteString.copyFrom(chunkContents)));
		}
		else
		docPayloadBuilder.setDoc(Document.newBuilder().setDocName(fileName).setDocSize(docSize).setTotalChunk(totalChunks).setChunkId(chunkId));

		if(nameSpace !=null && nameSpace.length() > 0)
			docPayloadBuilder.setSpace(NameSpace.newBuilder().setName(nameSpace));

		Request.Builder docQueryReqBuilder = Request.newBuilder();

		docQueryReqBuilder.setHeader(docQueryHeaader.build());

		docQueryReqBuilder.setBody(docPayloadBuilder.build());
		
		return docQueryReqBuilder.build();
		
	}
	
	public String checkDocADDHSResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(docAddHSResponseQueue.containsKey(key)){
			
			return docAddHSResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkDocAddResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(docAddResponseQueue.containsKey(key)){
			
			return docAddResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkDocQueryResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(docQueryResponseQueue.containsKey(key)){
			
			return docQueryResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkReplicaQueryResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		logger.info(" Key for checking replicaQuery response "+key);
		
		if(queryReplicaResponseQueue.containsKey(key)){
			
			return queryReplicaResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkDocRemoveResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(docRemoveResponseQueue.containsKey(key)){
			
			return docRemoveResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkReplicaRemoveResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		
		String noResult = "NA";
		
		if(replicaRemoveResponseQueue.containsKey(key)){
			
			return replicaRemoveResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkAddReplicaResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		logger.info(" Key for checkAddReplicaResponse "+key);
		
		String noResult = "NA";
		
		if(addReplicaResponseQueue.containsKey(key)){
			
			return addReplicaResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	
	public String checkReplicaHSResponse(String nameSpace , String fileName){
		
		String key = nameSpace+fileName;
		logger.info(" Key for checkReplicaHSResponse "+key);
		
		String noResult = "NA";
		
		if(replicaHSResponseQueue.containsKey(key)){
			
			return replicaHSResponseQueue.remove(key);
					
		}else{
			
			return noResult;
		}
	}
	

	public String getNodeId() {
		return nodeId;
	}


	private boolean enqueueRequest(Request request) {
		try {

			outboundRequestQueue.put(request);

			return true;

		} catch (InterruptedException e) {
			logger.error("message not enqueued for transmitting ", e.getMessage());
			return false;
		}
	}

	public  void enqueueResponse(Response response) {
		try {

			inboundResponseQueue.put(response);

		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing ", e.getMessage());
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

					if (ch.isWritable()) {
						logger.info("Sending request to the nodeId "+nodeId);
						ChannelFuture cf = ch.write(msg);
						if (cf.isDone() && !cf.isSuccess()) {
							logger.error("failed to send request to nodeId "+nodeId);
							conn.outboundRequestQueue.putFirst(msg);
						}

						System.gc();

					} else{
						conn.outboundRequestQueue.putFirst(msg);
						logger.error("Channel to nodeId "+nodeId+" is not writable");
					}
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					logger.error("Unexpected communcation failure with nodeId "+nodeId, e.getMessage());
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

					System.gc();
					
					eye.Comm.Response msg = owner.inboundResponseQueue.take();
					
					if (msg.getHeader().getRoutingId() == Header.Routing.DOCQUERY) {

						String msgKey = createKey(msg.getBody());
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid DocQueryResponse from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.docQueryResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCREMOVE){

						String msgKey = createKey(msg.getBody());
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid DocRemoveResponse from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.docRemoveResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.REPLICAREMOVE){

						String msgKey = createKey(msg.getBody());
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid ReplicaRemoveResponse from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.replicaRemoveResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCADDHANDSHAKE){

						String msgKey = createKey(msg.getBody());
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid DOCADDHANDSHAKE Response from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.docAddHSResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.DOCADD){

						String msgKey = createKey(msg.getBody());
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid DOCADD Response from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.docAddResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );

					}else if(msg.getHeader().getRoutingId() == Header.Routing.REPLICAHANDSHAKE){

						String msgKey = createKey(msg.getBody());
						logger.info(" msgKey for REPLICAHANDSHAKE "+msgKey);						
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid REPLICAHANDSHAKE Response from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.replicaHSResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );
					}else if(msg.getHeader().getRoutingId() == Header.Routing.ADDREPLICA){

						String msgKey = createKey(msg.getBody());
						logger.info(" msgKey for ADDREPLICA "+msgKey);						
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid ADDREPLICA Response from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.addReplicaResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );
					}if(msg.getHeader().getRoutingId() == Header.Routing.REPLICAQUERY){

						String msgKey = createKey(msg.getBody());
						logger.info(" msgKey for replicaQuery "+msgKey);						
						
						if(msgKey.equalsIgnoreCase("Invalid")){
							logger.error("Invalid REPLICAQUERY Response from node "+owner.nodeId+" document name missing");
							continue;
						}

						owner.queryReplicaResponseQueue.put(msgKey, msg.getHeader().getReplyCode().name() );
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
		
		private String createKey(PayloadReply body){
			
			String msgKey = "";

			String nameSpace = null;

			if(body.getSpacesCount() > 0)
				nameSpace = body.getSpaces(0).getName();

			String docName = body.getDocs(0).getDocName();

			if( docName == null || docName.length() ==0 )
					return "invalid";

			if(nameSpace !=null && nameSpace.length() >0)
				msgKey = nameSpace;

			msgKey=msgKey+docName;
			
			return msgKey;
		}
	}
}