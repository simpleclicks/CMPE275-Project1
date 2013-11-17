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
package poke.server.queue;

import java.io.File;
import java.io.FileInputStream;
import java.lang.Thread.State;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.resources.ChunkedResource;
import poke.server.resources.Resource;
import poke.server.resources.ResourceFactory;
import poke.server.resources.ResourceUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.GeneratedMessage;

import eye.Comm.Header.ReplyStatus;
import eye.Comm.Header.Routing;
import eye.Comm.Document;
import eye.Comm.NameSpace;
import eye.Comm.Payload;
import eye.Comm.PayloadReply;
import eye.Comm.Request;
import eye.Comm.Response;

/**
 * A server queue exists for each connection (channel). A per-channel queue
 * isolates clients. However, with a per-client model. The server is required to
 * use a master scheduler/coordinator to span all queues to enact a QoS policy.
 * 
 * How well does the per-channel work when we think about a case where 1000+
 * connections?
 * 
 * @author gash
 * 
 */
public class PerChannelQueue implements ChannelQueue {
	protected static Logger logger = LoggerFactory.getLogger("server");
	
	private static final String HOMEDIR = "home";

	private Channel channel;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> inbound;
	private LinkedBlockingDeque<com.google.protobuf.GeneratedMessage> outbound;
	private OutboundWorker oworker;
	private InboundWorker iworker;
	private static final int MAX_UNCHUNKED_FILE_SIZE = 26214400;
	
	// not the best method to ensure uniqueness
	private ThreadGroup tgroup = new ThreadGroup("ServerQueue-" + System.nanoTime());

	protected PerChannelQueue(Channel channel) {
		this.channel = channel;
		init();
	}

	protected void init() {
		inbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();
		outbound = new LinkedBlockingDeque<com.google.protobuf.GeneratedMessage>();

		iworker = new InboundWorker(tgroup, 1, this);
		iworker.start();

		oworker = new OutboundWorker(tgroup, 1, this);
		oworker.start();

		// let the handler manage the queue's shutdown
		// register listener to receive closing of channel
		// channel.getCloseFuture().addListener(new CloseListener(this));
	}

	protected Channel getChannel() {
		return channel;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#shutdown(boolean)
	 */
	@Override
	public void shutdown(boolean hard) {
		logger.info("server is shutting down");

		channel = null;

		if (hard) {
			// drain queues, don't allow graceful completion
			inbound.clear();
			outbound.clear();
		}

		if (iworker != null) {
			iworker.forever = false;
			if (iworker.getState() == State.BLOCKED || iworker.getState() == State.WAITING)
				iworker.interrupt();
			iworker = null;
		}

		if (oworker != null) {
			oworker.forever = false;
			if (oworker.getState() == State.BLOCKED || oworker.getState() == State.WAITING)
				oworker.interrupt();
			oworker = null;
		}

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueRequest(eye.Comm.Finger)
	 */
	@Override
	public void enqueueRequest(Request req) {
		try {
			inbound.put(req);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.server.ChannelQueue#enqueueResponse(eye.Comm.Response)
	 */
	@Override
	public void enqueueResponse(Response reply) {
		if (reply == null)
			return;

		try {
			outbound.put(reply);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for reply", e);
		}
	}

	protected class OutboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public OutboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "outbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (outbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			System.out.println("PerChannelQueue.OutboundWorker.run()");
			Channel conn = sq.channel;
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no outbound communication");
				return;
			}

			while (true) {
				if (!forever && sq.outbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.outbound.take();
					if (conn.isWritable()) {
						boolean rtn = false;
						if (channel != null && channel.isOpen() && channel.isWritable()) {
							ChannelFuture cf = channel.write(msg);
							
							// blocks on write - use listener to be async
							cf.awaitUninterruptibly();
							rtn = cf.isSuccess();
							if (!rtn)
								sq.outbound.putFirst(msg);
						}

					} else
						sq.outbound.putFirst(msg);
				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected communcation failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	protected class InboundWorker extends Thread {
		int workerId;
		PerChannelQueue sq;
		boolean forever = true;

		public InboundWorker(ThreadGroup tgrp, int workerId, PerChannelQueue sq) {
			super(tgrp, "inbound-" + workerId);
			this.workerId = workerId;
			this.sq = sq;

			if (inbound == null)
				throw new RuntimeException("connection worker detected null queue");
		}

		@Override
		public void run() {
			Channel conn = sq.channel;
			if (conn == null || !conn.isOpen()) {
				PerChannelQueue.logger.error("connection missing, no inbound communication");
				return;
			}

			while (true) {
				if (!forever && sq.inbound.size() == 0)
					break;

				try {
					// block until a message is enqueued
					GeneratedMessage msg = sq.inbound.take();

					// process request and enqueue response
					if (msg instanceof Request) {
						Request req = ((Request) msg);
						if (req == null)
							logger.error("req is null");
						// do we need to route the request?

						// handle it locally
						ChunkedResource crsc = null;
						Resource rsc = null;
						if(req.getHeader().getRoutingId() == Routing.DOCFIND){
							crsc = ResourceFactory.getInstance().chunkedResourceInstance(req.getHeader());
						}
						else {
							rsc = ResourceFactory.getInstance().resourceInstance(req.getHeader());
						}						

						Response reply = null;
						
						List<Response> responses;
						if (rsc == null && crsc == null) {
							logger.error("failed to obtain resource for " + req);
							reply = ResourceUtil.buildError(req.getHeader(), ReplyStatus.FAILURE,
									"Request not processed");
						} else if(req.getHeader().getRoutingId() == Routing.DOCFIND ){
							responses = crsc.process(req);
							if(responses.get(0).getHeader().getReplyCode() == ReplyStatus.SUCCESS){
								String fname = responses.get(0).getBody().getDocs(0).getDocName();
								String namespaceName = responses.get(0).getBody().getSpaces(0).getName();
								String fileExt = FilenameUtils.getExtension(fname);
								String respFname = FilenameUtils.getBaseName(fname) + fileExt;

								java.io.File file = FileUtils.getFile(fname);

								long fileSize = FileUtils.sizeOf(file);

								logger.info("Size of the file to be sent " + fileSize);

								long totalChunk = ((fileSize / MAX_UNCHUNKED_FILE_SIZE)) + 1;

								for (Response response : responses) {
									FileInputStream chunkeFIS = new FileInputStream(new File(fname));
									if(response.getHeader().getReplyCode() == ReplyStatus.SUCCESS){
										if(response.getBody().getDocs(0).getTotalChunk() <= 1){
											sq.enqueueResponse(response);
										}
										else{
											Response.Builder respEnqueue = Response.newBuilder();
											//TODO read each chunk from the file and enqueue
											int bytesRead = 26214400;
											int bytesActuallyRead = 0;
											long offset = 0;
											 if(response.getBody().getDocs(0).getChunkId() > 1 ){
												 offset = ((response.getBody().getDocs(0).getChunkId()-1) * bytesRead);
												 chunkeFIS.skip(offset);
											 }
												byte[] chunckContents = new byte[26214400];
												
												if(chunkeFIS.available() < 26214400){
													bytesRead = chunkeFIS.available();
													chunckContents = new byte[chunkeFIS.available()];
												}

												bytesActuallyRead = IOUtils.read(chunkeFIS, chunckContents, 0, bytesRead);
											
											/*int chunkId = (int) response.getBody().getDocs(0).getChunkId();
											
											int bytesOffset = 0;
											
											if(chunkId == 0){
												bytesOffset = 0;
											}
											else if(chunkId < response.getBody().getDocs(0).getTotalChunk()){
												
												bytesOffset = (int) (response.getBody().getDocs(0).getChunkId() - 1);
										
												//chunkId = ((int) response.getBody().getDocs(0).getChunkId())+1;
											}
											
											int bytesRead = 0;

											//FileInputStream chunkFIS = new FileInputStream(file);
											
											//int bytesRemaining = chunkFIS.available();
											
											chunkFIS.skip(bytesOffset* MAX_UNCHUNKED_FILE_SIZE);
											
											int bytesRemaining = chunkFIS.available();

												byte[] chunckContents = null; 

												if(bytesRemaining > MAX_UNCHUNKED_FILE_SIZE){

													chunckContents = new byte[MAX_UNCHUNKED_FILE_SIZE];

													bytesRead= IOUtils.read(chunkFIS, chunckContents , 0 , MAX_UNCHUNKED_FILE_SIZE);
												}
												else {
													chunckContents = new byte[bytesRemaining];

													//System.out.println("length of last chunk content "+chunckContents.length);

													bytesRead= IOUtils.read(chunkFIS, chunckContents , 0 , bytesRemaining);

												}*/

												logger.info("CHUNKED Contents of the chunk "+response.getBody().getDocs(0).getChunkId()+" : "+chunckContents);
												
												respEnqueue.setHeader(response.getHeader());
												PayloadReply.Builder respEnqueuePL = PayloadReply.newBuilder();
												
												respEnqueuePL.addDocsBuilder();
												respEnqueuePL.addSpacesBuilder();
												respEnqueuePL.setSpaces(0, NameSpace.newBuilder().setName(namespaceName));
												respEnqueuePL.setDocs(0,Document
														.newBuilder()
														.setDocName(respFname)
														.setDocExtension(fileExt)
														.setDocSize(fileSize).setTotalChunk(totalChunk)
														.setChunkContent(ByteString.copyFrom(chunckContents))
														.setChunkId(response.getBody().getDocs(0).getChunkId()));
												
												respEnqueue.setBody(respEnqueuePL.build());

												chunckContents = null;

												System.gc();

												//chunkId++;
												
												logger.info("CHUNKED enqueuing response for client request for : " + response.getBody().getDocs(0).getChunkId());
												Response tbE = respEnqueue.build();
												sq.enqueueResponse(tbE);
												
												Thread.sleep(3000);
										}
									}
									else{
										sq.enqueueResponse(response);
									}
									chunkeFIS.close();
								}
								
							}
							else
								sq.enqueueResponse(responses.get(0));
							
						}
						else {
							reply = rsc.process(req);
							sq.enqueueResponse(reply);
						}
					}

				} catch (InterruptedException ie) {
					break;
				} catch (Exception e) {
					PerChannelQueue.logger.error("Unexpected processing failure", e);
					break;
				}
			}

			if (!forever) {
				PerChannelQueue.logger.info("connection queue closing");
			}
		}
	}

	public class CloseListener implements ChannelFutureListener {
		private ChannelQueue sq;

		public CloseListener(ChannelQueue sq) {
			this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			sq.shutdown(true);
		}
	}
}
