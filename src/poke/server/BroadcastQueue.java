package poke.server;

import java.net.SocketAddress;
import java.util.concurrent.LinkedBlockingDeque;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Broadcast;
import eye.Comm.Management;

/**
 * The management queue exists as an instance per process (node)
 * 
 * @author gash
 * 
 */
public class BroadcastQueue {
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected static LinkedBlockingDeque<BroadcastQueueEntry> inbound = new LinkedBlockingDeque<BroadcastQueueEntry>();
	

	// TODO static is problematic
		
	private static InboundBroadcastWorker iworker;

	// not the best method to ensure uniqueness
	private static ThreadGroup tgroup = new ThreadGroup("BroadcastQueue-"
			+ System.nanoTime());

	public static void startup(String nodeId) {
		if (iworker != null)
			return;

		iworker = new InboundBroadcastWorker(tgroup, 1);
		iworker.setNodeId(nodeId);
		iworker.start();
		
	}

	public static void shutdown(boolean hard) {
		// TODO shutdon workers
	}

	public static void enqueueRequest(Broadcast req) {
		try {
			
			BroadcastQueueEntry entry = new BroadcastQueueEntry(req.getNodeId(),req.getIpAddress(),req.getPort(),req.getMgmtPort());
			inbound.put(entry);
		} catch (InterruptedException e) {
			logger.error("message not enqueued for processing", e);
		}
	}

	
	public static class BroadcastQueueEntry {
		public BroadcastQueueEntry(String nodeId, String ipAddress, int port, int mgmtPort) {
			this.nodeId = nodeId;
			this.ipAddress = ipAddress;
			this.port = port;
			this.mgmtPort = mgmtPort;
		}

		public String nodeId;
		public String ipAddress;
		public int port;
		public int mgmtPort;
		
	}
}
