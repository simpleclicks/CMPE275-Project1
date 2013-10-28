package poke.server;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BroadcastHandler extends SimpleChannelUpstreamHandler {
	protected static Logger logger = LoggerFactory.getLogger("server");

	public BroadcastHandler() {
		// logger.info("** HeartbeatHandler created **");
	}

	/**
	 * override this method to provide processing behavior
	 * 
	 * @param msg
	 */
	public void handleMessage(eye.Comm.Broadcast req, Channel channel) {
		if (req == null) {
			logger.error("ERROR: Unexpected content - null");
			return;
		}

		logger.info("BroadcastHandler got messsage");
		// ManagementQueue.enqueueRequest(req, channel);
	}

	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		logger.info("broadcast rcv: " + e.getRemoteAddress());
		BroadcastQueue.enqueueRequest((eye.Comm.Broadcast) e.getMessage());							

		// handleMessage((eye.Comm.Management) e.getMessage(), e.getChannel());
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		logger.error(
				"BroadcastHandler error, closing channel, reason: "
						+ e.getCause(), e);
		e.getCause().printStackTrace();
		e.getChannel().close();
	}

	/**
	 * usage:
	 * 
	 * <pre>
	 * channel.getCloseFuture().addListener(new BroadcastClosedListener(queue));
	 * </pre>
	 * 
	 * @author gash
	 * 
	 */
	public static class BroadcastClosedListener implements
			ChannelFutureListener {
		// private ManagementQueue sq;

		public BroadcastClosedListener(BroadcastQueue sq) {
			// this.sq = sq;
		}

		@Override
		public void operationComplete(ChannelFuture future) throws Exception {
			// if (sq != null)
			// sq.shutdown(true);
			// sq = null;
		}

	}
}
