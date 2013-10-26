package poke.server.nconnect;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Response;


public class NodeResponseHandler  extends SimpleChannelUpstreamHandler {
	
	protected static Logger logger = LoggerFactory.getLogger("NodeResponseHandler");
	
	NodeClient owner;
	
	public NodeResponseHandler(NodeClient owner){
		
		this.owner = owner;
	}
	
	@Override
	public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) {
		
		Response res = (Response)e.getMessage();
		
		this.owner.enqueueResponse(res);
		
		logger.info("Message has been received from the node "+res.getHeader().getOriginator()+" for "+res.getHeader().getRoutingId()+" as "+res.getHeader().getReplyMsg() );
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
		
		logger.error("Exception in Handler: " + e.getCause());
	
		logger.error("Closing channel : " + e.getCause());
		
		e.getChannel().close();
	}

}
