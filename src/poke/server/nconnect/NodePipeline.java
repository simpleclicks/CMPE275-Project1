package poke.server.nconnect;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.jboss.netty.handler.codec.protobuf.ProtobufDecoder;
import org.jboss.netty.handler.codec.protobuf.ProtobufEncoder;


public class NodePipeline implements ChannelPipelineFactory {
	
	private NodeResponseHandler handler;

	public NodePipeline(NodeResponseHandler handler) {
		this.handler = handler;
	}

	public ChannelPipeline getPipeline() throws Exception {
		
		ChannelPipeline pipeline = Channels.pipeline();
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(eye.Comm.Response.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());
		pipeline.addLast("handler", handler);

		return pipeline;
	}

}
