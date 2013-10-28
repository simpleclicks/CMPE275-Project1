package poke.server;

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

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.DatagramChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eye.Comm.Broadcast;

/**
 * The monitor is a client-side component to process responses from server
 * management messages/responses - heartbeats.
 * 
 * @author gash
 * 
 */
public class BroadcastConnector {
	protected static Logger logger = LoggerFactory.getLogger("server");

	protected ChannelFuture channel; // do not use directly call connect()!
	protected ConnectionlessBootstrap bootstrap;	

	// either host/port or handler, not both
	private String nodeId;
	private String hostAddress;
	private int port;
	private int mgmtPort;
	private int bport;

	/**
	 * most applications will supply a handler to process messages. This is the
	 * prefered constructor.
	 * 
	 * @param handler
	 * @param host
	 *            the hostname
	 * @param port
	 *            This is the management port
	 */
	public BroadcastConnector(String nodeId, String hostAddress, int port, int mgmtPort, int bport) {
		this.nodeId = nodeId;
		this.hostAddress = hostAddress;
		this.port = port;
		this.mgmtPort = mgmtPort;
		this.bport = bport;

		initUDP();
	}

	protected void initUDP() {
		NioDatagramChannelFactory cf = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		
		bootstrap = new ConnectionlessBootstrap(cf);

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("keepAlive", true);
		//bootstrap.setOption("broadcast", true);
		
		// Set up the pipeline factory.		
		bootstrap.setPipelineFactory(new BroadcastPipeline());
	}

	
	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
						
			NetworkInterface ni;
			try {
				ni = NetworkInterface.getByInetAddress(InetAddress.getLocalHost());
				String broadcastAddress = ni.getInterfaceAddresses().get(0).getBroadcast().toString().substring(1);	
				logger.info("sending broadcast to " + broadcastAddress + ":" + bport);
				channel = bootstrap.connect(new InetSocketAddress(broadcastAddress, bport));
				
			} catch (SocketException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// wait for the connection to establish
		//channel.awaitUninterruptibly();                       // No need to connect every time n why await... is outside the if?

		if (channel.isDone() && channel.isSuccess()) {
			// TODO add detection of closed channel
			return channel.getChannel();
		} else {
			channel = null;
			throw new RuntimeException("Not able to broadcast");
		}
	}

	public String getNodeInfo() {
		if (hostAddress != null)
			return hostAddress + ":" + port;
		else
			return "Unknown";
	}

	/**
	 * attempt to initialize (create) the connection to the node.
	 * 
	 * @return did a connect and message succeed
	 */
	public boolean initiateBroadcast() {
		// the join will initiate the other node's hbMgr to reply to
		// this node's (caller) listeners.

		boolean rtn = false;
		try {
			
			logger.info("BroadcastConnector: Broadcasting Availability ");
			Channel ch = connect();
			Broadcast.Builder b = Broadcast.newBuilder();
			b.setNodeId(this.nodeId);
			b.setIpAddress(this.hostAddress);
			b.setPort(this.port);
			b.setMgmtPort(this.mgmtPort);
			ch.write(b.build());
			rtn = true;
		} catch (Exception e) {
			e.printStackTrace();
			logger.info("BroadcastConnector: Broadcasting Error ");
		}

		return rtn;
	}

	public String getHostAddress() {
		return hostAddress;
	}

	public int getPort() {
		return port;
	}	
}

