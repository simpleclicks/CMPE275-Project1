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
package poke.monitor;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.bootstrap.ConnectionlessBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioDatagramChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.server.Server;
import poke.server.management.HeartbeatData;
import poke.server.management.HeartbeatManager;
import eye.Comm.Management;
import eye.Comm.Network;
import eye.Comm.Network.Action;

/**
 * The monitor is a client-side component to process responses from server
 * management messages/responses - heartbeats.
 * 
 * @author gash
 * 
 */
public class HeartMonitor {
	protected static Logger logger = LoggerFactory.getLogger("monitor");

	protected ChannelFuture channel; // do not use directly call connect()!
	protected ClientBootstrap bootstrap;

	// either host/port or handler, not both
	private String host;
	private int port;
	private MonitorHandler handler;
	private int retry;
	private String nodeId;
	
	private boolean isExternal = false;

	// protected ChannelFactory cf;

	public boolean isExternal() {
		return isExternal;
	}

	public void setExternal(boolean isExternal) {
		this.isExternal = isExternal;
	}

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
	public HeartMonitor(String host, int port, MonitorHandler handler, String nodeId) {
		this.handler = handler;
		this.host = host;
		this.port = port;
		this.nodeId = nodeId;

		retry = 0;
		initTCP();
	}

	public String getNodeId() {
		return nodeId;
	}

	/**
	 * this is used for demonstrations as it creates a handler to print hbMgr
	 * messages.
	 * 
	 * @param host
	 *            the hostname
	 * @param port
	 *            This is the management port
	 */
	protected HeartMonitor(String host, int port) {
		this.host = host;
		this.port = port;

		initTCP();
	}

	public MonitorHandler getHandler() {
		return handler;
	}

	public void release() {
		// TODO implement behavior to drop listeners and connection

		// if (cf != null)
		// cf.releaseExternalResources();
	}

	protected void initUDP() {
		NioDatagramChannelFactory cf = new NioDatagramChannelFactory(Executors.newCachedThreadPool());
		ConnectionlessBootstrap bootstrap = new ConnectionlessBootstrap(cf);

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("keepAlive", true);

		// Set up the pipeline factory.
		if (handler != null) {
			HeartPrintListener print = new HeartPrintListener(host + ":" + port);
			handler = new MonitorHandler();
			handler.addListener(print);
		}
		bootstrap.setPipelineFactory(new MonitorPipeline(handler));
	}



	protected void initTCP() {
		bootstrap = new ClientBootstrap(new NioClientSocketChannelFactory(Executors.newCachedThreadPool(),
				Executors.newFixedThreadPool(2)));

		bootstrap.setOption("connectTimeoutMillis", 10000);
		bootstrap.setOption("tcpNoDelay", true);
		bootstrap.setOption("keepAlive", true);

		// case of a demo code
		if (handler == null) {
			HeartPrintListener print = new HeartPrintListener(host + ":" + port);
			handler = new MonitorHandler();
			handler.addListener(print);
		}
		bootstrap.setPipelineFactory(new MonitorPipeline(handler));
	}

	/**
	 * create connection to remote server
	 * 
	 * @return
	 */
	protected Channel connect() {
		// Start the connection attempt.
		if (channel == null) {
			logger.info("connecting to " + host + ":" + port);
			channel = bootstrap.connect(new InetSocketAddress(host, port));
		}

		// wait for the connection to establish
		channel.awaitUninterruptibly();                       // No need to connect every time n why await... is outside the if?

		if (channel.isDone() && channel.isSuccess()) {
			// TODO add detection of closed channel
			return channel.getChannel();
		} else {
			channel = null;
			throw new RuntimeException("Not able to establish connection to server");
		}
	}

	public boolean isConnected() {
		if (channel == null)
			return false;
		else
		{	
			//logger.info("HeartMonitor is connector called "+ (channel.getChannel().isOpen() && channel.getChannel().isWritable()));
		return (channel.getChannel().isOpen() && channel.getChannel().isWritable());
		}
	}

	public String getNodeInfo() {
		if (host != null)
			return host + ":" + port;
		else
			return "Unknown";
	}

	/**
	 * attempt to initialize (create) the connection to the node.
	 * 
	 * @return did a connect and message succeed
	 * @throws Exception 
	 */
	public boolean initiateHeartbeat() throws Exception {
		// the join will initiate the other node's hbMgr to reply to
		// this node's (caller) listeners.

		boolean rtn = false;

		
		if(retry < 5)
		{
			try {

				//System.out.println("Tracing code flow 1 : HeartMonitor.java intiateHeartbeat");
				Channel ch = connect();
				Network.Builder n = Network.newBuilder();
				n.setOriginId(HeartbeatManager.getInstance().getNodeId());
				n.setNodeId(HeartbeatManager.getInstance().getNodeId());
				n.setAction(Action.NODEJOIN);
				Management.Builder m = Management.newBuilder();
				m.setIsExternal(isExternal);
				m.setGraph(n.build());
				ch.write(m.build());
				rtn = true;
			}
			catch(Exception e) {
				logger.info("Cannot send NODEJOIN connect to node");
				retry++;
			}
		}
		else {
			throw new Exception();
		}
		
		return rtn;
			
		
	}

	public String getHost() {
		return host;
	}

	public int getPort() {
		return port;
	}

	/**
	 * for demonstration - this will enter a loop waiting for hbMgr messages.
	 * 
	 * Note this will return if the node is not available.
	 */
	protected void waitForever() {
		try {
			boolean connected = initiateHeartbeat();
			while (connected) {
				Thread.sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String host = "localhost";
		int mport = 5670;

		if (args.length == 2) {
			try {
				host = args[0];
				mport = Integer.parseInt(args[1]);
			} catch (NumberFormatException e) {
				logger.warn("Unable to set port numbes, using default: 5670/5680");
			}
		}

		logger.info("trying to connect monitor to " + host + ":" + mport);
		HeartMonitor hm = new HeartMonitor(host, mport);
		hm.waitForever();
	}
}
