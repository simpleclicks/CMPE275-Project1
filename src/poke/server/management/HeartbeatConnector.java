/*
 * copyright 2013, gash
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
package poke.server.management;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.monitor.HeartMonitor;
import poke.monitor.MonitorHandler;
import poke.server.conf.JsonUtil;
import poke.server.conf.NodeDesc;
import poke.server.conf.ServerConf;
import poke.server.management.HeartbeatData.BeatStatus;

/**
 * The connector collects connection monitors (e.g., listeners implement the
 * circuit breaker) that maintain HB communication between nodes (to
 * client/requester).
 * 
 * @author gash
 * 
 */
public class HeartbeatConnector extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("management");
	protected static AtomicReference<HeartbeatConnector> instance = new AtomicReference<HeartbeatConnector>();

	//private ConcurrentLinkedQueue<HeartMonitor> monitors = new ConcurrentLinkedQueue<HeartMonitor>();
	private ConcurrentHashMap<String, HeartMonitor> monitors = new ConcurrentHashMap<String, HeartMonitor>();
	private int sConnectRate = 2000; // msec
	private boolean forever = true;
	private ServerConf conf;
	private String confPath;

	public String getConfPath() {
		return confPath;
	}

	public void setConfPath(String confPath) {
		this.confPath = confPath;
	}

	public ServerConf getConf() {
		return conf;
	}

	public void setConf(ServerConf conf) {
		this.conf = conf;
	}

	public static HeartbeatConnector getInstance() {
		instance.compareAndSet(null, new HeartbeatConnector());
		return instance.get();
	}

	/**
	 * The connector will only add nodes for connections that this node wants to
	 * establish. Outbound (we send HB messages to) requests do not come through
	 * this class.
	 * 
	 * @param node
	 */
	public void addConnectToThisNode(HeartbeatData node) {
		// null data is not allowed
		if (node == null || node.getNodeId() == null)
			throw new RuntimeException("Null nodes or IDs are not allowed");


		if(!monitors.containsKey(node.getHost())) {
			// register the node to the manager that is used to determine if a
			// connection is usable by the public messaging
			HeartbeatManager.getInstance().addNearestNode(node);

			// this class will monitor this channel/connection and together with the
			// manager, we create the circuit breaker pattern to separate
			// health-status from usage.
			HeartbeatListener hbmon = new HeartbeatListener(node);
			MonitorHandler handler = new MonitorHandler();
			handler.addListener(hbmon);
			HeartMonitor hm = new HeartMonitor(node.getHost(), node.getMgmtport(), handler, node.getNodeId());
			hm.setExternal(node.isExternal());
			monitors.put(node.getHost(),hm);

			//writeConfToFile();
		}
	}

	public void addConnectByBroadcast(HeartbeatData node) {

		if (node == null || node.getNodeId() == null)
			throw new RuntimeException("Null nodes or IDs are not allowed");


		if(!monitors.containsKey(node.getHost())) {

			addConnectToThisNode(node);

			NodeDesc nearestNode = new NodeDesc();
			nearestNode.setHost(node.getHost());
			nearestNode.setNodeId(node.getNodeId());
			nearestNode.setPort(node.getPort());
			nearestNode.setMgmtPort(node.getMgmtport());

			conf.addNearestNode(nearestNode);
			writeConfToFile();
		}
		else {
			logger.info("Broadcasted node already exists");
		}
	}

	public boolean addExternalNode(HeartbeatData node) {

		if(!monitors.containsKey(node.getHost())) {
			addConnectToThisNode(node);
			return true;
		}
		else 
			return false;
	}
	/**
	 * After removing the node from incoming and outgoing HB queues
	 * the node also has to be removed from monitor
	 * 
	 * 
	 * @param heart
	 */
	public void removeNodeFromMonitor(String nodeId, String host) {

		if(monitors.containsKey(host)) {

			monitors.remove(host);
			conf.getNearest().remove(nodeId);
			writeConfToFile();
		}
		/*else {
			logger.info("HeartbeatConnector: Cannot remove node. Monitor doesn't contain this node");
			System.out.println(monitors);
		}*/
	}

	@Override
	public void run() {

		while (forever) {
			try {
				validateConnection();
				if (monitors.size() == 0) {
					logger.info("HB connection monitor not started, no connections to establish");
					Thread.sleep(sConnectRate);
				} 
				else {
					logger.info("HB connection monitor starting, node has " + monitors.size() + " connections");

					Thread.sleep(sConnectRate);
					// try to establish connections to our nearest nodes
					
					for (HeartMonitor hb : monitors.values()) {
						//validateConnection();
						if (!hb.isConnected()) {
							try {
								logger.info("attempting to connect to node: " + hb.getNodeInfo());
								hb.initiateHeartbeat();

							} catch (Exception ie) {
								// do nothing
								logger.info("HeartMonitor: Cannot connect, Node doesn't exist");
								removeNodeFromMonitor(hb.getNodeId(), hb.getHost());
								HeartbeatManager.getInstance().removeNodeFromIncomingHB(hb.getNodeId());
							}
						}
					}

				}
			} catch (InterruptedException e) {
				logger.error("Unexpected HB connector failure", e);
				break;
			}
		}
		logger.info("ending hbMgr connection monitoring thread");
	}

	private void validateConnection() {
		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().incomingHB.values()) {
			// receive HB - need to check if the channel is readable
			logger.info("IncomingHB: "+hb.getStatus()+" Node: "+ hb.getNodeId());
			if (hb.channel == null) {
				if (hb.getStatus() == BeatStatus.Active || hb.getStatus() == BeatStatus.Weak) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			} else if (hb.channel.isConnected()) {
				if (hb.channel.isWritable()) {
					if (System.currentTimeMillis() - hb.getLastBeat() >= hb.getBeatInterval()) {
						//System.out.println("IncomingHB: isconnected and iswritable" + hb.getLastBeat() + ":" + hb.getBeatInterval());
						hb.incrementFailures();
						hb.setStatus(BeatStatus.Weak);
						//hb.isGood();
					} else {
						//System.out.println("IncomingHB else of isconnected and is writable");
						hb.setStatus(BeatStatus.Active);
						hb.setFailures(0);
					}
				} else
					hb.setStatus(BeatStatus.Weak);
			} else {
				if (hb.getStatus() != BeatStatus.Init) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			}
		}

		// validate connections this node wants to create
		for (HeartbeatData hb : HeartbeatManager.getInstance().outgoingHB.values()) {
			// emit HB - need to check if the channel is writable
			logger.info("OutgoingHB: "+hb.getStatus()+" Node: "+ hb.getNodeId());
			if (hb.channel == null) {
				if (hb.getStatus() == BeatStatus.Active || hb.getStatus() == BeatStatus.Weak) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			} else if (hb.channel.isConnected()) {
				if (hb.channel.isWritable()) {
					if (System.currentTimeMillis() - hb.getLastBeatSent() >= hb.getBeatInterval()) {
						//System.out.println("OutgoingHB: isconnected and iswritable " + hb.getLastBeatSent() + ":" + hb.getBeatInterval());
						hb.incrementFailures();
						hb.setStatus(BeatStatus.Weak);
						//hb.isGood();
					} else {
						//System.out.println("OutgoingHB: else of isconnected and is writable");
						hb.setStatus(BeatStatus.Active);
						hb.setFailures(0);
					}
				} else
					hb.setStatus(BeatStatus.Weak);
			} else {
				if (hb.getStatus() != BeatStatus.Init) {
					hb.setStatus(BeatStatus.Failed);
					hb.setLastFailed(System.currentTimeMillis());
					hb.incrementFailures();
				}
			}
		}
	}

	public void writeConfToFile() {

		String json = JsonUtil.encode(conf);
		FileWriter fw = null;
		try {
			try {
				fw = new FileWriter(new File(confPath));
				fw.write(json);


			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println("JSON: " + json);
		} finally {
			try {
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
