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
package poke.server;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.BroadcastQueue.BroadcastQueueEntry;
import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatData;

/**
 * The inbound management worker handles the receiving of heartbeats (network
 * status) and requests to for this node to send heartbeats. Nodes making a
 * request to receive heartbeats are in essence requesting to establish an edge
 * (comm) between two nodes. On failure, the connecter must initiate a reconnect
 * - to produce the hbMgr.
 * 
 * On loss of connection: When a connection is lost, the emitter will not try to
 * establish the connection. The edge associated with the lost node is marked
 * failed and all outbound (enqueued) messages are dropped (TBD as we could
 * delay this action to allow the node to detect and re-establish the
 * connection).
 * 
 * Connections are bi-directional (reads and writes) at this time.
 * 
 * @author gash
 * 
 */

public class InboundBroadcastWorker extends Thread {
	protected static Logger logger = LoggerFactory.getLogger("server");

	int workerId;
	boolean forever = true;
	String nodeId;

	public String getNodeId() {
		return nodeId;
	}

	public void setNodeId(String nodeId) {
		this.nodeId = nodeId;
	}

	public InboundBroadcastWorker(ThreadGroup tgrp, int workerId) {
		super(tgrp, "inbound-broadcast-" + workerId);
		this.workerId = workerId;

		//if (BroadcastQueue.outbound == null)
		//	throw new RuntimeException("connection worker detected null queue");
	}

	@Override
	public void run() {
		while (true) {
			if (!forever && BroadcastQueue.inbound.size() == 0)
				break;

			try {
				// block until a message is enqueued
				BroadcastQueueEntry msg = BroadcastQueue.inbound.take();
				logger.info("Inbound broadcast received");
				
				if(!msg.nodeId.equals(nodeId)) {
					//System.out.println(msg.ipAddress);
				HeartbeatData node = new HeartbeatData(msg.nodeId, msg.ipAddress, null, msg.mgmtPort);
				HeartbeatConnector.getInstance().addConnectByBroadcast(node);
				}
				else {
					logger.info("Received own broadcast: Ignoring....");
				}
				
				
			} catch (InterruptedException ie) {
				break;
			} catch (Exception e) {
				logger.error("Unexpected processing failure", e);
				break;
			}
		}

		if (!forever) {
			logger.info("broadcast queue closing");
		}
	}
}
