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

import java.net.SocketAddress;

import org.jboss.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.monitor.MonitorListener;
import poke.server.management.HeartbeatData.BeatStatus;
import poke.server.nconnect.NodeClient;
import poke.server.nconnect.NodeResponseQueue;

public class HeartbeatListener implements MonitorListener {
	protected static Logger logger = LoggerFactory.getLogger("management");

	private HeartbeatData data;

	public HeartbeatListener(HeartbeatData data) {
		this.data = data;
	}

	public HeartbeatData getData() {
		return data;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#getListenerID()
	 */
	@Override
	public String getListenerID() {
		return data.getNodeId();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see poke.monitor.MonitorListener#onMessage(eye.Comm.Management)
	 */
	@Override
	public void onMessage(eye.Comm.Management msg, Channel channel, SocketAddress socketaddress) {
		if (logger.isDebugEnabled())
			logger.debug(msg.getBeat().getNodeId());

		if (msg.hasGraph()) {
			logger.info("Received graph responses");
		} else if (msg.hasBeat() && msg.getBeat().getNodeId().equals(data.getNodeId())) {
			logger.info("Tracing code flow 2: HeartbeatLisner Received HB response from " + msg.getBeat().getNodeId());
			data.setLastBeat(System.currentTimeMillis());

			HeartbeatData hd = HeartbeatManager.getInstance().getHeartbeatData(msg.getBeat().getNodeId());
			
			if(hd.getStatus().compareTo(BeatStatus.Init) == 0) {
				HeartbeatManager.getInstance().addNearestNodeChannel(msg.getBeat().getNodeId(), channel, socketaddress);
			}
			
			if(!NodeResponseQueue.nodeExistCheck(data.getNodeId()) && !data.isExternal()){
				
				NodeClient activeNode = new NodeClient(data.getHost(), data.getPort(), data.getNodeId()); // creates public TCP connection with internal node
				
				NodeResponseQueue.addActiveNode(data.getNodeId(), activeNode);
				
				logger.info("New internal node with nodeId "+data.getNodeId()+" has been added to activeNode map");
				
				logger.info("number of active nodes in the network "+NodeResponseQueue.activeNodesCount());
			}else{
				
				if(!NodeResponseQueue.externalNodeExistCheck(data.getNodeId()) && data.isExternal()){
					
					NodeClient activeNode = new NodeClient(data.getHost(), data.getPort(), data.getNodeId()); // creates public TCP connection with external node
					
					NodeResponseQueue.addExternalNode(data.getNodeId(), activeNode);
					
					logger.info("New external node with nodeId "+data.getNodeId()+" has been added to externalNode map");
				}
			}
		
	} else
			logger.error("Received hbMgr from on wrong channel or unknown host: " + msg.getBeat().getNodeId());
	}

	@Override
	public void connectionFailed() {
		// note a closed management port is likely to indicate the primary port
		// has failed as well
	}

	@Override
	public void connectionReady() {
		// do nothing at the moment
	}
}
