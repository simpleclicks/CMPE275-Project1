package poke.server;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.conf.ServerConf;
import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatData;
import poke.server.resources.ResourceFactory;

public class BroadcastHandler extends Thread {

	protected static Logger logger = LoggerFactory.getLogger("server");
	static DatagramSocket recieveSocket;
	static int recievePort;
	DatagramPacket packet;
	static ServerConf broadcastConf;
	static BroadcastHandler broadcast;
	
	private BroadcastHandler(){
		
	}

	public static void intialize(int port, ServerConf conf) {
	
		recievePort = port;
		broadcastConf = conf;
		broadcast = new BroadcastHandler();
		 
	}
	
	public static BroadcastHandler getInstance() {
		
		if (broadcast == null)
			throw new RuntimeException("Server not intialized");

		return broadcast;
	}
	public void run() {

		try {

			recieveSocket = new DatagramSocket(recievePort, InetAddress.getByName("localhost"));
			recieveSocket.setBroadcast(true);

			while (true) {

				byte[] recvBuf = new byte[15000];

				packet = new DatagramPacket(recvBuf, recvBuf.length);

				recieveSocket.receive(packet);
				String message = new String(packet.getData()).trim();

				if (message.contains("NETWORK_DISCOVERY")) {

					String nodeId = message.split("_")[2];
					String hostAddress = packet.getAddress().getHostAddress();
					int port = Integer.valueOf(broadcastConf.getServer().getProperty("port"));
					int mgmtPort = Integer.valueOf(broadcastConf.getServer().getProperty("port.mgmt"));
					
					logger.info("Broadcast recieved");
					HeartbeatData node = new HeartbeatData(nodeId, hostAddress, port, mgmtPort);
					HeartbeatConnector.getInstance().addConnectToThisNode(node);
					
				}

			}
		} catch (SocketException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}


}
