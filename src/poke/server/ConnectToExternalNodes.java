package poke.server;

import java.util.Scanner;

import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatData;

public class ConnectToExternalNodes {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter the Host Address of the node");
		String hostAddress = scanner.next();
		System.out.println("Enter the Management Port of the node");
		int mgmtPort = scanner.nextInt();
		System.out.println("Enter the Port of the node");
		int port = scanner.nextInt();
		
		HeartbeatData hd = new HeartbeatData(hostAddress, hostAddress, port, mgmtPort);
		
		if(HeartbeatConnector.getInstance().addExternalNode(hd)) {
			
			System.out.println("Node added Successfully");
		}
		else {
			
			System.out.println("Cannot add node: Host Address already exists");
		}
		
		scanner.close();
		
	}

}
