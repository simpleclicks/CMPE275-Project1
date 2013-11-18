package poke.demo;

import java.io.File;
import java.io.BufferedReader;
import java.io.InputStreamReader;




import poke.client.ClientConnection;
import poke.client.ClientListener;
import poke.client.ClientPrintListener;
import poke.server.nconnect.NodeClient;
import poke.server.nconnect.NodeResponseQueue;

public class ExternalNodeClient {

	
	private String sep = File.separator;

	public void run() {
		
	
		String namespace = null;
		String docName = null;
		
		int size = 0;
		
		try {
			while (true) {
				
				BufferedReader buffer = new BufferedReader(new InputStreamReader(System.in));

				System.out.println("Enter the nodeID of external node you want to communicate with ");
				
				String nodeId = buffer.readLine();
				
				NodeClient nc  = NodeResponseQueue.getActiveExternalNode(nodeId);
				
				System.out.println("Select one function to be executed" +
						" 1.Document Query "+"\n"+" 2.Document add handshake "+"\n"+"3.Find document "+"\n"+"\n"+"4.Delete Document"+"\n"+"\n"+"5.Exit");
				
				int choice = Integer.parseInt (buffer.readLine());
				
				switch (choice) {
				case 1:
					System.out.println("You choose to query the document at external node "+nodeId);
					System.out.println("Enter the namespace for the document to be queried");
					namespace = buffer.readLine();
					System.out.println("Enter the name of the document to be queried ");
					docName = buffer.readLine();
					nc.queryFile(namespace, docName);
					Thread.sleep(5000);
					System.out.println("Response received from external node "+nodeId+" for docQuery as "+nc.checkDocQueryResponse(namespace, docName));
					break;
				case 2:
					System.out.println("You choose to send doc add handshake at external node"+nodeId);
					System.out.println("Enter the namespace for the document");
					namespace = buffer.readLine();
					System.out.println("Enter the name of the document");
					docName = buffer.readLine();
					System.out.println("Enter the size of the document");
					size = Integer.parseInt(buffer.readLine());
					nc.docAddHandshake(namespace, docName, size);
					Thread.sleep(5000);
					System.out.println("Response received from external node "+nodeId+" for docAddHandshake as "+nc.checkDocADDHSResponse(namespace, docName));
					break;
				case 3:
					System.out.println("You choose to send doc find to external node"+nodeId);
					System.out.println("Enter the namespace ");
					namespace = buffer.readLine();
					System.out.println("Enter the document name ");
					docName = buffer.readLine();
					nc.findFile(namespace, docName);
					Thread.sleep(5000);
					System.out.println("Response received from external node "+nodeId+" for document find as "+nc.checkDocFindResponse(namespace, docName));
					break;
				case 4:
					System.out.println("You choose to delete the document at external node "+nodeId);
					System.out.println("Enter the namespace for the document to be deleted");
					namespace = buffer.readLine();
					System.out.println("Enter the name of the document to be deleted ");
					docName = buffer.readLine();
					nc.removeDoc(namespace, docName);
					Thread.sleep(5000);
					System.out.println("Response received from external node "+nodeId+" for doc remove as "+nc.checkDocRemoveResponse(namespace, docName));
					break;
				case 5:
					System.out.println("You choose to terminate the client ");
					System.exit(0);
					break;

				default:
					break;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}
	
		}
	

	public static void main(String[] args) {
		try {
			
			ExternalNodeClient exClient = new ExternalNodeClient(); 
					
			exClient.run();

			System.out.println("\nExiting in 5 seconds");
			
			//Thread.sleep(5000);
			//System.exit(0);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
