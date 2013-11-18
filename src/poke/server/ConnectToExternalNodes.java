package poke.server;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import poke.server.management.HeartbeatConnector;
import poke.server.management.HeartbeatData;
import poke.server.nconnect.NodeClient;
import poke.server.nconnect.NodeResponseQueue;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectToExternalNodes extends Thread{

	protected static Logger logger = LoggerFactory.getLogger("ConnectToExternalNodes");

	@Override
	public void run(){

		String fileName = "conf//externalNode.conf";

		File file = new File(fileName);

		while(true){

			if(file.length()==0){
				try {
					Thread.sleep(8000);
				} catch (InterruptedException e) {

					e.printStackTrace();
				}
				continue;
			}else{

				try {

					List<String> fileContents = FileUtils.readLines(file);

					System.out.println("Number of lines in conf file "+fileContents.size());

					if(fileContents.size() ==5){

						String nodeId = fileContents.get(0);

						String host = fileContents.get(1);

						int port = Integer.valueOf(fileContents.get(2));

						int mgmtPort = Integer.valueOf(fileContents.get(3));
						
						String homogeneous = fileContents.get(4);

						if(homogeneous.equalsIgnoreCase("Yes")){
					
						HeartbeatData hd = new HeartbeatData(nodeId , host , port , mgmtPort );
						hd.setExternal(true);

						if(HeartbeatConnector.getInstance().addExternalNode(hd)) {

							logger.info("New homogeneous external node with nodeId "+nodeId+" has been added to externalNode map");
							
							FileUtils.writeStringToFile(file, "");
						}
						else {

							System.out.println("Cannot add node: Host Address already exists");
						}
					}else{
						
						NodeClient activeNode = new NodeClient(host, port , nodeId); // creates public TCP connection with external node
						
						NodeResponseQueue.addExternalNode(nodeId, activeNode);
						
						logger.info("New heterogeneous external node with nodeId "+nodeId+" has been added to externalNode map");

					}

					}else{
						logger.error("Invalid connection information: Verify the info provided");
						FileUtils.writeStringToFile(file, "");
					}

					Thread.sleep(8000);

				} catch (FileNotFoundException e) {

					logger.error("File not found exception in connect to external node "+e.toString());
					e.printStackTrace();
				} catch (IOException e) {
					logger.error("IO found exception in connect to external node "+e.toString());
					e.printStackTrace();
				} catch (InterruptedException e) {
					
					e.printStackTrace();
				}catch (Exception e){

					logger.info("General exception occurred in ConnectToExternalNodes "+e.getMessage());
					logger.info("please check the data provided");

					e.printStackTrace();
				}

			}
		}	

	}

}
