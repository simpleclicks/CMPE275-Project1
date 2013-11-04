package poke.server.replication;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import poke.server.nconnect.NodeResponseQueue;
import poke.server.storage.jdbc.DatabaseStorage;
/**
 * @author Kaustubh
 * @version 2.3
 * {@code : Initiates all the replication requests}
 * 
 */

public class ReplicationInitializer extends Thread {
	
	static final private DatabaseStorage dbAct = DatabaseStorage.getInstance();
	
	private static final Logger logger = LoggerFactory.getLogger("ReplicationInitializer");
	
	private static final int pollingInterval = 15000;
	
	@Override
	public void run() {
		
		while(true){
			
			try {
		
		 List<String> unReplFilesList = dbAct.documentsToBeReplicated();
		
		logger.info("Documents remaining to be replicated "+unReplFilesList.size());
		
		if(unReplFilesList.size()>0){
			
			while( unReplFilesList.size() > 0){
				
				String filePath = unReplFilesList.remove(0);
				
				NodeResponseQueue.multicastDocReplication(filePath);
				
				logger.info("Replication has been intiated for "+filePath);
				
				Thread.sleep(10000);
			
			}// file - replication for ends
			
		}else{
				
			Thread.sleep(pollingInterval);
		}// replication list size if-else ends
	
		} catch (InterruptedException e) {
			e.printStackTrace();
		}catch (Exception e) {
			logger.error(" Enounterd exception "+e.getMessage());
			e.printStackTrace();
		}
		
		}// while-true ends
	}
	
	

}
