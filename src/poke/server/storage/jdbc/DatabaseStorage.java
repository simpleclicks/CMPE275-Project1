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
package poke.server.storage.jdbc;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import poke.server.management.HeartbeatManager;
import poke.server.storage.Storage;

import com.jolbox.bonecp.BoneCP;
import com.jolbox.bonecp.BoneCPConfig;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.handlers.BeanHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.io.FileUtils;

public class DatabaseStorage {
	protected static Logger logger = LoggerFactory.getLogger("database");

	public static final String sDriver = "jdbc.driver";
	public static final String sUrl = "jdbc.url";
	public static final String sUser = "jdbc.user";
	public static final String sPass = "jdbc.password";

	protected Properties cfg;
	protected BoneCP cpool;
	static final private String self = HeartbeatManager.getInstance().getNodeId();
	
	private static DatabaseStorage ds = new DatabaseStorage();
	
	private static File dbLogs = new File("logs/db.log");

	public DatabaseStorage() {
		init();
	}
	
	public static DatabaseStorage getInstance() {
		return ds;
	}

	public void init() {
		if (cpool != null)
			return;

		try {
			Class.forName("com.mysql.jdbc.Driver");
			BoneCPConfig config = new BoneCPConfig();
			config.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/thunderbolts ");
			config.setUsername("root");
			config.setPassword("root");
			config.setMinConnectionsPerPartition(5);
			config.setMaxConnectionsPerPartition(10);
			config.setPartitionCount(1);
			config.setStatementsCacheSize(0);
			config.setPreparedStatementsCacheSize(0);
			cpool = new BoneCP(config);
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see gash.jdbc.repo.Repository#release()
	 */
	public void release() {
		if (cpool == null)
			return;

		cpool.shutdown();
		cpool = null;
	}

	public String getOwner(String namespace, String documentname) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Document> getOwnerRsh = new BeanHandler<Document>(Document.class);
		Document document= null;
		Connection conn = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where documentname = ? and namespacename = ?";
			document = qr.query(conn, sql, getOwnerRsh, documentname, namespace);
			
			if(document == null) {
				
				return "NA";
			}
			
		} catch (SQLException e) {
			logger.info("getOwner: Cannot get Owner for document "+documentname);
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return document.getOwner();
	}
	
	public boolean isReplicated(String namespace, String documentname) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Document> isReplicatedRsh = new BeanHandler<Document>(Document.class);
		Document document= null;
		Connection conn = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where documentname = ? and namespacename = ?";
			document = qr.query(conn, sql, isReplicatedRsh, documentname, namespace);
			
			if(document == null) {
				
				return false;
			} else {
				
				return document.isReplicated();
			}
			
		} catch (SQLException e) {
			logger.info("isReplicated: Cannot check whether document is replicated for document"+documentname);
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
	}
	
	public int countReplicate(String namespace, String documentname) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Document> countReplicateRsh = new BeanHandler<Document>(Document.class);
		Document document= null;
		Connection conn = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where documentname = ? and namespacename = ?";
			document = qr.query(conn, sql, countReplicateRsh, documentname, namespace);
			
			if(document == null) {
				return 0;
			} else {
				return document.getReplicationCount();
			}
			
		} catch (SQLException e) {
			logger.info("countReplicate: Cannot retrieve replication count for document "+documentname);
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return 0;
	}
	
	public String getReplicatedNode(String namespace, String documentname) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Document> getReplicatedNodeRsh = new BeanHandler<Document>(Document.class);
		Document document= null;
		Connection conn = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where documentname = ? and namespacename = ?";
			document = qr.query(conn, sql, getReplicatedNodeRsh, documentname, namespace);
			
			if(document == null) {
				return "DNE";
			} else {
				
				if(document.getReplicatedNode() !=null && document.getReplicatedNode().length() > 0)
				return document.getReplicatedNode() ;
				else
				return "NR";
			}
			
		} catch (SQLException e) {
			logger.info("getReplicatedNode: Cannot get replicated node for document "+documentname);
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return "NA";
	}
	
	public String getPreviousReplicatedNode(String namespace, String documentname) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<Document> getPreviousReplicatedNodeRsh = new BeanHandler<Document>(Document.class);
		Document document= null;
		Connection conn = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where documentname = ? and namespacename = ?";
			document = qr.query(conn, sql, getPreviousReplicatedNodeRsh, documentname, namespace);
			
			if(document == null) {
				return "NA";
			} else {
				return document.getPreviousReplicatedNode();
			}
			
		} catch (SQLException e) {
			logger.info("getPreviousReplicatedNode: Cannot get previous replicated node for document "+documentname);
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return "NA";
	}
	
	
	
	public boolean addDocumentInDatabase(String namespaceName, String documentname) {

		QueryRunner qr = new QueryRunner();
		Connection conn = null;
		int insertCount = 0;
		
		try {
			
			conn = cpool.getConnection();
						
			if(namespaceName != null) {
				String sql = "INSERT INTO Document(DocumentName, NamespaceName, IsReplicated, ReplicationCount, Owner) VALUES (?, ?, ?, ?, ?)";
				insertCount = qr.update(conn, sql, documentname, namespaceName, false, 0, self);
			} else {
				String sql = "INSERT INTO Document(DocumentName, IsReplicated, ReplicationCount, Owner) VALUES (?, ?, ?, ?)";	
				insertCount = qr.update(conn, sql, documentname, false, 0, "self");
			}
			
			if(insertCount < 1) {
				FileUtils.write(dbLogs, "DB insert failed for "+namespaceName+documentname,true);
				return false;
			}
			else {
				return true;
			}
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("addDocumentInDatabase: Cannot add document in database for document: "+documentname);
			try {
				FileUtils.write(dbLogs, "DB insert failed for "+namespaceName+documentname,true);
			} catch (IOException e1) {
				logger.info("addDocumentInDatabase: IOException while writing logs to file");
				e1.printStackTrace();
			}
			e.printStackTrace();		
			return false;
		} catch (IOException e) {
			logger.info("addDocumentInDatabase: IOException while writing logs to file");
			e.printStackTrace();
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
		
	}
	
	public boolean addReplicaInDatabase(String namespaceName, String documentname, String owner , String replicatedNode) {

		QueryRunner qr = new QueryRunner();
		Connection conn = null;
		int insertCount = 0;
		
		try {
			
			conn = cpool.getConnection();
						
			if(namespaceName != null) {
				String sql = "INSERT INTO Document(DocumentName, NamespaceName , IsReplicated, ToBeReplicated, Owner, ReplicatedNode) VALUES (?, ?, false, false, ?, ?)";
				insertCount = qr.update(conn, sql, documentname, namespaceName, owner, replicatedNode);
			} else {
				String sql = "INSERT INTO Document(DocumentName, IsReplicated, ToBeReplicated, Owner, ReplicatedNode) VALUES (?, false, false, ?, ?)";
				insertCount = qr.update(conn, sql, documentname, owner, replicatedNode);
			}
			
			if(insertCount < 1) {
				return false;
			}
			else {
				return true;
			}
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("addReplicaInDatabase: Cannot add replica in database for document "+documentname);
			e.printStackTrace();			
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
	}

	public boolean deleteDocumentInDatabase(String namespaceName, String documentname) {

		QueryRunner qr = new QueryRunner();
		
		Connection conn = null;
		int deleteCount = 0;
		
		try {
			
			conn = cpool.getConnection();
			conn.setAutoCommit(true);
			String sql = "Delete from Document where DocumentName = ? and NamespaceName = ?";
			logger.info("Deleteing document from DB "+namespaceName+documentname);
			deleteCount = qr.update(conn, sql, documentname, namespaceName);
			
			if(deleteCount < 1) {
				logger.info("Could not delete document "+namespaceName+documentname);
				return false;
			}
			else {
				logger.info("Delete document "+namespaceName+documentname);
				return true;
			}
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("deleteDocumentInDatabase: Cannot delete document in database for document "+documentname);
			e.printStackTrace();			
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
	}
	
	public boolean updateReplicationCount(String namespaceName, String documentname, String replicatedNode, int replicationCount) {

		QueryRunner qr = new QueryRunner();
		Connection conn = null;
		int insertCount = 0;
		
		try {
			
			conn = cpool.getConnection();
							
			String sql = "Update Document set ReplicatedNode = ?,ReplicationCount =? , IsReplicated = true , ToBeReplicated = false where DocumentName = ? and NamespaceName = ?";
			insertCount = qr.update(conn, sql, replicatedNode, replicationCount, documentname, namespaceName);
			
			if(insertCount < 1) {
				return false;
			}
			else {
				return true;
			}
						
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.info("addDocumentInDatabase: Cannot update replication count in database for document "+documentname);
			e.printStackTrace();			
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return false;
	}
	
	public List<String> getDocuments(String replicatedNode) {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<List<Document>> getDocumentsRsh = new BeanListHandler<Document>(Document.class);
		
		Connection conn = null;
		List<String> returnDocument = null;
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where replicatedNode = ?";
			List<Document> documentList = qr.query(conn, sql, getDocumentsRsh, replicatedNode);
			
			if(documentList == null) {
				return returnDocument;
			} else {
				ListIterator<Document> listIterator = documentList.listIterator();
				int index = 0;
				returnDocument = new ArrayList<String>();
				while (listIterator.hasNext()) {
					returnDocument.add(documentList.get(index).getNamespaceName()+"/"+documentList.get(index).getDocumentName());
					index++;
				}
				return returnDocument; 
			}
			
		} catch (SQLException e) {
			logger.info("getReplicatedNode: Cannot get documents for replicated node "+replicatedNode);
			e.printStackTrace();
		} finally {
			try {
				
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return returnDocument;
	}
	
	public List<String> documentsToBeReplicated() {
		
		QueryRunner qr = new QueryRunner();
		ResultSetHandler<List<Document>> getDocumentsRsh = new BeanListHandler<Document>(Document.class);
		
		Connection conn = null;
		List<String> returnDocument = new ArrayList<String>();
		
		try {
			
			conn = cpool.getConnection();
			String sql = "select * from document where owner = ? and ToBeReplicated = true and isReplicated = false and ReplicationCount < 1";
			List<Document> documentList = qr.query(conn, sql, getDocumentsRsh, self);
			
			if(documentList == null || documentList.size() == 0) {
			
				return returnDocument;
			
			} else {
				
				for(Document unRepDoc :documentList ){
					returnDocument.add(unRepDoc.getNamespaceName()+unRepDoc.getDocumentName());
				}
				
				return returnDocument; 
			}
			
		} catch (SQLException e) {
			
			logger.info("getReplicatedNode: Cannot get documents for owner "+self);
			e.printStackTrace();
		
		} finally {
			try {
				qr=null;
				conn.close();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return returnDocument;
	}


	public static void main(String args[]) {
		
		DatabaseStorage ds = new DatabaseStorage();
		//ds.addDocumentInDatabase(null, "abc.txt", false, 0, "four");
		//System.out.println(ds.getOwner("EMPTY", "abc.txt"));
		//ds.addReplicaInDatabase(null, "abc.txt", 1, "four", "five", null);
		ds.deleteDocumentInDatabase("home\\kau", "abc.txt");
	}

}
