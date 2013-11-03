package poke.server.storage.jdbc;

import java.sql.Time;

public class Document {
	

	private String documentName;
	
	private String namespaceName;
	
	private String owner;
	
	private String replicatedNode;
	
	private String previousReplicatedNode;
	
	private int replicationCount;
	  
	private boolean isReplicated;
	
	private Time modifiedTS;

	public Time getModifiedTS() {
		return modifiedTS;
	}

	public void setModifiedTS(Time modifiedTS) {
		this.modifiedTS = modifiedTS;
	}

	public String getDocumentName() {
		return documentName;
	}

	public void setDocumentName(String documentName) {
		this.documentName = documentName;
	}

	public String getNamespaceName() {
		return namespaceName;
	}

	public void setNamespaceName(String namespaceName) {
		this.namespaceName = namespaceName;
	}

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner = owner;
	}

	public String getReplicatedNode() {
		return replicatedNode;
	}

	public void setReplicatedNode(String replicatedNode) {
		this.replicatedNode = replicatedNode;
	}

	public String getPreviousReplicatedNode() {
		return previousReplicatedNode;
	}

	public void setPreviousReplicatedNode(String previousReplicatedNode) {
		this.previousReplicatedNode = previousReplicatedNode;
	}

	public int getReplicationCount() {
		return replicationCount;
	}

	public void setReplicationCount(int replicationCount) {
		this.replicationCount = replicationCount;
	}

	public boolean isReplicated() {
		return isReplicated;
	}

	public void setReplicated(boolean isReplicated) {
		this.isReplicated = isReplicated;
	}

}
