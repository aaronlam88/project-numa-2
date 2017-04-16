package gash.router.server;

public class FileChunkObject {
	private String hostAddress = "0.0.0.0";
	private int chunk_id = -1;
	private String fileName = "";
	private int node_id = -1;
	private int port_id = -1;
	
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public int getChunk_id() {
		return chunk_id;
	}
	public void setChunk_id(int chunk_id) {
		this.chunk_id = chunk_id;
	}
	public String getHostAddress() {
		return hostAddress;
	}
	public void setHostAddress(String hostAddress) {
		this.hostAddress = hostAddress;
	}
	public int getPort_id() {
		return port_id;
	}
	public void setPort_id(int port_id) {
		this.port_id = port_id;
	}
	public int getNode_id() {
		return node_id;
	}
	public void setNode_id(int node_id) {
		this.node_id = node_id;
	}
}
