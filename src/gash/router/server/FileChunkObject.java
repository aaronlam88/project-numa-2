package gash.router.server;

public class FileChunkObject {
	String hostAddress = "0.0.0.0";
	private int chunk_id = -1;
	private String fileName = "";
	int host_id = -1;
	int port_id = -1;
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
}
