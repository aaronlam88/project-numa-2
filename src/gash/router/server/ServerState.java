package gash.router.server;

import java.nio.file.Paths;
import java.util.Deque;
import java.util.Hashtable;
import java.util.LinkedList;
import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;
import pipe.common.Common.ChunkLocation;

public class ServerState {
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	public Hashtable<String, ChunkLocation[]> hashTable;
	private String dataPath;
	public Deque<FileChunkObject> incoming;
	
	public ServerState(String dbpath){
		if(dbpath != null){
			this.dataPath = dbpath;
		}else{
			dataPath = Paths.get(".", "data").toAbsolutePath().normalize().toString();
		}
		
		incoming = new LinkedList<FileChunkObject>();
	}
	
	public String getDbPath(){
		return this.dataPath;
	}
	
	public RoutingConf getConf() {
		return conf;
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}

	public EdgeMonitor getEmon() {
		return emon;
	}

	public void setEmon(EdgeMonitor emon) {
		this.emon = emon;
	}

	public TaskList getTasks() {
		return tasks;
	}

	public void setTasks(TaskList tasks) {
		this.tasks = tasks;
	}
}
