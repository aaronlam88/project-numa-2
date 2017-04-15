package gash.router.server;

import java.util.Hashtable;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;
import pipe.common.Common.Chunk;
import pipe.common.Common.ChunkLocation;

public class ServerState {
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	
	private Hashtable<Chunk, ChunkLocation> hashTable;

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

	public Hashtable<Chunk, ChunkLocation> getHashTable() {
		return hashTable;
	}

	public void setHashTable(Hashtable<Chunk, ChunkLocation> hashTable) {
		this.hashTable = hashTable;
	}

}
