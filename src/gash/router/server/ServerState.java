package gash.router.server;

import java.nio.file.Paths;
import java.util.Deque;
import java.util.Hashtable;
import java.util.LinkedList;
import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;
import pipe.common.Common.LocationList;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import java.util.concurrent.LinkedBlockingDeque;

public class ServerState {
	// The Log file for the system. Record filename and it location.
	public static Hashtable<String, LocationList> hashTable = new Hashtable<>();
	
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	private String dataPath;
	
	private int currentLeader; //current leader node id
	private boolean isLeader;
	private int currentTerm;
	private boolean voted; 
	
	// This queue has info about incoming chunks that has been saved to file system
	// Worker should report this to leader and get logs updated
	public Deque<FileChunkObject> incoming;
	
	// These queues contain packets that are not destined for me. To e forwarded into network.
	// Don't forward to same node which sent it.
	// Can be forwarded by edge monitor. Content is pushed by commandhandler and workerhandler
	public LinkedBlockingDeque<WorkMessage> wmforward;
	public LinkedBlockingDeque<CommandMessage> cmforward;
	
	public ServerState(String dbpath){
		if(dbpath != null){
			this.dataPath = dbpath;
		}else{
			dataPath = Paths.get(".", "data").toAbsolutePath().normalize().toString();
		}
		
		incoming = new LinkedList<FileChunkObject>();
		
		wmforward = new LinkedBlockingDeque<WorkMessage>();
		cmforward = new LinkedBlockingDeque<CommandMessage>();
		
		currentLeader = -1; // unknown
		isLeader = false; // doesn't assume itself as leader
		//if both currentLeader is unknown and !isLeader call for election
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

	public int getCurrentLeader() {
		return currentLeader;
	}

	public void setCurrentLeader(int currentLeader) {
		this.currentLeader = currentLeader;
	}

	public boolean isLeader() {
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
	}

	public int getCurrentTerm() {
		return currentTerm;
	}

	public void setCurrentTerm(int currentTerm) {
		this.currentTerm = currentTerm;
	}

	public boolean isVoted() {
		return voted;
	}

	public void setVoted(boolean voted) {
		this.voted = voted;
	}
}
