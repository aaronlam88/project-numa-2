package gash.router.server;

import java.nio.file.Paths;
import java.util.Deque;
import java.util.Hashtable;
import java.util.LinkedList;

import gash.router.container.RoutingConf;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.tasks.TaskList;
import gash.router.server.election.ServerElectionStatus;

import pipe.common.Common.LocationList;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;
import java.util.concurrent.LinkedBlockingDeque;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerState {
	protected static Logger logger = LoggerFactory.getLogger("ServerState");
	public boolean keepWorking = true;
	// Performance monitor for task stealing 
	private PerformanceMonitor perfmon;
	private RoutingConf conf;
	private EdgeMonitor emon;
	private TaskList tasks;
	private ServerElectionStatus status;
	public int CPUthreshhold = 60;
	private int currentLeader; // current leader node id
	private boolean isLeader;
	public static Hashtable<String, LocationList.Builder> hashTable = new Hashtable<>();
	private String dataPath;
	private int currentTerm;
	private boolean voted;

	// This queue has info about incoming chunks that has been saved to file
	// system
	// Worker should report this to leader and get logs updated
	public Deque<FileChunkObject> incoming;

	// These queues contain packets that are not destined for me. To e forwarded
	// into network.
	// Don't forward to same node which sent it.
	// Can be forwarded by edge monitor. Content is pushed by commandhandler and
	// workerhandler
	public LinkedBlockingDeque<WorkMessage> wmforward;	
	public LinkedBlockingDeque<CommandMessage> cmforward;

	public ServerState(String dbpath) {
		if (dbpath != null) {
			this.dataPath = dbpath;
		} else {
			dataPath = Paths.get(".", "data").toAbsolutePath().normalize().toString();
			System.out.println(dataPath);
		}
		wmforward = new LinkedBlockingDeque<WorkMessage>();
		incoming = new LinkedList<FileChunkObject>();

		currentLeader = -1; // unknown
		isLeader = true; // doesn't assume itself as leader
		// if both currentLeader is unknown call for election
		cmforward = new LinkedBlockingDeque<CommandMessage>();

		this.status = new ServerElectionStatus();
		perfmon = new PerformanceMonitor();

		logger.info("ServerElectionStatus values initialized");

		
	}

	public String getDbPath() {
		return this.dataPath;
	}

	public RoutingConf getConf() {
		return conf;
	}

	public int getPerformanceStat() {
		return (int) perfmon.getCpuUsage();
	}

	public void setConf(RoutingConf conf) {
		this.conf = conf;
	}
	public void updateConf(){
		emon.updateEdges();
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
		this.currentLeader = status.getLeaderId();
		return currentLeader;
	}

	public void setCurrentLeader(int currentLeader) {
		this.currentLeader = currentLeader;
		status.setLeaderId(this.currentLeader);
	}

	public boolean isLeader() {
		this.isLeader = status.getLeader();
		return isLeader;
	}

	public void setLeader(boolean isLeader) {
		this.isLeader = isLeader;
		status.setLeader(this.isLeader);
	}

	public ServerElectionStatus getStatus() {
		return status;
	}

	public void setStatus(ServerElectionStatus status) {
		this.status = status;
	}

	public int getCurrentTerm() {
		this.currentTerm = status.getCurrentTerm();
		return currentTerm;
	}

	public void setCurrentTerm(int currentTerm) {
		this.currentTerm = currentTerm;
		status.setCurrentTerm(this.currentTerm);
	}

	public boolean isVoted() {
		return voted;
	}

	public void setVoted(boolean voted) {
		this.voted = voted;
	}

}
