package gash.router.server.election;

import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ServerElectionStatus{
	protected static Logger logger = LoggerFactory.getLogger("ServerElectionStatus");

	private int currentTerm;   // latest term server has seen (initialized to on first boot, increases monotonically)
	private boolean isVotedFor;
	private int votedFor;      // candidateId that received vote in current term (or null if none)
	private int totalVotesRecievedForThisTerm;
	private ArrayList<Log> log;      //  each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	//TODO map is better than arraylist of an object
	private int lastTermInLog;

	private int commitIndex;    // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	// to be increased after second message after append entry - after final commit
	private int lastAplliedIndex;  //index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	// to be increased after sending a replication message
	private boolean leader;     // whether the server is leader or not in this term
	private int nextIndex;      // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	private int prevIndex;    // for each server, index of highest log entry known to be replicated on server initialized to 0, increases monotonically)
	private boolean follower ;
	private boolean candidate;
	private int leaderId;     //initialized to zero which means the server initially has no leader and in a follower state, tyring to conenct to leader
	private boolean electionTimeout=true;
	private boolean heartbeatTimeout=false;
	private int totalAppendEntrySuccessForThisTerm;
	private int totalNodesDiscovered;
	private ArrayList<Integer> nodesThatRepliedBeats = new ArrayList<Integer>();


	public ServerElectionStatus(){

		logger.info("ServerElectionStatus values initialized");
		
		this.currentTerm=0;
		this.isVotedFor=false;
		this.lastTermInLog=1;
		this.commitIndex=0;
		this.lastAplliedIndex=0;
		this.leader=false;
		this.follower=true;
		this.candidate=false;
		this.leaderId=0;
		this.nextIndex=0;
		this.prevIndex=1;
		this.totalAppendEntrySuccessForThisTerm=1;
		//this.totalNodesDiscovered=1;
		this.totalVotesRecievedForThisTerm=0;
		this.electionTimeout=true;
		this.heartbeatTimeout=false;
	}
	
	public ArrayList<Integer> getNodesThatRepliedBeats(){
		return nodesThatRepliedBeats;
	}

	public void setNodesThatRepliedBeatsInList(int nodeid){
		this.nodesThatRepliedBeats.add(nodeid);
	}

	public void removeAllInList(){
		this.nodesThatRepliedBeats.clear();
	}

	public int getTotalNodesDiscovered() {
		return totalNodesDiscovered;
	}

	public void setTotalNodesDiscovered(int tnd) {
		this.totalNodesDiscovered = tnd;
	}

	public boolean isElectionTimeout() {
		return electionTimeout;
	}


	public void setElectionTimeout(boolean et) {
		this.electionTimeout = et;
	}

	public boolean isIsVotedFor() {
		return isVotedFor;
	}


	public void setIsVotedFor(boolean vf) {
		this.isVotedFor = vf;
	}


	public boolean isHeartbeatTimeout() {
		return heartbeatTimeout;
	}


	public void setHeartbeatTimeout(boolean hbt) {
		this.heartbeatTimeout = hbt;
	}

	public int getCurrentTerm() {
		return currentTerm;
	}


	public void setCurrentTerm(int ct) {
		this.currentTerm = ct;
	}


	public int getVotedFor() {
		return votedFor;
	}


	public void setVotedFor(int vf) {
		this.votedFor = vf;
	}


	public int getTotalVotesRecievedForThisTerm() {
		return totalVotesRecievedForThisTerm;
	}


	public void setTotalVotesRecievedForThisTerm(int tvrt) {
		this.totalVotesRecievedForThisTerm = tvrt;
	}

	public int getTotalAppendEntrySuccessForThisTerm(){
		return totalAppendEntrySuccessForThisTerm;
	}

	public void setTotalAppendEntrySuccessForThisTerm(int tae_term){
		this.totalAppendEntrySuccessForThisTerm=tae_term;
	}

	public ArrayList<Log> getLog() {
		return log;
	}


	public void setLog(ArrayList<Log> log) {
		this.log = log;
	}


	public int getLastTermInLog() {
		return lastTermInLog;
	}


	public void setLastTermInLog(int lti) {
		this.lastTermInLog = lti;
	}


	public int getCommitIndex() {
		return commitIndex;
	}


	public void setCommitIndex(int ci) {
		this.commitIndex = ci;
	}


	public int getLastAplliedIndex() {
		return lastAplliedIndex;
	}


	public void setLastAplliedIndex(int lai) {
		this.lastAplliedIndex = lai;
	}


	public boolean getLeader() {
		return leader;
	}


	public void setLeader(boolean l) {
		this.leader = l;
	}


	public int getNextIndex() {
		return nextIndex;
	}


	public void setNextIndex(int ni) {
		this.nextIndex = ni;
	}


	public int getPrevIndex() {
		return prevIndex;
	}


	public void setPrevIndex(int pi) {
		this.prevIndex = pi;
	}


	public boolean getFollower() {
		return follower;
	}


	public void setFollower(boolean fl) {
		this.follower = fl;
	}


	public boolean getCandidate() {
		return candidate;
	}


	public void setCandidate(boolean can) {
		this.candidate = can;
	}


	public int getLeaderId() {
		return leaderId;
	}


	public void setLeaderId(int li) {
		this.leaderId = li;
	}


	/*public Log createLogMessage(String msg,int term_num){

		Log log =new Log(msg,term_num);
		this.lastTermInLog=term_num;
		return log;
	}*/

	/*public Log getLastLog(){
		return log.get(log.size()-1);
	}

	public void setLog(Log addlog){
		if(this.log!=null){
			this.log.add(addlog);
		}
		else{
			log= new ArrayList<Log>();
		}
	}
	*/

	private static class Log{
		private String message;
		private int term;

		private Log(String message,int term){
			this.message=message;
			this.term=term;
		}
	}
}
