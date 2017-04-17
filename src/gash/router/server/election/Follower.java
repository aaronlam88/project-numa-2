package gash.router.server.election;

import gash.router.server.ServerState;
import gash.router.server.election.Candidate;
import java.util.Timer;
import gash.router.server.election.Candidate;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import gash.router.server.election.Leader;

public class Follower implements Runnable{
	protected static Logger logger = LoggerFactory.getLogger("follower");

	private boolean isFollower=false;
	private int currentTerm;
	private int currentNodeId;
	private static ServerState state;
	Timer electionTimer=null;
	Timer beatTimer=null;
	
	public Follower(ServerState state){

		isFollower = state.getStatus().getFollower();
		this.state=state;
		
	}

	public void startElectionTimer(){

		if(isFollower){

			currentTerm=state.getStatus().getCurrentTerm();
			currentNodeId=state.getConf().getNodeId();

			try{
				 electionTimer= new Timer();
				 electionTimer.schedule(new TimerTask(){
					public void run(){
						try{
							while(state.getStatus().isElectionTimeout()){
								System.out.println("started election");
								if(state.getStatus().getLeaderId()==0){
									boolean pos=true;
									boolean neg=false;
									System.out.println("leaderid is zero");
									System.out.println(System.currentTimeMillis());

									//ServerElectionStatus status=;	
									state.getStatus().setFollower(neg);
									state.getStatus().setLeader(neg);
									state.getStatus().setCandidate(pos);

									//state.setStatus(status);

									System.out.println(state.getStatus().getCandidate());

									Candidate cobj = new Candidate(state);
									Thread th = new Thread(cobj);
									th.start();
									state.getStatus().setElectionTimeout(false);
								}
							}
						}
						catch(Exception e) {
							e.printStackTrace();
						}
					}
				}, ThreadLocalRandom.current().nextInt(1000, 5000));  // start the process after this many ms, and chec kthe result

			}
			catch(Exception e){
				logger.error("Election timeout failed", e);
			}

		}
		
	}


	@Override
	public void run(){

		if(state.getStatus().isHeartbeatTimeout()){
			try{
				this.startHeartBeatTimer();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}

		if(state.getStatus().isElectionTimeout()){
			try{
				this.startElectionTimer();
			}
			catch(Exception e){
				e.printStackTrace();
			}
		}


	}
	

	public void startHeartBeatTimer(){

		if(isFollower){
			try{
				beatTimer= new Timer();
				 beatTimer.schedule(new TimerTask(){
					public void run(){
						try{
							if(state.getStatus().isHeartbeatTimeout()){
								if(state.getStatus().getNextIndex()!=state.getStatus().getPrevIndex()){
									state.getStatus().setFollower(false);
									state.getStatus().setLeader(false);
									state.getStatus().setCandidate(true);

									Candidate cobj = new Candidate(state);
									cobj.startElection();
									state.getStatus().setElectionTimeout(false);
									
								}
								else if(state.getStatus().getNextIndex()==state.getStatus().getPrevIndex()){
									
									state.getStatus().setLeader(false);
									state.getStatus().setCandidate(false);
									state.getStatus().setFollower(true);

								}
							state.getStatus().setHeartbeatTimeout(false);
							}
						}
						catch(Exception e) {
							e.printStackTrace();
						}
					}
				}, ThreadLocalRandom.current().nextInt(3000, 5000)); 

			}
			catch(Exception e){
				e.printStackTrace();
			}
		}
	}


	//TODO if appendEntry is received in the state of the follower then maks the necessary setting and keep it in a follwoer stage

	//TODO dont process the write messagge if the sever is in the follower state
}
