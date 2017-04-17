/**
 * Copyright 2016 Gash.
 *
 * This file and intellectual content is protected under the Apache License, version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at:
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package gash.router.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Header;
import pipe.common.Common.Failure;
import pipe.common.Common.AppendLogItem;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.LocationList;
import pipe.common.Common.Log;
import pipe.common.Common.RemoveLogItem;
import pipe.common.Common.RequestAppendItem;
import pipe.common.Common.RequestRemoveItem;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import pipe.voteRequest.VoteRequest.VoteReq;
import pipe.voteRequest.VoteRequest.Results;
import gash.router.server.election.Leader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

import gash.router.server.election.Follower;
import gash.router.server.election.Candidate;

import pipe.appendEntries.AppendEntries.AppendEntry;
import pipe.appendEntries.AppendEntries.AppendEntriesResult;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ServerState state;
	protected boolean debug = false;

	public WorkHandler(ServerState state) {
		if (state != null) {
			this.state = state;
		}
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			// TODO add logging
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		//if (debug)
		PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?


		try {
			logger.info("entered the try");
			logger.info(Integer.toString(msg.getVrMsg().getCandidateId()));
			
			if (msg.getHeader().getDestination() == -1 && state.isLeader()) {
												
			} else if (msg.hasBeat()) {
				@SuppressWarnings("unused")
				Heartbeat hb = msg.getBeat();
				logger.debug("heartbeat from " + msg.getHeader().getNodeId());
			} else if (msg.hasPing()) {
				@SuppressWarnings("unused")
				//logger.info("ping from " + msg.getHeader().getNodeId());
				boolean p = msg.getPing();
				WorkMessage.Builder rb = WorkMessage.newBuilder();
				rb.setPing(true);
				channel.write(rb.build());

			} else if (msg.hasErr()) {
				@SuppressWarnings("unused")
				Failure err = msg.getErr();
				logger.error("failure from " + msg.getHeader().getNodeId());
				// PrintUtil.printFailure(err);
			} else if (msg.hasTask()) {
				@SuppressWarnings("unused")
				Task t = msg.getTask();
			} else if (msg.hasState()) {
				@SuppressWarnings("unused")
				WorkState s = msg.getState();
			}
			else if(msg.hasVrMsg()){
				logger.info("got vote request from node: "+ msg.getVrMsg().getCandidateId());
				logger.info("got vote request for term : "+ msg.getVrMsg().getTerm());
				logger.info("last log index recieved: " +msg.getVrMsg().getLastLogIndex());
				logger.info("last log term recieved: " + msg.getVrMsg().getLastLogTerm());

				 int receivedTerm=msg.getVrMsg().getTerm();
				 int thisTerm=state.getStatus().getCurrentTerm();
				 int recievedLogTerm=msg.getVrMsg().getLastLogTerm();
				 int thisLogTerm=state.getStatus().getLastTermInLog();
				 int receivedLogIndex = msg.getVrMsg().getLastLogIndex();
				 int thisLogIndex = state.getStatus().getCommitIndex();

				 state.getStatus().setHeartbeatTimeout(false);

					if(receivedTerm>=thisTerm && receivedTerm>=thisLogTerm){
						if(receivedLogIndex>=thisLogIndex){


							Header.Builder hb = Header.newBuilder();
							hb.setNodeId(state.getConf().getNodeId());
							hb.setDestination(msg.getVrMsg().getCandidateId());
							hb.setTime(System.currentTimeMillis());

							Results.Builder rb= Results.newBuilder();
							rb.setTerm(receivedTerm);
							rb.setVoteGranted(true);

							WorkMessage.Builder wm = WorkMessage.newBuilder();
							wm.setHeader(hb);
							wm.setVrResult(rb);
							wm.setSecret(121316549);

							// start timeout after voting
							if(state.getStatus().getFollower()){

							state.getStatus().setNextIndex(state.getStatus().getNextIndex()+1);
							state.getStatus().setHeartbeatTimeout(true);
							Follower follower =new Follower(state);
							Thread th = new Thread(follower);
							th.start();

						}

							channel.write(wm.build());
						}
					}
			}
			else if(msg.hasVrResult()){
				// this section deals with the response recived from the vote request in leader election
				logger.info("result of vote request recieved from node: "+msg.getHeader().getNodeId());
				logger.info("term voted for:" + msg.getVrResult().getTerm());
				logger.info("issucess: " + msg.getVrResult().getVoteGranted());

				//increase the vote count and decide whether it is the majority or not, if yes the ndeclare th enode to be leader
				//and change the serverstate

				 int senderNodeId =msg.getHeader().getNodeId();
				 int votedForTerm = msg.getVrResult().getTerm();
				 int thisNode=state.getConf().getNodeId();
				 boolean isSuccess=msg.getVrResult().getVoteGranted(); 
				 int currentTerm = state.getStatus().getCurrentTerm();

				if(isSuccess){
					//update the count 
					 int totalVotes=state.getStatus().getTotalVotesRecievedForThisTerm();
					 int totalNodes=state.getConf().getTotalNodes();
					 boolean majorityCount=false;
					state.getStatus().setTotalVotesRecievedForThisTerm(totalVotes+1);

					if(totalNodes%2==0){
						if(totalVotes+1>=(totalNodes/2)+1){
								majorityCount=true;
						}
					}
					else{
						if(totalVotes+1>=(totalNodes/2)){
							majorityCount=true;	
						}
					}

					if(majorityCount){

						//we received the majority count //declare the node leader // call AppendEntry messages

						state.getStatus().setFollower(false);
						state.getStatus().setCandidate(false);
						state.getStatus().setLeader(true);
						state.getStatus().setLeaderId(thisNode);

						Leader lead = new Leader(this.state);
						lead.sendAppendEntries();
						
					}
					else{
						// set a candidate state and start election again
						state.getStatus().setFollower(false);
						state.getStatus().setCandidate(true);
						state.getStatus().setLeader(false);
						state.getStatus().setLeaderId(0);
						state.getStatus().setTotalVotesRecievedForThisTerm(0);
						state.getStatus().setElectionTimeout(true);
						state.getStatus().setHeartbeatTimeout(false);
						state.getStatus().setNextIndex(0);
						state.getStatus().setPrevIndex(1);


						Candidate cn= new Candidate(state);
						cn.startElection();
					}

				}
				

			}
			
			else if(msg.hasAeMsg()){
				//when AppendEntry resonse id to be send back
				//TODO add other fault tolerant checks, update index and set look for hearbeat timeout

				logger.info("got vote request from node: "+ msg.getAeMsg().getLeaderId());
				logger.info("got vote request for term : "+ msg.getAeMsg().getTerm());
				logger.info("last log index recieved: " +msg.getAeMsg().getPrevLogIndex());
				logger.info("last log term recieved: " + msg.getAeMsg().getPrevLogTerm());


				//start the timer flag
				state.getStatus().setHeartbeatTimeout(false);


				 int recievedTerm=msg.getAeMsg().getTerm();
				 int thisTerm=state.getStatus().getCurrentTerm();
				 int recievedLogTerm=msg.getAeMsg().getPrevLogTerm();
				 int thisLogTerm=state.getStatus().getLastTermInLog();
				 int receivedLogIndex = msg.getAeMsg().getLeaderCommit();
				 int thisLogIndex = state.getStatus().getCommitIndex();
				 List<String> entry = msg.getAeMsg().getEntriesList();

				if(recievedTerm>=thisTerm && recievedTerm>=thisLogTerm){
					if(receivedLogIndex>=thisLogIndex){


						// create file if there is none else append the entry

						BufferedWriter bw = null;
						FileWriter fw = null;

						try{

							File file = new File(state.getDbPath()+"/appendEntryLog.csv");

							if (!file.exists()) {
								file.createNewFile();
							}

							fw = new FileWriter(file.getAbsoluteFile(), true);
							bw = new BufferedWriter(fw);

							for(int i=0;i<entry.size();i++){
								bw.write(entry.get(i));
								bw.write(",");
							}
							bw.write("\n");

							logger.info("Entry appended in Workhandler for hearbeat success");

						}
						catch(Exception e){
								e.printStackTrace();
						}
						finally{
							try {	
									//TODO mark the success flag so that we know which response to send to the lcient back
									if (bw != null)
										bw.close();

									if (fw != null)
										fw.close();

								} catch (Exception ex) {

									ex.printStackTrace();

								}
						}

						// if the log writing was successful return success msg or fail message

						Header.Builder hb = Header.newBuilder();
						hb.setNodeId(state.getConf().getNodeId());
						hb.setDestination(msg.getAeMsg().getLeaderId()); //send message back to leader who sent appendentry message
						hb.setTime(System.currentTimeMillis());

						AppendEntriesResult.Builder rb= AppendEntriesResult.newBuilder();
						rb.setTerm(recievedTerm);
						rb.setSuccess(true);

						WorkMessage.Builder wm = WorkMessage.newBuilder();
						wm.setHeader(hb);
						wm.setAeResult(rb);
						wm.setSecret(121316551);

						//start teh timer of the follwer

						if(state.getStatus().getFollower()){

							state.getStatus().setLeaderId(msg.getAeMsg().getLeaderId());
							state.getStatus().setNextIndex(state.getStatus().getNextIndex()+1);
							state.getStatus().setElectionTimeout(false);
							state.getStatus().setHeartbeatTimeout(true);
							Follower follower =new Follower(state);
							Thread th = new Thread(follower);
							th.start();

						}

						channel.write(wm.build());
					}
				}

				

			}
			else if(msg.hasAeResult()){
				//TODO if more than n/2 +1 success then

				logger.info("AppendEntryResutl recived in workhandler");
				logger.info("result of Append Etnry recieved from node: "+msg.getHeader().getNodeId());
				logger.info("term appended entry for:" + msg.getAeResult().getTerm());
				logger.info("issucess: " + msg.getAeResult().getSuccess());


				 int followerNodeId =msg.getHeader().getNodeId();
				 int thisNode=state.getConf().getNodeId();
				 int successForTerm = msg.getAeResult().getTerm();
				 int currentTerm = state.getStatus().getCurrentTerm();
				 boolean isSuccess=msg.getAeResult().getSuccess(); 
				

				if(isSuccess){
					//TODO what if half the node doesnt reply??
					
					logger.info("successful entry form above node");
				}

			}
			else if (msg.hasGetLog()) {
				logger.info("request log from: " + msg.getHeader().getNodeId());
				// sender want to the log!
				// build log message from hashTable
				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(state.getConf().getNodeId());
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(msg.getHeader().getNodeId());
				
				Log.Builder logmsg = Log.newBuilder();
				logmsg.putAllHashTable(ServerState.hashTable);
				// write log file back to sender
				channel.writeAndFlush(logmsg);
			} else if (msg.hasRequestAppend() && state.isLeader()) {
				// FOLLOWER want to append, ONLY LEADER should read this message
				RequestAppendItem request = msg.getRequestAppend();
				// get locationList from filename
				LocationList locationList = ServerState.hashTable.get(request.getFilename());
				// loop to get chunk_id, update the Node List associated with the chunk_id
				for(ChunkLocation chunkLoc : locationList.getLocationListList()) {
					if(chunkLoc.getChunkid() == request.getChunkId()) {
						chunkLoc.getNodeList().add(request.getNode());
					}
				}
				ServerState.hashTable.put(request.getFilename(), locationList);
				
				// append success, notify all FOLLOWERS
				// build append message to send out
				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(state.getConf().getNodeId());
				hb.setMaxHops(-1);
				hb.setTime(System.currentTimeMillis());
				
				AppendLogItem.Builder append = AppendLogItem.newBuilder();
				append.setFilename(request.getFilename());
				append.setChunkId(request.getChunkId());
				append.setNode(request.getNode());
				
				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setAppend(append);
				wb.setHeader(hb);
				// send append message to FOLLOWERS
				state.wmforward.addLast(wb.build());
			} else if (msg.hasRequestRemove() && state.isLeader()) {
				// FOLLOWER want to remove, ONLY LEADER should read this message
				RequestRemoveItem request = msg.getRequestRemove();
				ServerState.hashTable.remove(request.getFilename());
				// remove success, notify all FOLLOWERS
				// build remove message to send out
				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(state.getConf().getNodeId());
				hb.setMaxHops(-1);
				hb.setTime(System.currentTimeMillis());
				
				RemoveLogItem.Builder remove = RemoveLogItem.newBuilder();
				remove.setFilename(request.getFilename());
				
				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setRemove(remove);
				wb.setHeader(hb);
				
				// send remove message to FOLLOWERS
				state.wmforward.addLast(wb.build());
				
			} else if (msg.hasAppend() && msg.getHeader().getNodeId() == state.getCurrentLeader()) {
				// only leader should send out this message, check is from
				// leader?
				// get file, and locationList from msg and add to
				// hashTable
				AppendLogItem request = msg.getAppend();
				// get locationList from filename
				LocationList locationList = ServerState.hashTable.get(request.getFilename());
				// loop to get chunk_id, update the Node List associated with the chunk_id
				for(ChunkLocation chunkLoc : locationList.getLocationListList()) {
					if(chunkLoc.getChunkid() == request.getChunkId()) {
						chunkLoc.getNodeList().add(request.getNode());
					}
				}
				
			} else if (msg.hasRemove() && msg.getHeader().getNodeId() == state.getCurrentLeader()) {
				// only leader should send out this message, check is from
				// leader?
				// get filename from msg, remove the filename for hashTable
				RemoveLogItem item = msg.getRemove();
				ServerState.hashTable.remove(item.getFilename());
				
			}
			logger.info("gotcha you bastard");
		} catch (Exception e) {
			// TODO add logging
			logger.error("Exception: " + e.getMessage()); 
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();

	}

	/**
	 * a message was received from the server. Here we dispatch the message to
	 * the client's thread pool to minimize the time it takes to process other
	 * messages.
	 * 
	 * @param ctx
	 *            The channel the message was received from
	 * @param msg
	 *            The message
	 */
	@Override
	protected void channelRead0(ChannelHandlerContext ctx, WorkMessage msg) throws Exception {

		if(msg.getHeader().getDestination() == state.getConf().getNodeId() ||  msg.getHeader().getNodeId()!= state.getConf().getNodeId())
			handleMessage(msg, ctx.channel());
		else if (msg.getHeader().getDestination() == -1) {
			state.wmforward.addLast(msg);					// this is broadcast message, should have a look
			handleMessage(msg, ctx.channel());
		}
		else {
			// this is a private message for someone else, just forward it
				state.wmforward.addLast(msg);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}