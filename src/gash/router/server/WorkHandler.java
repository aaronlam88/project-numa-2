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
import pipe.work.Work.BeatResponse;
import pipe.voteRequest.VoteRequest.Results;
import gash.router.server.election.Leader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.util.List;
import java.util.Map;

import gash.router.server.election.Follower;
import gash.router.container.RoutingConf;
import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.election.Candidate;
import io.netty.util.ReferenceCountUtil;

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

	private void startStealing(WorkMessage msg) {
		int node_id = msg.getHeader().getNodeId();
		if (state.getConf().getRouting() != null && state.getPerformanceStat() < 50) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				if (e.getId() == node_id) {
					TaskStealer ts = new TaskStealer(e, 3, state);
					Thread t = new Thread(ts);
					t.start();
				}
			}
		}

	}

	/**
	 * check to see if we should discard WorkMessage msg
	 * 
	 * @param msg
	 * @return true: we don't need to care about this msg, discard it (return)
	 *         false: we have to read this msg or forward it.
	 */
	protected boolean shouldDiscard(WorkMessage msg) {
		Header header = msg.getHeader();
		int maxHop = header.getMaxHops();
		int src = header.getNodeId();

		if (maxHop == 0) {
			return true;
		}

		if (src == state.getConf().getNodeId()) {
			System.out.println("Message has come around");
			return true;
		}
		// the above cases should cover all the problems
		return false;
	}

	/**
	 * rebuild msg so it can be forward to other node, namely --maxHop
	 * 
	 * @param msg
	 * @return WorkMessage with new maxHop = old maxHop - 1
	 */
	protected WorkMessage rebuildMessage(WorkMessage msg) {
		Header header = msg.getHeader();
		int maxHop = header.getMaxHops();
		--maxHop;
		// build new header from old header, only update maxHop
		Header.Builder hb = Header.newBuilder();
		hb.mergeFrom(header);
		hb.setMaxHops(maxHop);

		// build new msg from old msg, only update Header
		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.mergeFrom(msg);
		wb.setHeader(hb.build());
		return wb.build();
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

		if (shouldDiscard(msg)) {
			return;
		}
		if (msg.getHeader().getDestination() == state.getConf().getNodeId()) {
			handleMessage(msg, ctx.channel());
		} else if (msg.getHeader().getDestination() == -1) {
			handleMessage(msg, ctx.channel());
			msg = rebuildMessage(msg);
			state.wmforward.addLast(msg);
		} else {
			msg = rebuildMessage(msg);
			state.wmforward.addLast(msg);
		}

	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */
	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			System.out.println("ERROR: Unexpected content - " + msg);
			return;
		}

		//PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?

		try {
			//System.out.println("entered the try");
			// System.out.println(Integer.toString(msg.getVrMsg().getCandidateId()));

			if (msg.hasBeat()) {
				@SuppressWarnings("unused")
				Heartbeat gb = msg.getBeat();

				// System.out.println(msg.toString());
				System.out.println("Heartbeat from " + msg.getHeader().getNodeId());

				int cpuUsage = msg.getBeat().getState().getEnqueued();
				if (cpuUsage > state.CPUthreshhold) {
					startStealing(msg);
				}

				// retrieve requestType and work accordingly
				// if request send response; if ersponse update the count

				//System.out.println("recieved beat request");

				
				// * if (mt == 1) { // A request heartbeat message
//				 WorkState.Builder sb = WorkState.newBuilder();
//				 sb.setEnqueued(state.getPerformanceStat());
//				 sb.setProcessed(-1);
				 

				BeatResponse.Builder bb = BeatResponse.newBuilder();
				// bb.setState(sb);
				bb.setResponse(2);

				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(state.getConf().getNodeId());
				hb.setTime(System.currentTimeMillis());
				// hb.setMaxHops(state.getConf().getTotalNodes());
				hb.setDestination(msg.getHeader().getNodeId());

				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setHeader(hb);
				wb.setBeatReply(bb);
				wb.setSecret(121316552);

				state.wmforward.addLast(wb.build());
//				channel.writeAndFlush(wb.build()); //TODO

			} else if (msg.hasBeatReply()) {

				//System.out.println(msg.toString());

//				System.out.println(
//						"recieved beat response;inside hasReply(); sender node id: " + msg.getHeader().getNodeId());

				if (state.getStatus().getNodesThatRepliedBeats().contains(msg.getHeader().getNodeId())) {
					// do nothing
					//System.out.println("Message from this node already considered; doing nothing to process");
				} else {
//					System.out.println("heartbeat reply received; addding node to disocverednode list");
					state.getStatus().setNodesThatRepliedBeatsInList(msg.getHeader().getNodeId());

					int gtnd = state.getStatus().getTotalNodesDiscovered();
					state.getStatus().setTotalNodesDiscovered(gtnd + 1);

				}
			} else if (msg.hasAddEdge()) {
				int id = msg.getAddEdge().getNodeToAdd();
				String host = msg.getAddEdge().getHost();
				int port = msg.getAddEdge().getPort();
				int command = msg.getAddEdge().getCommand();
				RoutingEntry re = new RoutingEntry(id, host, port, command);

				RoutingConf rc = new RoutingConf();
				rc.addEntry(re);

//				System.out.println("new entry added to the prevous node of newly added node");
//				System.out.println("message to add sent by: " + msg.getHeader().getNodeId());
//				System.out.println("Edge added to: " + state.getConf().getNodeId());
			} else if (msg.hasPing()) {
				@SuppressWarnings("unused")
				// logger.info("ping from " + msg.getHeader().getNodeId());
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

			} else if (msg.hasVrMsg()) {
				System.out.println("got vote request from node: " + msg.getVrMsg().getCandidateId());
				System.out.println("got vote request for term : " + msg.getVrMsg().getTerm());
				System.out.println("last log index recieved: " + msg.getVrMsg().getLastLogIndex());
				System.out.println("last log term recieved: " + msg.getVrMsg().getLastLogTerm());

				int receivedTerm = msg.getVrMsg().getTerm();
				int thisTerm = state.getStatus().getCurrentTerm();
				//int recievedLogTerm = msg.getVrMsg().getLastLogTerm();
				int thisLogTerm = state.getStatus().getLastTermInLog();
				int receivedLogIndex = msg.getVrMsg().getLastLogIndex();
				int thisLogIndex = state.getStatus().getCommitIndex();

				state.getStatus().setElectionTimeout(false);
				state.getStatus().setHeartbeatTimeout(true);

				if (receivedTerm >= thisTerm && receivedTerm >= thisLogTerm) {
					if (receivedLogIndex >= thisLogIndex) {

//						System.out.println("conditions in vote request is approvable by this server");

						Header.Builder hb = Header.newBuilder();
						hb.setNodeId(state.getConf().getNodeId());
						hb.setDestination(msg.getVrMsg().getCandidateId());
						hb.setTime(System.currentTimeMillis());

						Results.Builder rb = Results.newBuilder();
						rb.setTerm(receivedTerm);
						rb.setVoteGranted(true);

						WorkMessage.Builder wm = WorkMessage.newBuilder();
						wm.setHeader(hb);
						wm.setVrResult(rb);
						wm.setSecret(121316549);

						// start timeout after voting
						if (!state.getStatus().isIsVotedFor() && !state.getStatus().getLeader()) {

//							System.out.println("sending the message back to requesting node");
							state.getStatus().setFollower(true);
							state.getStatus().setNextIndex(state.getStatus().getNextIndex() + 1);
							state.getStatus().setHeartbeatTimeout(true);

							state.getStatus().setFollower(true);
							state.getStatus().setCandidate(false);
							state.getStatus().setLeader(false);
							state.getStatus().setIsVotedFor(true);

							WorkMessage pr = wm.build();

							System.out.println(pr.toString());
							state.wmforward.addLast(pr);
//							channel.writeAndFlush(pr);

						}

						Follower follower = new Follower(state);
						Thread th = new Thread(follower);
						th.start();

					}
				}
			} else if (msg.hasVrResult()) {
				// this section deals with the response recived from the vote
				// request in leader election
				// we received the majority count //declare the node leader //
				// call AppendEntry messages
				System.out.println("result of vote request recieved from node: " + msg.getHeader().getNodeId());
				System.out.println("term voted for:" + msg.getVrResult().getTerm());
				System.out.println("issucess: " + msg.getVrResult().getVoteGranted());

				// increase the vote count and decide whether it is the majority
				// or not, if yes the ndeclare th enode to be leader
				// and change the serverstate

				int senderNodeId = msg.getHeader().getNodeId();
				int votedForTerm = msg.getVrResult().getTerm();
				int thisNode = state.getConf().getNodeId();
				boolean isSuccess = msg.getVrResult().getVoteGranted();
				int currentTerm = state.getStatus().getCurrentTerm();

				if (isSuccess) {
					// update the count
					int totalVotes = state.getStatus().getTotalVotesRecievedForThisTerm();
					int totalNodes = state.getConf().getTotalNodes();
					boolean majorityCount = false;
					state.getStatus().setTotalVotesRecievedForThisTerm(totalVotes + 1);

					if (totalNodes % 2 == 0) {
						if (totalVotes + 1 >= (totalNodes / 2) + 1) {
							majorityCount = true;
						}
					} else {
						if (totalVotes + 1 >= (totalNodes / 2)) {

							majorityCount = true;
						}
					}

					if (majorityCount) {

						// we received the majority count //declare the node
						// leader // call AppendEntry messages
						System.out
								.println("we recieved majority of the count and declaring leader: nodeid: " + thisNode);
						System.out.println("");
						System.out.println("setting voted_for boolean as false");

						state.getStatus().setIsVotedFor(false);
						state.getStatus().setFollower(false);
						state.getStatus().setCandidate(false);
						state.getStatus().setLeader(true);
						state.getStatus().setLeaderId(thisNode);
						state.getStatus().setCurrentTerm(votedForTerm);
						state.getStatus().setLastTermInLog(votedForTerm - 1);
						state.getStatus().setElectionTimeout(false);
						state.getStatus().setHeartbeatTimeout(false);

						Leader lead = new Leader(state);
						Thread t = new Thread(lead);
						t.run();

					} else {
						// set a candidate state and start election again
						System.out.println("we didnt recieve majority in the vote count");
						state.getStatus().setFollower(false);
						state.getStatus().setCandidate(true);
						state.getStatus().setLeader(false);
						state.getStatus().setLeaderId(0);
						state.getStatus().setTotalVotesRecievedForThisTerm(0);
						state.getStatus().setElectionTimeout(true);
						state.getStatus().setHeartbeatTimeout(false);
						state.getStatus().setNextIndex(0);
						state.getStatus().setPrevIndex(1);

						Candidate cn = new Candidate(state);
						Thread t = new Thread(cn);
						t.run();
					}

				}

			}

			else if (msg.hasAeMsg()) {
				// when AppendEntry resonse id to be send back
				// TODO add other fault tolerant checks, update index and set
				// look for hearbeat timeout

				System.out.println("got appendEntry from node: " + msg.getAeMsg().getLeaderId());
				System.out.println("got appendEntry for term : " + msg.getAeMsg().getTerm());
				System.out.println("leader commit recieved: " + msg.getAeMsg().getLeaderCommit());
				System.out.println("last log term recieved: " + msg.getAeMsg().getPrevLogTerm());

				// start the timer flag
				state.getStatus().setHeartbeatTimeout(false);
				state.getStatus().setElectionTimeout(false);

				int recievedTerm = msg.getAeMsg().getTerm();
				int thisTerm = state.getStatus().getCurrentTerm();
				int recievedLogTerm = msg.getAeMsg().getPrevLogTerm();
				int thisLogTerm = state.getStatus().getLastTermInLog();
				int receivedLogIndex = msg.getAeMsg().getLeaderCommit();
				int thisLogIndex = state.getStatus().getCommitIndex();
				List<String> entry = msg.getAeMsg().getEntriesList();

				if (recievedTerm >= thisTerm && recievedTerm >= thisLogTerm) {
					if (receivedLogIndex >= thisLogIndex) {

						// update the realted fields, update commit index. if we
						// use 2 phase commit, we will have last applied index
						// as well

						state.getStatus().setCommitIndex(thisLogIndex + 1);

						// create file if there is none else append the entry

						BufferedWriter bw = null;
						FileWriter fw = null;

						try {

							/*
							 * System.out.println("creating file"); File file =
							 * new File(state.getDbPath() +
							 * "/appendEntryLog.txt");
							 * 
							 * if (!file.exists()) { file.createNewFile(); }
							 * 
							 * fw = new FileWriter(file.getAbsoluteFile(),
							 * true); bw = new BufferedWriter(fw);
							 * 
							 * for (int i = 0; i < entry.size(); i++) {
							 * bw.write(entry.get(i)); bw.write(","); }
							 * bw.write("\n"); bw.flush();
							 */

							PrintWriter pw = new PrintWriter(new File(
									state.getDbPath() + "/appendEntryLog_" + state.getConf().getNodeId() + ".txt"));

							StringBuilder sb = new StringBuilder();

							for (int i = 0; i < entry.size(); i++) {
								sb.append(entry.get(i));
								sb.append(",");
							}

							sb.append("\n");

							pw.write(sb.toString());

							pw.close();

							System.out.println("Entry appended in Workhandler for hearbeat success");

						} catch (Exception e) {
							e.printStackTrace();
						} finally {
							try {
								// TODO mark the success flag so that we know
								// which response to send to the lcient back
								if (bw != null)
									bw.close();

								if (fw != null)
									fw.close();

							} catch (Exception ex) {

								ex.printStackTrace();

							}
						}

						// if the log writing was successful return success msg
						// or fail message

						Header.Builder hb = Header.newBuilder();
						hb.setNodeId(state.getConf().getNodeId());

						// send message back to leader who sent append entry
						// message
						hb.setDestination(msg.getAeMsg().getLeaderId());

						hb.setTime(System.currentTimeMillis());

						AppendEntriesResult.Builder rb = AppendEntriesResult.newBuilder();
						rb.setTerm(recievedTerm);
						rb.setSuccess(true);

						WorkMessage.Builder wm = WorkMessage.newBuilder();
						wm.setHeader(hb);
						wm.setAeResult(rb);
						wm.setSecret(121316551);

						// start teh timer of the follwer

						if (state.getStatus().getFollower()) {

							state.getStatus().setLeaderId(msg.getAeMsg().getLeaderId());
							state.getStatus().setNextIndex(state.getStatus().getNextIndex() + 1);
							state.getStatus().setElectionTimeout(false);
							state.getStatus().setHeartbeatTimeout(true);
							Follower follower = new Follower(state);
							Thread th = new Thread(follower);
							th.start();

						}

//						channel.writeAndFlush(wm.build());
						state.wmforward.addLast(wm.build());
					}
				}

			} else if (msg.hasAeResult()) {
				// TODO if more than n/2 +1 success then

				System.out.println("append entry recieved");
				System.out.println(msg.toString());

				System.out.println("AppendEntryResutl recived in workhandler");
				System.out.println("result of Append Etnry recieved from node: " + msg.getHeader().getNodeId());
				System.out.println("term appended entry for:" + msg.getAeResult().getTerm());
				System.out.println("issucess: " + msg.getAeResult().getSuccess());

				// int followerNodeId = msg.getHeader().getNodeId();
				// int thisNode = state.getConf().getNodeId();
				// int successForTerm = msg.getAeResult().getTerm();
				// int currentTerm = state.getStatus().getCurrentTerm();
				// boolean isSuccess = msg.getAeResult().getSuccess();
				int totalNodes = state.getConf().getTotalNodes();
				int totalSuccess = state.getStatus().getTotalAppendEntrySuccessForThisTerm();

				state.getStatus().setTotalAppendEntrySuccessForThisTerm(totalSuccess + 1);

				boolean majorityCount = false;

				if (totalNodes % 2 == 0) {
					if (totalSuccess + 1 >= (totalNodes / 2) + 1) {
						majorityCount = true;
					}
				} else {
					if (totalSuccess + 1 >= (totalNodes / 2)) {
						majorityCount = true;
					}
				}

				if (!majorityCount) {

					System.out.println("not majority success in AppendEntry");
					state.getStatus().setFollower(false);
					state.getStatus().setCandidate(true);
					state.getStatus().setLeader(false);
					state.getStatus().setLeaderId(0);
					state.getStatus().setTotalVotesRecievedForThisTerm(0);
					state.getStatus().setElectionTimeout(true);
					state.getStatus().setHeartbeatTimeout(false);
					state.getStatus().setNextIndex(0);
					state.getStatus().setPrevIndex(1);

					Candidate cn = new Candidate(state);
					Thread t = new Thread(cn);
					t.run();

				} else {
					// set few terms
				}

			}

			else if (msg.hasGetLog()) {
				System.out.println("request log from: " + msg.getHeader().getNodeId());
				// sender want to the log!
				// build log message from hashTable
				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(state.getConf().getNodeId());
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(msg.getHeader().getNodeId());
				Log.Builder logmsg = Log.newBuilder();

				for (Map.Entry<String, LocationList.Builder> entry : ServerState.hashTable.entrySet()) {
					logmsg.putHashTable(entry.getKey(), entry.getValue().build());
				}

				// write log file back to sender
//				state.wmforward.addLast(logmsg);
				channel.writeAndFlush(logmsg);
			} else if (msg.hasRequestAppend() && state.isLeader()) {
				// FOLLOWER want to append, ONLY LEADER should read this message
				RequestAppendItem request = msg.getRequestAppend();
				// get locationList from filename
				LocationList.Builder locationList = ServerState.hashTable.get(request.getFilename());
				// loop to get chunk_id, update the Node List associated with

				// the chunk_id

				if (locationList == null) {

					LocationList.Builder lb = LocationList.newBuilder();
					ChunkLocation.Builder cb = ChunkLocation.newBuilder();
					cb.addNode(request.getNode());
					lb.setLocationList(request.getChunkId(), cb.build());
					ServerState.hashTable.put(request.getFilename(), lb);
					return;
				} else {
					for (ChunkLocation chunkLoc : locationList.getLocationListList()) {
						if (chunkLoc.getChunkId() == request.getChunkId()) {
							chunkLoc.getNodeList().add(request.getNode());
						}
					}
					ServerState.hashTable.put(request.getFilename(), locationList);
				}

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
				LocationList.Builder locationList = ServerState.hashTable.get(request.getFilename());
				// loop to get chunk_id, update the Node List associated with
				// the chunk_id

				if (locationList == null) {
					ChunkLocation.Builder cb = ChunkLocation.newBuilder();
					cb.addNode(request.getNode());
					LocationList.Builder lb = LocationList.newBuilder();
					lb.addLocationList(cb.build());
					ServerState.hashTable.put(request.getFilename(), lb);
					return;

				}

				for (ChunkLocation chunkLoc : locationList.getLocationListList()) {
					if (chunkLoc.getChunkId() == request.getChunkId()) {
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
			// logger.info("gotcha you bastard");
		} catch (Exception e) {
			// TODO add logging
			logger.error("Exception: " + e.getMessage());
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(state.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			WorkMessage.Builder rb = WorkMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.writeAndFlush(rb.build());
		}

		System.out.flush();

	}
}