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
import pipe.common.Common.AppendLogItem;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.LocationList;
import pipe.common.Common.Log;
import pipe.common.Common.RemoveLogItem;
import pipe.common.Common.RequestAppendItem;
import pipe.common.Common.RequestRemoveItem;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.Task;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * replace println with logging!
 * 
 * @author gash
 * 
 */
public class WorkHandler extends SimpleChannelInboundHandler<WorkMessage> {
	protected static Logger logger = LoggerFactory.getLogger("work");
	protected ServerState serverState;
	protected boolean debug = true;

	public WorkHandler(ServerState serverState) {
		if (serverState != null) {
			this.serverState = serverState;
		}
	}

	/**
	 * override this method to provide processing behavior. T
	 * 
	 * @param msg
	 */

	public void handleMessage(WorkMessage msg, Channel channel) {
		if (msg == null) {
			logger.error("ERROR: Unexpected content - " + msg);
			return;
		}

		if (debug)
			PrintUtil.printWork(msg);

		// TODO How can you implement this without if-else statements?
		try {
			if (msg.getHeader().getDestination() == -1 && serverState.isLeader()) {
				// this is a broadcast message, leader will ignore this
				// DO NOTHING
			} else if (msg.hasBeat()) {
				@SuppressWarnings("unused")
				Heartbeat hb = msg.getBeat();
				logger.info("heartbeat from " + msg.getHeader().getNodeId());
			} else if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
				@SuppressWarnings("unused")
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
			} else if (msg.hasGetLog()) {
				logger.info("request log from: " + msg.getHeader().getNodeId());
				// sender want to the log!
				// build log message from hashTable
				Header.Builder hb = Header.newBuilder();
				hb.setNodeId(serverState.getConf().getNodeId());
				hb.setTime(System.currentTimeMillis());
				hb.setDestination(msg.getHeader().getNodeId());
				
				Log.Builder logmsg = Log.newBuilder();
				logmsg.putAllHashTable(ServerState.hashTable);
				// write log file back to sender
				channel.writeAndFlush(logmsg);
			} else if (msg.hasRequestAppend() && serverState.isLeader()) {
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
				
				// append success, notify all FOLLOWERS
				// build append message to send out
				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(serverState.getConf().getNodeId());
				hb.setMaxHops(-1);
				
				AppendLogItem.Builder append = AppendLogItem.newBuilder();
				append.setFilename(request.getFilename());
				append.setChunkId(request.getChunkId());
				append.setNode(request.getNode());
				
				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setAppend(append);
				wb.setHeader(hb);
				// send append message to FOLLOWERS
				serverState.wmforward.addLast(wb.build());
			} else if (msg.hasRequestRemove() && serverState.isLeader()) {
				// FOLLOWER want to remove, ONLY LEADER should read this message
				RequestRemoveItem request = msg.getRequestRemove();
				ServerState.hashTable.remove(request.getFilename());
				// remove success, notify all FOLLOWERS
				// build remove message to send out
				Header.Builder hb = Header.newBuilder();
				hb.setDestination(-1);
				hb.setNodeId(serverState.getConf().getNodeId());
				hb.setMaxHops(-1);
				
				RemoveLogItem.Builder remove = RemoveLogItem.newBuilder();
				remove.setFilename(request.getFilename());
				
				WorkMessage.Builder wb = WorkMessage.newBuilder();
				wb.setRemove(remove);
				wb.setHeader(hb);
				
				// send remove message to FOLLOWERS
				serverState.wmforward.addLast(wb.build());
				
			} else if (msg.hasAppend() && msg.getHeader().getNodeId() == serverState.getCurrentLeader()) {
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
				
			} else if (msg.hasRemove() && msg.getHeader().getNodeId() == serverState.getCurrentLeader()) {
				// only leader should send out this message, check is from
				// leader?
				// get filename from msg, remove the filename for hashTable
				RemoveLogItem item = msg.getRemove();
				ServerState.hashTable.remove(item.getFilename());
				
			}
		} catch (Exception e) {
			logger.error("Exception: " + e.getMessage());
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(serverState.getConf().getNodeId());
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
		if(msg.getHeader().getDestination() == serverState.getConf().getNodeId())
			handleMessage(msg, ctx.channel());
		else
			serverState.wmforward.addLast(msg);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}