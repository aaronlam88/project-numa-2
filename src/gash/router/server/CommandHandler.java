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

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//import gash.router.container.RoutingConf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import pipe.common.Common.Chunk;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.Failure;
import pipe.common.Common.GetLog;
import pipe.common.Common.Node;
import pipe.common.Common.Request;
import pipe.common.Common.Response;
import pipe.common.Common.ResponseStatus;
import routing.Pipe.CommandMessage;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;

/**
 * The message handler processes json messages that are delimited by a 'newline'
 * 
 * TODO replace println with logging!
 * 
 * @author gash
 * 
 */
public class CommandHandler extends SimpleChannelInboundHandler<CommandMessage> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected ServerState serverState;
	protected RingBuffer<CommandMessageEvent> ringBuffer;

	public CommandHandler(ServerState serverState, RingBuffer<CommandMessageEvent> ringBuffer) {
		this.ringBuffer = ringBuffer;
		if (serverState != null) {
			this.serverState = serverState;
		}
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
	protected void channelRead0(ChannelHandlerContext ctx, CommandMessage msg) throws Exception {
		long sequence = ringBuffer.next(); // Grab the next sequence
		try {
			CommandMessageEvent event = ringBuffer.get(sequence);
			event.set(msg, ctx.channel()); // Fill with data
		} finally {
			ringBuffer.publish(sequence);
		}
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		logger.error("Unexpected exception from downstream.", cause);
		ctx.close();
	}

}

class CommandMessageEvent {
	public CommandMessage msg;
	public Channel channel;

	public void set(CommandMessage msg, Channel chnl) {
		this.msg = msg;
		this.channel = chnl;
	}
}

class CommandMessageEventFactory implements EventFactory<CommandMessageEvent> {
	public CommandMessageEvent newInstance() {
		return new CommandMessageEvent();
	}
}

class CommandMessageEventHandler implements EventHandler<CommandMessageEvent> {
	protected static Logger logger = LoggerFactory.getLogger("cmd");
	protected ServerState serverState;

	CommandMessageEventHandler(ServerState serverState) {
		this.serverState = serverState;
	}

	public void onEvent(CommandMessageEvent event, long sequence, boolean endOfBatch) {
		CommandMessage msg = event.msg;
		Channel channel = event.channel;
		if (msg == null) {
			System.out.println("ERROR: Unexpected content - " + msg);
			logger.error("ERROR: Unexpected content - " + msg);
			return;
		}

		PrintUtil.printCommand(msg);

		try {
			if (msg.hasPing()) {
				logger.info("ping from " + msg.getHeader().getNodeId());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			} else if (msg.hasReq()) {
				Request req = msg.getReq();
				switch (req.getRequestType()) {
				case READFILE:
					if (req.hasRrb()) {
						if (req.getRrb().hasChunkId()) {
							
						} else {
							// TODO send file and chunk locations from log in
							// response
							// send back log.get(req.getRrb().getChunkId());
							// TODO send failure file not found
						}
					} else {
						// TODO send failure - Invalid read request
					}
					break;
				case WRITEFILE:
					if (req.hasRwb()) {
						if (req.getRwb().hasChunk()) {
							// TODO save chunk data on local fs
						} else {
							// TODO send failure message - no chunk data
						}
					} else {
						// TODO send failure message - Invalid write request
					}
					break;
				// case DELETEFILE:
				//
				// break;
				// case UPDATEFILE:
				//
				// break;
				default:
					break;
				}

			} else if (msg.hasResp()) {
				Response res = msg.getResp();
				switch (res.getResponseType()) {
				// case READFILE:
				// if(res.hasRrb()){
				// if(res.getRrb().hasChunkId()){
				// //TODO send the chunk in response
				// //TODO send failure chunk not found
				// }else{
				// //TODO send file and chunk locations from log in response
				// //TODO send failure file not found
				// }
				// }else{
				// //TODO send failure - Invalid read request
				// }
				// break;
				case WRITEFILE:
					if (res.hasAck()) {
						if (res.getAck() == ResponseStatus.Fail) {
							// TODO send chunk data that is not received by
							// client for given chunk ids in response
						}
					}
					break;
				// case DELETEFILE:
				//
				// break;
				// case UPDATEFILE:
				//
				// break;
				default:
					break;
				}
			} else if (msg.hasGetLog()) {
				//TODO return log file
				GetLog.Builder lb = GetLog.newBuilder();
			} else if (msg.hasAddChunk()) {
				//TODO only leader should send out this message, check is from leader?
				//TODO get chunk_id, and chunk_location from msg and add to hashTable
			} else if (msg.hasRemoveChunk()) {
				//TODO only leader should send out this message, check is from leader?
				//TODO get chunk_id from msg, remove the chunk_id for hashTable
			}
			else {
				// unrecognised message
			}

		} catch (Exception e) {
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(this.serverState.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.write(rb.build());
		}

		System.out.flush();
	}
}