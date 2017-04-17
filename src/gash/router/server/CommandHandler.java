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
import pipe.common.Common.Chunk;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.LocationList;
import pipe.common.Common.ReadResponse;

import pipe.common.Common.Request;
import pipe.common.Common.Response;
import pipe.common.Common.ResponseStatus;
import pipe.common.Common.TaskType;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

import com.google.protobuf.ByteString;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import java.io.*;
import java.nio.file.Paths;

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
		System.out.println("Channel Read");
		if(msg.getHeader().getDestination() == serverState.getConf().getNodeId()){
			long sequence = ringBuffer.next(); // Grab the next sequence
			try {
				CommandMessageEvent event = ringBuffer.get(sequence);
				event.set(msg, ctx.channel()); // Fill with data
			} finally {
				ringBuffer.publish(sequence);
			}
		}else{
			serverState.cmforward.addLast(msg);
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

	private void processReadRequest(CommandMessage msg, Channel channel) throws Exception {
		Request req = msg.getReq();

		if (req.hasRrb()) {

			FileInputStream fin = null;
			File file = null;

			Header.Builder hd = Header.newBuilder();
			hd.setDestination(msg.getHeader().getNodeId());
			hd.setNodeId(serverState.getConf().getNodeId());

			Chunk.Builder ch = Chunk.newBuilder();
			ch.setChunkId((int) req.getRrb().getChunkId());

			ReadResponse.Builder rrb = ReadResponse.newBuilder();
			rrb.setFilename(req.getRrb().getFilename());

			Response.Builder rsp = Response.newBuilder();
			rsp.setReadResponse(rrb);
			rsp.setFilename(req.getRrb().getFilename());
			rsp.setAck(ResponseStatus.Success);
			rsp.setResponseType(TaskType.READFILE);

			CommandMessage.Builder cm = CommandMessage.newBuilder();
			cm.setHeader(hd);
			cm.setResp(rsp);

			if (req.getRrb().hasChunkId()) {
				// send the chunk data in response
				// send failure if chunk not found
				try {
					String chunkName = new String(req.getRrb().getFilename() + "." + req.getRrb().getChunkId());
					file = new File(Paths.get(serverState.getDbPath(), chunkName).toString());
					fin = new FileInputStream(file);
					byte fileContent[] = new byte[(int) file.length()];
					fin.read(fileContent);
					ch.setChunkData(ByteString.copyFrom(fileContent));
					rrb.setChunk(ch);

				} catch (Exception e) {
					System.out.println("Error exception" + e);
					rsp.setAck(ResponseStatus.Fail);
				} finally {
					fin.close();
					file = null;
					fin = null;
				}
			} else {
				// send file and chunk locations from log in
				// response
				// send failure file not found

				// read locations form hashtable and send to client
				LocationList locationList= ServerState.hashTable.get(req.getRrb().getFilename());

				if (locationList != null) {
					
					for (ChunkLocation chunkLocation : locationList.getLocationListList()) {
						rrb.setChunkLocation(chunkLocation.getChunkid(), chunkLocation);
					}
				} else {
					rsp.setAck(ResponseStatus.Fail);
				}
			}
			channel.writeAndFlush(cm.build());
		} else {
			throw new Exception("Invalid message type");
		}
	}

	public void processWriteRequest(CommandMessage msg, Channel channel) throws Exception {
		Request req = msg.getReq();
		if (req.hasRwb()) {
			if (req.getRwb().hasChunk()) {
				
				// save chunk data on local fs
				// Send a event to worker thread about pending log
				// update
				FileOutputStream fout = null;
				File file = null;
				try{
					String chunkName = new String(req.getRwb().getFilename() + "." + req.getRwb().getChunk().getChunkId());
					file = new File(Paths.get(serverState.getDbPath(), chunkName).toString());
					file.createNewFile();
					fout = new FileOutputStream(file);
					fout.write(req.getRwb().getChunk().getChunkData().toByteArray());
					
					FileChunkObject nod = new FileChunkObject();
					nod.setNode_id(serverState.getConf().getNodeId());
					nod.setHostAddress(serverState.getConf().getHostAddress());
					nod.setPort_id(serverState.getConf().getCommandPort());
					nod.setChunk_id(req.getRwb().getChunk().getChunkId());
					nod.setFileName(req.getRwb().getFilename());
					
					serverState.incoming.addLast(nod);
					
					System.out.println("File writeing " + chunkName);
				}catch(Exception e){
					System.out.println("Error exception" + e);
				}finally{
					System.out.println("File write ends");
					fout.close();
				}
			} else {
				// TODO send failure message - no chunk data
			}
		} else {
			throw new Exception("Invalid message type");
		}
	}

	public void onEvent(CommandMessageEvent event, long sequence, boolean endOfBatch) {
		System.out.println("OnEvent");
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
				boolean p = msg.getPing();
				CommandMessage.Builder rb = CommandMessage.newBuilder();
				rb.setPing(true);
				channel.writeAndFlush(rb.build());
			} else if (msg.hasMessage()) {
				logger.info(msg.getMessage());
			} else if (msg.hasReq()) {
				Request req = msg.getReq();
				switch (req.getRequestType()) {
				case READFILE:
					processReadRequest(msg, channel);

					break;

				case WRITEFILE:
					processWriteRequest(msg, channel);
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
					case WRITEFILE:
						if (res.hasAck()) {
							if (res.getAck() == ResponseStatus.Fail) {
								// TODO send chunk data that is not received by
								// client for given chunk ids in response
							}
						}
						break;
					default:
						break;
				}
			} else {
				throw new Exception("Invalid message type");

			}

		} catch (Exception e) {
			Failure.Builder eb = Failure.newBuilder();
			eb.setId(this.serverState.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());
			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setErr(eb);
			channel.writeAndFlush(rb.build());
		}

		System.out.flush();
		channel.close();
	}
}