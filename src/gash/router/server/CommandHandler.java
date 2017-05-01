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
import pipe.common.Common.Chunk;
import pipe.common.Common.ChunkLocation;
import pipe.common.Common.Failure;
import pipe.common.Common.Header;
import pipe.common.Common.LocationList;
import pipe.common.Common.Node;
import pipe.common.Common.ReadResponse;

import pipe.common.Common.Request;
import pipe.common.Common.Response;
import pipe.common.Common.TaskType;
import pipe.work.Work.WorkMessage;
import routing.Pipe.CommandMessage;

import com.google.protobuf.ByteString;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import java.io.*;
import java.nio.file.Paths;
import java.util.List;

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
	 * check to see if we should discard WorkMessage msg
	 * 
	 * @param msg
	 * @return true: we don't need to care about this msg, discard it (return)
	 *         false: we have to read this msg or forward it.
	 */
	protected boolean shouldDiscard(CommandMessage msg) {
		// System.out.println(msg);
		Header header = msg.getHeader();
		int maxHop = header.getMaxHops();
		int src = header.getNodeId();
		// long time = header.getTime();

		// if max hop == 0, discard
		if (maxHop == 0) {
			return true;
		}
		if (src == serverState.getConf().getNodeId()) {
			System.out.println("Message has come around");
			return true;
		}

		// the above cases should cover all the problems

		return false;
	}

	/**
	 * rebuild msg so it can be forward to other node, namely --maxHop
	 * 
	 * 
	 * @param msg
	 * @return WorkMessage with new maxHop = old maxHop - 1
	 */
	protected CommandMessage rebuildMessage(CommandMessage msg) {
		Header header = msg.getHeader();
		int maxHop = header.getMaxHops();
		--maxHop;
		// build new header from old header, only update maxHop
		Header.Builder hb = Header.newBuilder();
		hb.mergeFrom(header);
		hb.setMaxHops(maxHop);

		// build new msg from old msg, only update Header
		CommandMessage.Builder cb = CommandMessage.newBuilder();
		cb.mergeFrom(msg);
		cb.setHeader(hb.build());
		return cb.build();
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
		if (shouldDiscard(msg)) {
			return;
		}
		// if (msg.hasPing()) {
		// System.out.println("Ping from " + msg.getHeader().getNodeId());
		//System.out.println(msg);
		// }
		PrintUtil.printCommand(msg);
		// System.out.println(msg.toString());
		if (msg.getHeader().getNodeId() == serverState.client_id) {
			// client node id
			serverState.getEmon().addClientEdge(msg.getHeader().getNodeId(), ctx.channel());
		}

		if (!msg.getHeader().hasDestination() || msg.getHeader().getDestination() == serverState.getConf().getNodeId()
				|| msg.getHeader().getDestination() == 3) {
			long sequence = ringBuffer.next(); // Grab the next sequence
			try {
				CommandMessageEvent event = ringBuffer.get(sequence);
				event.set(msg, ctx.channel()); // Fill with data
			} finally {
				ringBuffer.publish(sequence);
			}
		} else {
			msg = rebuildMessage(msg);
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
		Request req = msg.getRequest();

		if (req.hasRrb()) {

			FileInputStream fin = null;
			File file = null;

			Header.Builder hd = Header.newBuilder();
			hd.setDestination(msg.getHeader().getNodeId());
			hd.setNodeId(serverState.getConf().getNodeId());
			hd.setTime(System.currentTimeMillis());

			Chunk.Builder ch = Chunk.newBuilder();
			ch.setChunkId((int) req.getRrb().getChunkId());

			ReadResponse.Builder rrb = ReadResponse.newBuilder();
			rrb.setFilename(req.getRrb().getFilename());

			Response.Builder rsp = Response.newBuilder();
			rsp.setFilename(req.getRrb().getFilename());
			rsp.setStatus(Response.Status.SUCCESS);
			rsp.setResponseType(TaskType.RESPONSEREADFILE);

			CommandMessage.Builder cm = CommandMessage.newBuilder();
			cm.setHeader(hd);

			if (req.getRrb().hasChunkId()) {
				// send the chunk data in response
				// send failure if chunk not found
				System.out.println("Collecting chunk and sending it");
				try {
					String chunkName = new String(req.getRrb().getFilename() + "." + req.getRrb().getChunkId());
					file = new File(Paths.get(serverState.getDbPath(), chunkName).toString());
					fin = new FileInputStream(file);
					byte fileContent[] = new byte[(int) file.length()];
					fin.read(fileContent);
					ch.setChunkData(ByteString.copyFrom(fileContent));
					rrb.setChunk(ch);

				} catch (Exception e) {
					System.out.println("Error exception" + e.toString());
					rsp.setStatus(Response.Status.ERROR);
				} finally {
					fin.close();
					file = null;
					fin = null;
				}
			} else {
				System.out.println("Collecting file info");
				LocationList.Builder locationList = ServerState.hashTable.get(req.getRrb().getFilename());

				if (locationList != null) {

					for (ChunkLocation chunkLocation : locationList.getLocationListList()) {
						rrb.setChunkLocation(chunkLocation.getChunkId(), chunkLocation);
					}
				} else {
					System.out.println("No file found");
					rsp.setStatus(Response.Status.FILENOTFOUND);
				}
			}
			rsp.setReadResponse(rrb);
			cm.setResponse(rsp);
			// channel.writeAndFlush(cm.build());
			serverState.cmforward.addLast(cm.build());

		} else {
			throw new Exception("Invalid message type");
		}
	}

	public void processWriteRequest(CommandMessage msg, Channel channel) throws Exception {
		Request req = msg.getRequest();
		if (req.hasRwb()) {
			if (req.getRwb().hasChunk()) {

				// save chunk data on local fs
				// Send a event to worker thread about pending log
				// update
				FileOutputStream fout = null;
				File file = null;
				boolean fileexits = false;
				try {
					String filename = req.getRwb().getFilename();
					LocationList.Builder lb1;
					if (ServerState.hashTable.containsKey(filename)) {
						lb1 = ServerState.hashTable.get(filename);
						List<ChunkLocation> chl = lb1.getLocationListList();
						for (ChunkLocation item : chl) {
							if (item.getChunkId() == req.getRwb().getChunk().getChunkId()) {
								fileexits = true;
								System.out.println("File exits");
							}
						}
					}
					if (!fileexits) {
						System.out.println("File not exits");
						String chunkName = new String(
								req.getRwb().getFilename() + "." + req.getRwb().getChunk().getChunkId());
						file = new File(Paths.get(serverState.getDbPath(), chunkName).toString());
						file.createNewFile();
						fout = new FileOutputStream(file);
						fout.write(req.getRwb().getChunk().getChunkData().toByteArray());
						System.out.println("File written");
						if (serverState.getConf().getNodeId() == 31) {
							System.out.println("Yes leader");
							// build <filename, LocationList> String filename =
							// req.getRwb().getFilename();
							int chunkId = req.getRwb().getChunk().getChunkId();
							Node.Builder nb = Node.newBuilder();
							nb.setHost(serverState.getConf().getHostAddress());
							nb.setPort(serverState.getConf().getCommandPort());
							nb.setNodeId(serverState.getConf().getNodeId());

							ChunkLocation.Builder clb = ChunkLocation.newBuilder();
							clb.setChunkId(chunkId);
							clb.addNode(nb.build());
							// clb.setNode(0, nb.build());

							LocationList.Builder lb;
							if (ServerState.hashTable.containsKey(filename)) {
								lb = ServerState.hashTable.get(filename);
							} else {
								lb = LocationList.newBuilder();
							}

							lb.addLocationList(clb.build());

							ServerState.hashTable.put(filename, lb);

							// construct a work message to send out to Followers
							Header.Builder hb = Header.newBuilder();
							hb.setDestination(-1);
							hb.setNodeId(serverState.getConf().getNodeId());
							hb.setMaxHops(-1);
							hb.setTime(System.currentTimeMillis());

							AppendLogItem.Builder append = AppendLogItem.newBuilder();
							append.setFilename(filename);
							append.setChunkId(chunkId);
							append.setNode(nb.build());

							WorkMessage.Builder wb = WorkMessage.newBuilder();
							wb.setAppend(append);
							wb.setHeader(hb);
							wb.setSecret(serverState.secret);
							serverState.wmforward.addLast(wb.build());

						} else {
							System.out.println("No leader");
							FileChunkObject nod = new FileChunkObject();
							nod.setNode_id(serverState.getConf().getNodeId());
							nod.setHostAddress(serverState.getConf().getHostAddress());
							nod.setPort_id(serverState.getConf().getCommandPort());
							nod.setChunk_id(req.getRwb().getChunk().getChunkId());
							nod.setFileName(req.getRwb().getFilename());

							serverState.incoming.addLast(nod);
						}
						System.out.println("File writting " + chunkName);

						if (msg.getHeader().hasDestination()) {
							// Reply success to client
							Header.Builder hd = Header.newBuilder();
							hd.setDestination(msg.getHeader().getNodeId());
							hd.setNodeId(serverState.getConf().getNodeId());
							hd.setTime(System.currentTimeMillis());
							hd.setMaxHops(-1);

							Response.Builder rsp = Response.newBuilder();
							rsp.setFilename(req.getRrb().getFilename());
							rsp.setStatus(Response.Status.SUCCESS);
							rsp.setResponseType(TaskType.RESPONSEWRITEFILE);

							CommandMessage.Builder cm = CommandMessage.newBuilder();
							cm.setHeader(hd);
							cm.setResponse(rsp);

							serverState.cmforward.addLast(cm.build());
						} else {
							// msg = rebuildMessage(msg);
							serverState.cmforward.addLast(msg);
						}
					}
					// channel.writeAndFlush(cm.build());

				} catch (Exception e) {
					System.out.println("Error exception" + e);
				} finally {
					System.out.println("File write ends");
					System.out.println(ServerState.hashTable.size());
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

		if (msg.getHeader().getDestination() < serverState.minRange
				&& msg.getHeader().getDestination() > serverState.maxRange)
			return;

		try {
			if (msg.hasPing()) {

				Header.Builder hd = Header.newBuilder();
				// System.out.println("Got ping");
				hd.setDestination(msg.getHeader().getNodeId());

				hd.setNodeId(msg.getHeader().getDestination());
				hd.setTime(System.currentTimeMillis());
				hd.setMaxHops(serverState.maxHops);
				CommandMessage.Builder rb = CommandMessage.newBuilder();
				rb.setHeader(hd);

				rb.setPing(true);

				serverState.cmforward.addLast(rb.build());

			} else if (msg.hasRequest()) {
				Request req = msg.getRequest();
				switch (req.getRequestType()) {
				case REQUESTREADFILE:
					System.out.println(msg);
					processReadRequest(msg, channel);
					break;

				case REQUESTWRITEFILE:

					// replicate to first neighbour

					// case REPLICATION:
					System.out.println("Write file request");

					processWriteRequest(msg, channel);
					if (serverState.getConf().getRouting().size() > 0) {
						CommandMessage.Builder replicate = CommandMessage.newBuilder();
						replicate.mergeFrom(msg);
						Header.Builder hb = replicate.getHeaderBuilder();

						hb.setDestination(serverState.getConf().getRouting().get(0).getId());
						hb.setMaxHops(serverState.maxHops);

						replicate.setHeader(hb);
						serverState.cmforward.add(replicate.build());
					}
					break;
				default:
					break;
				}

			} else if (msg.hasResponse()) {
				Response res = msg.getResponse();
				switch (res.getResponseType()) {
				case REQUESTWRITEFILE:
					if (res.hasStatus()) {
						if (res.getStatus() != Response.Status.SUCCESS) {
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
			System.out.println("Caught an exception:");
			System.out.println(e.toString());

			Failure.Builder eb = Failure.newBuilder();
			eb.setId(this.serverState.getConf().getNodeId());
			eb.setRefId(msg.getHeader().getNodeId());
			eb.setMessage(e.getMessage());

			Header.Builder hd = Header.newBuilder();
			hd.setDestination(msg.getHeader().getNodeId());
			hd.setNodeId(serverState.getConf().getNodeId());
			hd.setTime(System.currentTimeMillis());

			CommandMessage.Builder rb = CommandMessage.newBuilder(msg);
			rb.setHeader(hd);
			rb.setErr(eb);

			channel.writeAndFlush(rb.build());
		} finally {
			System.out.flush();
			// channel.close();
		}
	}
}