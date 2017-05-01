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
package gash.router.server.edges;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import io.netty.channel.Channel;

import gash.router.container.RoutingConf.RoutingEntry;
import gash.router.server.FileChunkObject;
import gash.router.server.WorkInit;
import gash.router.server.ServerState;
import pipe.common.Common.Header;
import pipe.common.Common.Node;
import pipe.common.Common.RequestAppendItem;
import pipe.work.Work.Heartbeat;
import pipe.work.Work.WorkMessage;
import pipe.work.Work.WorkState;
import redis.clients.jedis.Jedis;
import pipe.work.Work.AddEdge;
import routing.Pipe.CommandMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private EdgeList outboundEdges;
	private EdgeList commandEdges;
	private EdgeList inboundEdges;
	private EdgeInfo clientEdge;
	private EdgeList globalNeighbour;

	
	private long dt = 2000;

	private ServerState state;

	private EventLoopGroup group;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.commandEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.globalNeighbour = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		updateEdges();

		group = new NioEventLoopGroup();

	}
	
		public void setGlobalNeighbours() {

		try {

			Jedis globalRedis = new Jedis(state.getConf().getRedishost());
			// globalRedis.select(0);
			globalRedis.set("3", state.getConf().getHostAddress() + ":" + state.getConf().getCommandPort());
			System.out.println("---Redis updated---");
			globalRedis.close();
		} catch (Exception e) {
			System.out.println("---Problem with redis at updateing my leader---");
		}

	}

	public void FetchGlobalNeighbours() {
		try {

			Jedis globalRedis = new Jedis(state.getConf().getRedishost());
			String url = globalRedis.get("4");
			System.out.println(url);
			String host = url.split(":")[0];
			int port = Integer.parseInt(url.split(":")[1]);
			globalRedis.close();
			globalNeighbour.clear();
			globalNeighbour.addNode(4, host, port);

		} catch (Exception e) {
			System.out.println("---Problem with redis while fetching neighbour---");
		}
	}

	public void addClientEdge(int hostId, Channel ctx) {
		clientEdge = new EdgeInfo(hostId, " ", 2048);
		clientEdge.setChannel(ctx);
		clientEdge.setActive(true);
		System.out.println("Got the client edge");
	}

	public EdgeInfo getClientEdge() {
		return clientEdge;
	}

	public void sendClient(CommandMessage msg) {
		clientEdge.getChannel().writeAndFlush(msg);
	}

	public void updateEdges() {
		outboundEdges.clear();
		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
				commandEdges.addNode(e.getId(), e.getHost(), e.getCommand());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private WorkMessage createHB(EdgeInfo ei) {
		WorkState.Builder sb = WorkState.newBuilder();
		sb.setEnqueued(state.getPerformanceStat());
		sb.setProcessed(-1);

		Heartbeat.Builder bb = Heartbeat.newBuilder();
		bb.setState(sb);
		bb.setMessageType(1);

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setTime(System.currentTimeMillis());

		hb.setMaxHops(10);
		hb.setDestination(-1);

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setSecret(121316546);

		return wb.build();
	}

	private WorkMessage addEntryMessage() {

		// send previous node a message to add entry to my node to create and
		// maintain a ring

		int toSendNodeId;

		if (state.getConf().getNodeId() == 1) {
			toSendNodeId = state.getConf().getTotalNodes();
		} else {
			toSendNodeId = state.getConf().getNodeId() - 1;
		}

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setTime(System.currentTimeMillis());
		hb.setMaxHops(state.getConf().getTotalNodes());
		hb.setDestination(toSendNodeId);

		AddEdge.Builder ae = AddEdge.newBuilder();
		ae.setNodeToAdd(state.getConf().getNodeId());
		// ae.setHost(state.getConf().getHostAddress());
		ae.setHost("localhost");
		ae.setPort(state.getConf().getWorkPort());
		ae.setCommand(state.getConf().getCommandPort());

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setAddEdge(ae);
		wb.setSecret(121316552);

		return wb.build();
	}

	public void shutdown() {
		state.keepWorking = false;
	}

	@Override
	public void run() {

		Process_WorkForward wFoward = new Process_WorkForward(inboundEdges, outboundEdges, state);
		Thread thread1 = new Thread(wFoward);
		thread1.start();

		Process_CommandForward cFoward = new Process_CommandForward(inboundEdges, commandEdges, globalNeighbour, state);
		Thread thread2 = new Thread(cFoward);
		thread2.start();

		Process_InComming pIncomming = new Process_InComming(inboundEdges, outboundEdges, state);
		Thread thread3 = new Thread(pIncomming);
		thread3.start();

		while (state.keepWorking) {

			try {
				sendHeartBeat();
				Thread.sleep(dt);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private void sendHeartBeat() {

		System.out.println("getting the count of nodes that has been discovered before setting hopcount: " + state.getStatus().getTotalNodesDiscovered());

		//state.getConf().setTotalNodes(state.getStatus().getTotalNodesDiscovered());

		System.out.println("before setting hopcount in createHB" + state.getConf().getTotalNodes());


		for (EdgeInfo ei : this.outboundEdges.map.values()) {
			if (ei.getChannel() != null && ei.isActive()) {
				// System.out.println(
				// "retrieving total nodes set as discovered in conf: " +
				// state.getConf().getTotalNodes());
				WorkMessage wm = createHB(ei);

				ei.getChannel().writeAndFlush(wm);

				state.getStatus().setTotalNodesDiscovered(1);
				state.getStatus().removeAllInList();
				logger.info("send heart beat to " + ei.getRef());

			} else {
				try {
					onAdd(ei);
					WorkMessage wm = createHB(ei);
					ei.getChannel().writeAndFlush(wm);
					logger.info("send heart beat to " + ei.getRef());
				} catch (Exception e) {
					System.out.println("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
				}
			}
		}

	}

	public void initConnect(EdgeInfo ei) {
		if (ei == null)
			return;
		try {
			WorkInit si = new WorkInit(state, false);
			Bootstrap b = new Bootstrap();
			b.group(group).channel(NioSocketChannel.class).handler(si);
			b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
			b.option(ChannelOption.TCP_NODELAY, true);
			b.option(ChannelOption.SO_KEEPALIVE, true);

			// Make the connection attempt.
			ChannelFuture channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

			// want to monitor the connection to the server s.t. if we loose the
			// connection, we can try to re-establish it.
			ei.setChannel(channel.channel());
			ei.setActive(channel.channel().isActive());
		} catch (Exception e) {
			// logger.error("cannot connect to node " + ei.getRef());
		}
	}

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		createInboundIfNew(ei.getRef(), ei.getHost(), ei.getPort());
		initConnect(ei);
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		ei.setActive(false);
		ei.getChannel().close();
	}

	public void setOutboundEdges(EdgeList outboundEdges) {
		this.outboundEdges = outboundEdges;
	}

	public EdgeList getOutboundEdges() {
		return this.outboundEdges;
	}

	public void setInboundEdges(EdgeList inboundEdges) {
		this.inboundEdges = inboundEdges;
	}

	class Process_WorkForward implements Runnable {
		// protected static Logger logger =
		// LoggerFactory.getLogger("Process_WorkForward");

		private EdgeList outboundEdges;
		private EdgeList inboundEdges;
		private ServerState state;

		public Process_WorkForward(EdgeList in, EdgeList out, ServerState state) {
			this.outboundEdges = out;
			this.inboundEdges = in;
			this.state = state;
		}

		@Override
		public void run() {
			while (state.keepWorking) {
				try {
					if (!state.wmforward.isEmpty()) {
						process_wmforward();
					}
				} catch (Exception e) {
					System.out.println("Work Message forward failed." + e.getMessage());
				}
			}
		}

		// public void createInboundIfNew(int ref, String host, int port) {
		// inboundEdges.createIfNew(ref, host, port);
		// }

		private void process_wmforward() {
			WorkMessage msg = state.wmforward.poll();

			if (msg != null) {
				for (EdgeInfo ei : this.outboundEdges.map.values()) {
					// createInboundIfNew(ei.getRef(), ei.getHost(),
					// ei.getPort());
					if (ei.getChannel() != null && ei.isActive()) {
						System.out.println("Sending out");
						System.out.println(msg);
						ei.getChannel().writeAndFlush(msg);
					} else {
						try {
							onAdd(ei);
							System.out.println("Sending out");
							System.out.println(msg);
							ei.getChannel().writeAndFlush(msg);
						} catch (Exception e) {
							System.out.println(
									"error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
						}
					}
					// }
				}
			}
		}

	}

	class Process_CommandForward implements Runnable {
		// private static Logger logger =
		// LoggerFactory.getLogger("Process_CommandForward");

		private EdgeList outboundEdges;
		private EdgeList inboundEdges;
		private EdgeList global;
		private ServerState state;

		public Process_CommandForward(EdgeList in, EdgeList out, EdgeList global, ServerState state) {
			this.outboundEdges = out;
			this.inboundEdges = in;
			this.global = global;
			this.state = state;
		}

		@Override
		public void run() {
			while (state.keepWorking) {
				try {
					if (!state.cmforward.isEmpty()) {
						process_cmforward();
					}
				} catch (Exception e) {
					System.out.println("Command Message forward failed." + e.getMessage());
				}
			}
		}

		private void process_cmforward() {
			CommandMessage msg = state.cmforward.poll();

			if (msg != null) {
				if (msg.getHeader().hasDestination() || msg.getHeader().getDestination() == state.client_id) {
					state.getEmon().sendClient(msg);
				} else if (msg.getHeader().getDestination() >= state.minRange
						&& msg.getHeader().getDestination() <= state.maxRange) {
					for (EdgeInfo ei : this.outboundEdges.map.values()) {
						// createInboundIfNew(ei.getRef(), ei.getHost(),
						// ei.getPort());
						if (ei.getChannel() != null && ei.isActive()) {
							ei.getChannel().writeAndFlush(msg);
						} else {
							try {
								onAdd(ei);
								ei.getChannel().writeAndFlush(msg);
							} catch (Exception e) {
								System.out.println(
										"error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
							}
						}
					}
				} else {
					if (this.global.map.size() == 0) {
						FetchGlobalNeighbours();
					}
					for (EdgeInfo ei : this.global.map.values()) {
						if (ei.getChannel() != null && ei.isActive()) {

							ei.getChannel().writeAndFlush(msg);
						} else {
							try {
								onAdd(ei);
								ei.getChannel().writeAndFlush(msg);
							} catch (Exception e) {
								System.out.println(
										"error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
							}
						}
					}

				}
			}
		}
	}

	class Process_InComming implements Runnable {
		// private static Logger logger =
		// LoggerFactory.getLogger("Process_InComming");

		private EdgeList outboundEdges;
		private EdgeList inboundEdges;
		private ServerState state;

		public Process_InComming(EdgeList in, EdgeList out, ServerState state) {
			this.outboundEdges = out;
			this.inboundEdges = in;
			this.state = state;
		}

		@Override
		public void run() {
			while (state.keepWorking) {
				try {
					if (!state.incoming.isEmpty()) {
						process_incoming();
					}
				} catch (Exception e) {
					System.out.println("Incomming forward failed." + e.getMessage());
				}
			}
		}

		private void process_incoming() {
			FileChunkObject fco = state.incoming.remove();
			Header.Builder hb = Header.newBuilder();
			hb.setDestination(state.getCurrentLeader());
			hb.setNodeId(state.getConf().getNodeId());
			hb.setMaxHops(-1);

			Node.Builder nb = Node.newBuilder();
			nb.setHost(fco.getHostAddress());
			nb.setPort(fco.getPort_id());
			nb.setNodeId(fco.getNode_id());

			RequestAppendItem.Builder append = RequestAppendItem.newBuilder();
			append.setFilename(fco.getFileName());
			append.setChunkId(fco.getChunk_id());
			append.setNode(nb);

			WorkMessage.Builder wb = WorkMessage.newBuilder();
			wb.setRequestAppend(append.build());
			wb.setHeader(hb.build());

			for (EdgeInfo ei : this.outboundEdges.map.values()) {
				createInboundIfNew(ei.getRef(), ei.getHost(), ei.getPort());
				if (ei.getChannel() != null && ei.isActive()) {
					ei.getChannel().writeAndFlush(wb.build());

				} else {
					try {
						onAdd(ei);
						ei.getChannel().writeAndFlush(wb.build());
					} catch (Exception e) {
						System.out
								.println("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
					}
				}
			}
		}

	}

	public EdgeList getInboundEdges() {
		return this.inboundEdges;
	}
}