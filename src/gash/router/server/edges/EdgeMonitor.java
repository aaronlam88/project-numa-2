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
import routing.Pipe.CommandMessage;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;

public class EdgeMonitor implements EdgeListener, Runnable {
	protected static Logger logger = LoggerFactory.getLogger("edge monitor");

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 20000;
	private ServerState state;
	private boolean forever = true;

	public EdgeMonitor(ServerState state) {
		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();
		this.state = state;
		this.state.setEmon(this);

		updateEdges();
	}

	public void updateEdges(){
		outboundEdges.clear();
		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
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

		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(state.getConf().getNodeId());
		hb.setTime(System.currentTimeMillis());

		if(state.isLeader()) {
			hb.setMaxHops(-1);
			hb.setDestination(state.getConf().getNodeId());
		} else {
			hb.setMaxHops(1);
			hb.setDestination(ei.getRef());
		}

		WorkMessage.Builder wb = WorkMessage.newBuilder();
		wb.setHeader(hb);
		wb.setBeat(bb);
		wb.setSecret(121316546);
		
		return wb.build();
	}

	public void shutdown() {
		forever = false;
		state.keepWorking = false;
	}

	@Override
	public void run() {

		Process_WorkForward wFoward = new Process_WorkForward(inboundEdges, outboundEdges, state);
		Thread thread1 = new Thread(wFoward);
		thread1.start();
		
		Process_CommandForward cFoward = new Process_CommandForward(inboundEdges, outboundEdges, state);
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
		for (EdgeInfo ei : this.outboundEdges.map.values()) {
			if (ei.getChannel() != null && ei.isActive()) {
				//ei.retry = 0;
				WorkMessage wm = createHB(ei);
				ei.getChannel().writeAndFlush(wm);
				logger.info("send heart beat to " + ei.getRef());
			} else {
				try {
					EventLoopGroup group = new NioEventLoopGroup();
					WorkInit si = new WorkInit(state, false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);

					ChannelFuture channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

					ei.setChannel(channel.channel());
					ei.setActive(channel.channel().isActive());
				} catch (Exception e) {
					logger.error("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
				}
			}
		}

	}

	@Override
	public synchronized void onAdd(EdgeInfo ei) {
		// TODO check connection
	}

	@Override
	public synchronized void onRemove(EdgeInfo ei) {
		// TODO ?
	}

	public void setOutboundEdges(EdgeList outboundEdges){
		this.outboundEdges=outboundEdges;
	}

	public EdgeList getOutboundEdges(){
		return this.outboundEdges;
	}

	public void setInboundEdges(EdgeList inboundEdges){
		this.inboundEdges=inboundEdges;
	}


class Process_WorkForward implements Runnable {
	//protected static Logger logger = LoggerFactory.getLogger("Process_WorkForward");

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
			if (!state.wmforward.isEmpty()) {
				process_wmforward();
			}
		}
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private void process_wmforward() {
		for (EdgeInfo ei : this.outboundEdges.map.values()) {
			if (ei.getChannel() != null && ei.isActive()) {
				createInboundIfNew(ei.getRef(), ei.getHost(), ei.getPort());
				WorkMessage wm = state.wmforward.poll();
				ei.getChannel().writeAndFlush(wm);
			} else {
				try {
					EventLoopGroup group = new NioEventLoopGroup();
					WorkInit si = new WorkInit(state, false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);

					ChannelFuture channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

					ei.setChannel(channel.channel());
					ei.setActive(channel.channel().isActive());
				} catch (Exception e) {
					logger.error("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
				}
			}
		}
	}
}


class Process_CommandForward implements Runnable {
	//private static Logger logger = LoggerFactory.getLogger("Process_CommandForward");

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private ServerState state;

	public Process_CommandForward(EdgeList in, EdgeList out, ServerState state) {
		this.outboundEdges = out;
		this.inboundEdges = in;
		this.state = state;
	}

	@Override
	public void run() {
		while (state.keepWorking) {
			if (!state.cmforward.isEmpty()) {
				process_cmforward();
			}
		}
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private void process_cmforward() {
		for (EdgeInfo ei : this.outboundEdges.map.values()) {
			if (ei.getChannel() != null && ei.isActive()) {
				createInboundIfNew(ei.getRef(), ei.getHost(), ei.getPort());
				CommandMessage cm = state.cmforward.poll();
				ei.getChannel().writeAndFlush(cm);
			} else {
				try {
					EventLoopGroup group = new NioEventLoopGroup();
					WorkInit si = new WorkInit(state, false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);

					ChannelFuture channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

					ei.setChannel(channel.channel());
					ei.setActive(channel.channel().isActive());
				} catch (Exception e) {
					logger.error("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
				}
			}
		}
	}
}

class Process_InComming implements Runnable {
//	private static Logger logger = LoggerFactory.getLogger("Process_InComming");

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
			if (!state.incoming.isEmpty()) {
				process_incoming();
			}
		}
	}

	public void createInboundIfNew(int ref, String host, int port) {
		inboundEdges.createIfNew(ref, host, port);
	}

	private void process_incoming() {
		for (EdgeInfo ei : this.outboundEdges.map.values()) {
			createInboundIfNew(ei.getRef(), ei.getHost(), ei.getPort());
			if (ei.getChannel() != null && ei.isActive()) {
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

				ei.getChannel().writeAndFlush(wb.build());

			} else {
				try {
					EventLoopGroup group = new NioEventLoopGroup();
					WorkInit si = new WorkInit(state, false);
					Bootstrap b = new Bootstrap();
					b.group(group).channel(NioSocketChannel.class).handler(si);
					b.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
					b.option(ChannelOption.TCP_NODELAY, true);
					b.option(ChannelOption.SO_KEEPALIVE, true);

					ChannelFuture channel = b.connect(ei.getHost(), ei.getPort()).syncUninterruptibly();

					ei.setChannel(channel.channel());
					ei.setActive(channel.channel().isActive());
				} catch (Exception e) {
					logger.error("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
				}
			}
		}
	}

}

	public EdgeList getInboundEdges(){
		return this.inboundEdges;
	}
}
