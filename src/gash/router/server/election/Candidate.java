package gash.router.server.election;

import gash.router.server.ServerState;
import gash.router.server.edges.EdgeMonitor;
import gash.router.server.edges.EdgeList;
import gash.router.server.edges.EdgeInfo;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import gash.router.server.WorkInit;

import pipe.common.Common.Header;
import pipe.voteRequest.VoteRequest.VoteReq;
import pipe.work.Work.WorkMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gash.router.container.RoutingConf.RoutingEntry;

public class Candidate implements Runnable {
	protected static Logger logger = LoggerFactory.getLogger("Candidate");

	private boolean isCandidate;
	private int currentTerm;
	private int currentNodeId;
	private int votedFor;
	private boolean leader;
	private int leaderId;
	private int totalVotesRecievedForThisTerm;
	private ServerState state;

	private EdgeList outboundEdges;
	private EdgeList inboundEdges;
	private long dt = 2000;
	private boolean forever = true;

	public Candidate(ServerState state) {

		if (state == null)
			throw new RuntimeException("state is null");

		this.outboundEdges = new EdgeList();
		this.inboundEdges = new EdgeList();

		this.isCandidate = state.getStatus().getCandidate();
		logger.info("Candidate true or not::  " + isCandidate);
		state.getStatus().setTotalVotesRecievedForThisTerm(0);
		this.state = state;

		if (state.getConf().getRouting() != null) {
			for (RoutingEntry e : state.getConf().getRouting()) {
				outboundEdges.addNode(e.getId(), e.getHost(), e.getPort());
			}
		}

		// cannot go below 2 sec
		if (state.getConf().getHeartbeatDt() > this.dt)
			this.dt = state.getConf().getHeartbeatDt();

	}

	@Override
	public void run() {
		while (forever) {
			try {
				startElection();
				Thread.sleep(2000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	public void startElection() {
		logger.info("gets int o startElection method");

		if (isCandidate) {

			this.currentTerm = state.getStatus().getCurrentTerm();
			this.currentNodeId = state.getConf().getNodeId();

			state.getStatus().setVotedFor(currentNodeId);
			state.getStatus()
					.setTotalVotesRecievedForThisTerm(state.getStatus().getTotalVotesRecievedForThisTerm() + 1);

			EdgeMonitor em = new EdgeMonitor(state);
			this.outboundEdges = em.getOutboundEdges();

			for (EdgeInfo ei : this.outboundEdges.getMap().values()) {
				// System.out.println(ei.getChannel().toString());
				if (ei.getChannel() != null && ei.isActive()) {
					// ei.retry = 0;
					WorkMessage wm = createVoteRequest();
					ei.getChannel().writeAndFlush(wm);
					logger.info("you did turn off");
					this.forever = false;
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
						logger.info("reached here exactly where you turn the loop off");

					} catch (Exception e) {
						logger.error("error in conecting to node " + ei.getRef() + " exception " + e.getMessage());
					}
				}
			}

		}
	}

	public WorkMessage createVoteRequest() {
		Header.Builder hb = Header.newBuilder();
		hb.setNodeId(this.state.getConf().getNodeId());
		hb.setDestination(-1);
		hb.setTime(System.currentTimeMillis());

		VoteReq.Builder vr = VoteReq.newBuilder();
		vr.setTerm(this.currentTerm + 1);
		System.out.println("node id is: " + this.currentNodeId);
		vr.setCandidateId(this.currentNodeId);
		vr.setLastLogIndex(this.state.getStatus().getCommitIndex());
		vr.setLastLogTerm(this.state.getStatus().getLastTermInLog());

		WorkMessage.Builder wm = WorkMessage.newBuilder();
		wm.setPing(true);
		wm.setHeader(hb);
		wm.setVrMsg(vr);
		wm.setSecret(121316548);

		logger.info("conencted message");

		return wm.build();
	}

}