package gash.router.server;

import java.util.List;
import java.util.Map;
import gash.router.container.RoutingConf.RoutingEntry;
import pipe.common.Common.Header;
import pipe.common.Common.LocationList;
import pipe.common.Common.Node;
import pipe.common.Common.ReadBody;
import pipe.common.Common.Request;
import pipe.common.Common.TaskType;
import routing.Pipe.CommandMessage;

public class TaskStealer implements Runnable {
	private RoutingEntry e;
	private int quantity;
	private ServerState state;

	public TaskStealer(RoutingEntry e, int quantity, ServerState state) {
		this.e = e;
		this.quantity = quantity;
		this.state = state;
	}

	@Override
	public void run() {
		for (Map.Entry<String, LocationList.Builder> entry : ServerState.hashTable.entrySet()) {
			int count = entry.getValue().getLocationListCount();

			for (int i = 0; i < count; ++i) {
				List<Node> nodes = entry.getValue().getLocationList(i).getNodeList();
				for (Node n : nodes) {
					if (n.getNodeId() == e.getId() && quantity > 0) {

						Header.Builder hd = Header.newBuilder();
						hd.setDestination(e.getId());
						hd.setNodeId(state.getConf().getNodeId());
						hd.setTime(System.currentTimeMillis());
						hd.setMaxHops(state.getConf().getTotalNodes());

						Request.Builder req = Request.newBuilder();
						ReadBody.Builder rrb = ReadBody.newBuilder();
						rrb.setFilename(entry.getKey());
						rrb.setChunkId(entry.getValue().getLocationList(i).getChunkid());
						req.setRequestType(TaskType.REQUESTREADFILE);
						req.setRrb(rrb);
						CommandMessage.Builder cm = CommandMessage.newBuilder();
						cm.setHeader(hd);
						cm.setReq(req);
						state.cmforward.add(cm.build());
						quantity -= 1;
					}
				}
			}
		}
	}

}
