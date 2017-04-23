package gash.router.server;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;

//import gash.router.container.RoutingConf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.ZlibCodecFactory;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import routing.Pipe.CommandMessage;

/**
 * Initializes the external interface
 * 
 * @author gash
 *
 */
public class CommandInit extends ChannelInitializer<SocketChannel> {
	boolean compress = false;
	// RoutingConf conf;
	RingBuffer<CommandMessageEvent> ringBuffer;
	ServerState state;

	@SuppressWarnings("unchecked")
	public CommandInit(ServerState state, boolean enableCompression) {
		super();
		compress = enableCompression;
		// this.conf = state.getConf();
		this.state = state;

		// Proactor pattern - create a thread pool to serve client requests.
		// speed efficiency
		// Disruptor pattern message queue as a buffer. Avoiding locks provides
		// huge speed gain
		// As mostly the message queue is either full or empty and so using ring
		// buffer and producer/consumer events
		// provides efficiency. Code help from LMAX Disruptor github repo

		// Executor that will be used to construct new threads for consumers
		Executor executor = Executors.newCachedThreadPool();
		CommandMessageEventFactory factory = new CommandMessageEventFactory();
		int bufferSize = 1024; // Specify the size of the ring buffer, must be
								// power of 2.
		Disruptor<CommandMessageEvent> disruptor = new Disruptor<CommandMessageEvent>(factory, bufferSize, executor);
		disruptor.handleEventsWith(new CommandMessageEventHandler(state));
		disruptor.start();
		this.ringBuffer = disruptor.getRingBuffer();
	}

	@Override
	public void initChannel(SocketChannel ch) throws Exception {
		ChannelPipeline pipeline = ch.pipeline();

		// Enable stream compression (you can remove these two if unnecessary)
		if (compress) {
			pipeline.addLast("deflater", ZlibCodecFactory.newZlibEncoder(ZlibWrapper.GZIP));
			pipeline.addLast("inflater", ZlibCodecFactory.newZlibDecoder(ZlibWrapper.GZIP));
		}

		/**
		 * length (4 bytes).
		 * 
		 * Note: max message size is 64 Mb = 67108864 bytes this defines a
		 * framer with a max of 64 Mb message, 4 bytes are the length, and strip
		 * 4 bytes
		 */
		pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(67108864, 0, 4, 0, 4));

		// decoder must be first
		pipeline.addLast("protobufDecoder", new ProtobufDecoder(CommandMessage.getDefaultInstance()));
		pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
		pipeline.addLast("protobufEncoder", new ProtobufEncoder());

		// our server processor (new instance for each connection)
		// Command handler creates command message events and pushes it into
		// disruptor ring buffer.
		pipeline.addLast("handler", new CommandHandler(state, ringBuffer));
	}
}
