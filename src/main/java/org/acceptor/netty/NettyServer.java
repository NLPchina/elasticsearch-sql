package org.acceptor.netty;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.util.CharsetUtil;

public class NettyServer {

	private static final int BIZTHREADSIZE = 100;
	private static final int BIZGROUPSIZE = Runtime.getRuntime().availableProcessors() * 2;
	private static final EventLoopGroup bossGroup = new NioEventLoopGroup(BIZGROUPSIZE);
	private static final EventLoopGroup workerGroup = new NioEventLoopGroup(BIZTHREADSIZE);

	public static void service(int port) throws Exception {
		ServerBootstrap bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup);
		bootstrap.channel(NioServerSocketChannel.class);
		bootstrap.childHandler(new ChannelInitializer<Channel>() {

			@Override
			protected void initChannel(Channel ch) throws Exception {
				ChannelPipeline pipeline = ch.pipeline();
				pipeline.addLast(new StringDecoder(CharsetUtil.UTF_8));
				pipeline.addLast(new StringEncoder(CharsetUtil.UTF_8));
				pipeline.addLast(new TcpServerHandler());
			}

		});
		ChannelFuture f = bootstrap.bind(port).sync();
		f.channel().closeFuture().sync();
		System.out.println("Netty Server Started...");
	}

	protected static void shutdown() {
		workerGroup.shutdownGracefully();
		bossGroup.shutdownGracefully();
	}

}
