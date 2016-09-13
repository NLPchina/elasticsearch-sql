package org.acceptor.netty;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.acceptor.util.Processor;

public class TcpServerHandler extends ChannelInboundHandlerAdapter {
	
	private Executor executor = null;
	
	public TcpServerHandler() {
		executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}
	
	@Override
	public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
		
		executor.execute(new Processor(ctx, msg.toString()));
	}

	@Override
	public void channelActive(ChannelHandlerContext ctx) throws Exception {
		super.channelActive(ctx);
	}

	@Override
	public void channelInactive(ChannelHandlerContext ctx) throws Exception {
		super.channelInactive(ctx);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {

	}
}
