package org.acceptor;

import java.util.concurrent.CountDownLatch;

import org.acceptor.netty.NettyServer;

public class Sql4Es {
	public static void main(String[] args) throws NumberFormatException, Exception {
		if(args.length != 1) {
			System.out.println("Please Input The Server Port");
		}
		CountDownLatch shutDown = new CountDownLatch(1);
		NettyServer.service(Integer.parseInt(args[0]));
		shutDown.await();
	}
}
