package org.marcusbb.queue.kafka.utils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;

import java.io.IOException;
import java.net.InetSocketAddress;

public class CustomServerCnxnFactory extends NIOServerCnxnFactory {

    public CustomServerCnxnFactory() throws IOException {
        super();
    }

    @Override
    public void configure(InetSocketAddress addr, int maxcc) throws IOException {
        super.configure(addr, maxcc);
        setMaxClientCnxnsPerHost(100);
    }
}
