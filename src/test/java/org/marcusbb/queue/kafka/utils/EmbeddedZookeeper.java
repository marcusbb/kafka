package org.marcusbb.queue.kafka.utils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

public class EmbeddedZookeeper {
    private int port = -1;
    private int tickTime = 500;

    private ServerCnxnFactory factory;
    private File snapshotDir;
    private File logDir;
    private ZooKeeperServer zooKeeperServer;

    public EmbeddedZookeeper() {
        this(-1);
    }

    public EmbeddedZookeeper(int port) {
        this.port = resolvePort(port);
    }

    public EmbeddedZookeeper(int port, int tickTime) {
        this.port = resolvePort(port);
        this.tickTime = tickTime;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    public void startup() throws IOException {
        this.snapshotDir = TestUtils.constructTempDir("embedded-zk/snapshot");
        this.logDir = TestUtils.constructTempDir("embedded-zk/log");

        try {
            zooKeeperServer = new ZooKeeperServer(snapshotDir, logDir, tickTime);
            factory = new NIOServerCnxnFactory();
            factory.configure(new InetSocketAddress("localhost", port), 1024);
            factory.startup(zooKeeperServer);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    public void shutdown() {
        if (factory != null) {
            factory.shutdown();
        }
        if (zooKeeperServer != null) {
            zooKeeperServer.shutdown();
        }
        try {
            TestUtils.deleteFile(snapshotDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
        try {
            TestUtils.deleteFile(logDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
    }

    public String getConnection() {
        return "localhost:" + port;
    }

    public int getPort() {
        return port;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
        sb.append("connection=").append(getConnection());
        sb.append('}');
        return sb.toString();
    }
}
