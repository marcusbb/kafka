package org.marcusbb.queue.kafka.utils;

import static org.marcusbb.queue.kafka.utils.TestUtils.constructTempDir;
import static org.marcusbb.queue.kafka.utils.TestUtils.resolvePort;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.ZkUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.mutable.Buffer;

public class EmbeddedKafkaBroker {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedKafkaBroker.class);

    private final Integer brokerId;
    private final Integer port;
    private final String zkConnection;
    private final Properties baseProperties;

    private final String brokerList;

    private KafkaServer kafkaServer;
    private File logDir;
    private ZkUtils zkUtils;

    public EmbeddedKafkaBroker(int brokerId, String zkConnection) {
        this(brokerId, -1, zkConnection, new Properties());
    }

    public EmbeddedKafkaBroker(int brokerId, int port, String zkConnection, Properties baseProperties) {
        this.brokerId = brokerId;
        this.port = resolvePort(port);
        this.zkConnection = zkConnection;
        this.baseProperties = baseProperties;

        LOGGER.info("Starting broker[{}] on port {}", this.brokerId, this.port);
        this.brokerList = "localhost:" + this.port;
    }

    public void startup() {
        logDir = constructTempDir("kafka-local");

        /*
         * Kafka server.properties
         */
        Properties properties = new Properties();
        properties.putAll(baseProperties);
        properties.setProperty("zookeeper.connect", zkConnection);
        properties.setProperty("broker.id", brokerId.toString());
        properties.setProperty("host.name", "localhost");
        properties.setProperty("port", Integer.toString(port));
        properties.setProperty("log.dir", logDir.getAbsolutePath());
        properties.setProperty("num.partitions", String.valueOf(1));
        properties.setProperty("auto.create.topics.enable", String.valueOf(Boolean.TRUE));
        properties.setProperty("log.flush.interval.messages", String.valueOf(1));
        properties.setProperty("offsets.topic.replication.factor", String.valueOf(1));


        kafkaServer = startBroker(properties);
    }


    private KafkaServer startBroker(Properties props) {
        zkUtils = ZkUtils.apply(
                zkConnection,
                30000,
                30000,
                false);
        List<KafkaMetricsReporter> kmrList = new ArrayList<>();
        Buffer<KafkaMetricsReporter> metricsList = scala.collection.JavaConversions.asScalaBuffer(kmrList);
        KafkaServer server = new KafkaServer(new KafkaConfig(props), new SystemTime(), Option.<String>empty(), metricsList);
        server.startup();
        return server;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Integer getPort() {
        return port;
    }

    public void shutdown() {
        if (kafkaServer != null) {
            kafkaServer.shutdown();
        }
        try {
            TestUtils.deleteFile(logDir);
        } catch (FileNotFoundException e) {
            LOGGER.info("Could not delete {} - not found", logDir.getAbsolutePath());
        }
    }

    public ZkUtils getZkUtils() {
        return zkUtils;
    }

    public void createTopic(String topicName) {
        createTopic(topicName, 1);
    }

    public void createTopic(String topicName, int partitionCount) {
        int replicationCount = 1;
        AdminUtils.createTopic(getZkUtils(), topicName, partitionCount, replicationCount, new Properties(), RackAwareMode.Enforced$.MODULE$);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaBroker{");
        sb.append("brokerList='").append(brokerList).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
