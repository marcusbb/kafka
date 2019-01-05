package org.marcusbb.queue.kafka;

import java.util.Properties;

import org.eclipse.jetty.server.Server;
import org.marcusbb.queue.kafka.utils.EmbeddedKafkaBroker;
import org.marcusbb.queue.kafka.utils.EmbeddedZookeeper;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Minimal Kafka Test Server
 *
 */
public class StandaloneKafka {

	private static final Logger LOGGER = LoggerFactory.getLogger(StandaloneKafka.class);

	private static EmbeddedZookeeper zookeeper;
	private static EmbeddedKafkaBroker broker;
	private static Server schemaServer;

	public static void main(String []args) throws Exception {
		start(2181,9092,8081);
	}
	
	public synchronized static void start(int zkPort,int brokerPort,int schemaPort) throws Exception {

		if (zookeeper != null) {
			LOGGER.info("zookeeper server already started");
			return;
		}

		zookeeper = new EmbeddedZookeeper(zkPort);
		broker = new EmbeddedKafkaBroker(0, brokerPort, zookeeper.getConnection(), new Properties());

		zookeeper.startup();
		broker.startup();
		LOGGER.info("========================================= Server started =========================================");

		Thread.sleep(10000);
		Properties schemaConfig = new Properties();
		schemaConfig.put("listeners","http://localhost:" + schemaPort);
		schemaConfig.put("kafkastore.connection.url","localhost:" + zkPort);
		schemaConfig.put("kafkastore.topic","_schemas");
		schemaConfig.put("debug","false");

	    SchemaRegistryRestApplication app = new SchemaRegistryRestApplication(schemaConfig);
	    schemaServer = app.createServer();
	    schemaServer.start();
	    LOGGER.info("==================================== Schema Server started ========================================");

		//graceful shutdown
		Runtime.getRuntime().addShutdownHook(new Thread() {
			
			public void run() {
				broker.shutdown();
				zookeeper.shutdown();
				try {
					schemaServer.stop();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					LOGGER.error("Error while stopping schema server", e);
				}
			}
		});
		
		
	}
	public static void shutdown() {
		broker.shutdown();
		zookeeper.shutdown();
		try {
			schemaServer.stop();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
