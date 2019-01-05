package org.marcusbb.queue.kafka.producer.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.marcusbb.queue.kafka.producer.LoggingProducerInterceptor;
import org.marcusbb.queue.kafka.producer.Producer;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SimpleProducer<T> implements Producer<T> {

	Logger logger = LoggerFactory.getLogger(SimpleProducer.class);

	KafkaProducer<byte[], byte[]> producer = null;
	ByteSerializer<T> serializer;

	public SimpleProducer(Properties props, ByteSerializer<T> serializer)  {
		Properties p = new Properties();
		p.putAll(props);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());
		producer = new KafkaProducer<>(p);
		this.serializer = serializer;

		String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
		String brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
		String clientIp = null;
		try {
			clientIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("could not determine host ip address");
		}

		logger.info("register producer from host={} with brokers={}", clientIp, brokers);
	}

	@Override
	public void add(String topic, T message) {
		add(topic,message,null);
	}

	@Override
	public void add(String topic, T message, Object partitionKey) {
		ProducerRecord<byte[], byte[]> record = null;
		byte []payload = serializer.serialize(message);
		if (partitionKey != null) {
			byte [] kbytes;
			if (partitionKey instanceof byte[]){
				kbytes = (byte[]) partitionKey;
			}else {
				kbytes = partitionKey.toString().getBytes(StandardCharsets.UTF_8);
			}
			record = new ProducerRecord<>(topic, kbytes, payload);
		}else {
			record = new ProducerRecord<>(topic, payload);
		}
		try {
			producer.send(record).get();
			logger.info("message {} sent to topic {}",message,topic);
		} catch (Exception ex){
			throw new RuntimeException(ex);
		}
	}

	@Override
	public void rollback() {
		throw new UnsupportedOperationException();
	}

	@Override
	public void flush() {
	}

	public void close() {
		if (producer != null)
			producer.close();
	}

}
