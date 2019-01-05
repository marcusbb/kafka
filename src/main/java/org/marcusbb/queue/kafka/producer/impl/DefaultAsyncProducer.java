package org.marcusbb.queue.kafka.producer.impl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.marcusbb.queue.kafka.producer.AsyncProducer;
import org.marcusbb.queue.kafka.producer.LoggingProducerInterceptor;
import org.marcusbb.queue.kafka.producer.ProducerResult;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class DefaultAsyncProducer<T,K> implements AsyncProducer<T,K> {

	Logger logger = LoggerFactory.getLogger(DefaultAsyncProducer.class);

	private KafkaProducer<byte[], byte[]> producer = null;
	private String topic;
	private ByteSerializer<T> serializer;
	
	public DefaultAsyncProducer(Properties props,ByteSerializer<T> serializer)  {
		Properties p = new Properties();
		p.putAll(props);
		p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		p.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, LoggingProducerInterceptor.class.getName());

		producer = new KafkaProducer<>(p);
		
		this.serializer = serializer;
		this.topic = topic;

		String clientId = props.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
		String brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
		String clientIp = null;
		try {
			clientIp = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			logger.warn("Could not retrieve producer IP address, while it connects the brokers");
		}

		logger.info("Register producer={} from instance={} to brokers={}", clientId, clientIp, brokers);
	}

	@Override
	public Future<ProducerResult> add(String topic, T message) {
		return add(topic,message,null);
	}

	@Override
	public Future<ProducerResult> add(String topic, T message, K partitionKey) {
		ProducerRecord<byte[], byte[]> record = null;
		byte []payload = serializer.serialize(message);
		if (partitionKey != null){
			byte[] kbytes;
			if (partitionKey instanceof byte[]) {
				kbytes = (byte[])(partitionKey);
			} else {
				kbytes = partitionKey.toString().getBytes(StandardCharsets.UTF_8);
			}
			record = new ProducerRecord<>(topic,kbytes, payload );
		}else {
			record = new ProducerRecord<>(topic,payload );
		}

		return new DefaultProducerResult<>(producer.send(record));
	}

	public void close() {
		if (producer != null)
			producer.close();
	}


	private static final class DefaultProducerResult<RecordMetadata> implements Future<ProducerResult> {

		private Future<RecordMetadata> wrapped;

		private DefaultProducerResult(Future<RecordMetadata> wrappedFuture) {
			this.wrapped = wrappedFuture;
		}
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return wrapped.cancel(mayInterruptIfRunning);
		}
		@Override
		public boolean isCancelled() {
			return wrapped.isCancelled();
		}
		@Override
		public boolean isDone() {
			return wrapped.isDone();
		}

		@Override
		public ProducerResult<RecordMetadata> get() throws InterruptedException, ExecutionException {
			final RecordMetadata result = wrapped.get();
			return new ProducerResult<RecordMetadata>() {
				public RecordMetadata getResult()  {
					return result;
				}
			};
		}

		@Override
		public ProducerResult<RecordMetadata> get(final long timeout, final TimeUnit unit)
				throws InterruptedException, ExecutionException, TimeoutException {
			final RecordMetadata result = wrapped.get(timeout, unit);
			return new ProducerResult<RecordMetadata>() {
				public RecordMetadata getResult() {
					return result;
				}
			};
		}

	}

}
