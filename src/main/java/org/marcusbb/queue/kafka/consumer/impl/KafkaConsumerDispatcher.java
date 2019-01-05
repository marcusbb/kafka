package org.marcusbb.queue.kafka.consumer.impl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.kafka.consumer.Consumer;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * 
 * 
 *	An at least once message delivery consumer,
 *	
 *	Delivers and commits offsets in a batch interval controlled by polling batch size
 *
 *
 */
public class KafkaConsumerDispatcher<T> extends AbstractConsumer<T> {

	private KafkaProducer<byte[],byte[]> errorProducer;

	public KafkaConsumerDispatcher(Properties props, ByteSerializer<T> serializer, Stats stats) {
		super(props, serializer, stats);

		if (props.containsKey("consumer.throwOnDeliveryFailure"))
			this.throwOnDeliveryFailure = Boolean.parseBoolean(props.getProperty("consumer.throwOnDeliveryFailure"));
	}

	public KafkaConsumerDispatcher(Properties props, ByteSerializer<T> serializer) {
	this(props, serializer, null);
	}

	@Override
	public void register(String topic, Consumer<T> consumer) {
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		errorProducer = new KafkaProducer<byte[], byte[]>(props);
		super.register(topic, consumer);
	}

	@Override
	protected KafkaProducer<byte[], byte[]> getErrorProducer() {
		return errorProducer;
	}

	@Override
	public void runLoop()  {

		while (subscribed ) {
			//will cycle on the kafka consumer poll timeout

			ConsumerRecords<byte[], byte[]> records = ConsumerRecords.empty();
			try {
				records = kafkaConsumer.poll(poll);
			}
			 // unrecoverable exceptions
			 catch (KafkaException | IllegalArgumentException | IllegalStateException unrecoverableEx ) {
				logger.error("unrecoverable exception on poll from topic={},error={}", topicName, unrecoverableEx.getMessage(), unrecoverableEx);
				throw unrecoverableEx;
			} catch (Exception e) {
				logger.error("exception on poll from topic={},error={}", topicName, e.getMessage(), e);
			}

			for (ConsumerRecord<byte[], byte[]> record : records) {
				logger.debug("Got records: count={}, offset={}, topic={}, partition={} ", records.count(), record.offset() ,record.topic(), record.partition() );

				TopicPartition topicPartition = new TopicPartition(record.topic(),record.partition());
				Long offset = deliveredOffsets.get(topicPartition);

				if (offset != null && record.offset() <= offset) {
					logger.warn("skipping offset to avoid re-delivery of message : topic={},partition={},offset={}",record.topic(),record.partition(),record.offset());
					continue;
				}

				T msg = null;
				try {

					//this could be a serialization error
					msg = handleSerialization(record.value());

					deliver(msg, record.key());

				} catch (Exception ex){
					handleException(ex, msg,record);
				}

				deliveredOffsets.put(topicPartition,record.offset());
			}
			// for each of the records partitions commit the marker
			commitOffsets(records);
		}

		// shutdown error producer
		if (errorProducer !=null){
			errorProducer.close();
			errorProducer = null;
		}

	}
	
	

}
