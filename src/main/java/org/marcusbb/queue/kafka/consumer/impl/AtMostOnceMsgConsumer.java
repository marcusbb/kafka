package org.marcusbb.queue.kafka.consumer.impl;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * 
 *	An at most once message delivery semantics.
 *	There is no retry message produced.
 *  
 *  It has an additional performance penalty by saving the offset for each message dispatched.
 *
 */
public class AtMostOnceMsgConsumer<T> extends AbstractConsumer<T> {

	//Currently, we don't have specific metrics to collect from this consumer.
	public AtMostOnceMsgConsumer(Properties props, ByteSerializer<T> serializer) {
		super(props, serializer, null);
	}
	
	
	@Override
	protected KafkaProducer<byte[], byte[]> getErrorProducer() {
		throw new UnsupportedOperationException("This consumer does not support error topics");
	}


	@Override
	public void runLoop()  {
		
		while (subscribed ) {
			//will cycle on the kafka consumer poll count

			ConsumerRecords<byte[], byte[]> records = kafkaConsumer.poll(poll);

			for (ConsumerRecord<byte[], byte[]> record : records) {
				logger.debug("Got records: count={}, offset={}, topic={}, partition={} ", records.count(), record.offset() ,record.topic(), record.partition() );
				try {
					//this could be a serialization error
					T msg = handleSerialization(record.value());
					deliver(msg, record.key());
				}catch (Exception ex){
					logger.error("Exception in processing record - topic:{},partition:{},offset:{},error:{}",
							topicName,record.partition(),record.offset(),ex.getMessage(),ex);
				}
			}
			commitOffsets(records);
		}
	}
	
	

}
