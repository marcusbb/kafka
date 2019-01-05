package org.marcusbb.queue.kafka.producer;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingProducerInterceptor implements ProducerInterceptor<byte[], byte[]>{

	Logger logger = LoggerFactory.getLogger(LoggingProducerInterceptor.class);
	private Map<String,?> config;
	
	//required due to reflection concerns?
	public LoggingProducerInterceptor() {}
	
	public LoggingProducerInterceptor(Class<?> ref) {
		logger = LoggerFactory.getLogger(ref);
	}
	
	@Override
	public void configure(Map<String, ?> configs) {
		this.config = configs;
		logger.info("configure: {}",configs);
	}

	@Override
	public ProducerRecord<byte[], byte[]> onSend(ProducerRecord<byte[], byte[]> record) {
		//return unmodified data
		return record;
	}

	@Override
	public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
		if (metadata != null) {
			//logger.debug("successfully sent/acknowleged");
		}
		if (exception != null) {
			String metadataLog = "";
			if (metadata != null){
				 metadataLog = "topic="+metadata.topic()+",partition="+metadata.partition();
			}
			logger.error("producer exception: msg={}, configs={},metadata={}",exception.getMessage(),config,metadataLog,exception);
		}
		
	}

	@Override
	public void close() {
		// No clean up necessary
		
	}

}
