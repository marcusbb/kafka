package org.marcusbb.queue.kafka;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;



/**
 * currently requires connectivity to kafka service
 * 
 * @author marcus
 *
 */
public class KafkaTest {

	Properties props = null;
	
	@BeforeClass
	public static void beforeClass() {
		/*kafkaUnitServer = new KafkaUnit(6600,6601);
		kafkaUnitServer.setKafkaBrokerConfig("log.segment.bytes", "1024");
		kafkaUnitServer.startup();*/
	}
	@AfterClass
	public static void afterClass() {
		//kafkaUnitServer.shutdown();
	}
	public static class Member implements Serializable {
		int id;
		void setId(int id) {
			this.id = id;
		}
		int getId() {
			return id;
		}
	}
	@Before
	public void before() {
		props = new Properties();
		 props.put("bootstrap.servers", "localhost:6601");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	}
	//@Test
	public void testSimple() {
		

		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 0; i < 100; i++)
		     producer.send(new ProducerRecord<String, String>("my-topic", Integer.toString(i), Integer.toString(i)));

		 producer.close();
	}
	
	//@Test
	public void binarySerialization() throws Exception {
		
		props.put("value.serializer", ByteArraySerializer.class.getName());
		 Producer<String, byte[]> producer = new KafkaProducer<>(props);
		 
		 Kryo kryo = new Kryo();
		 Output output = new Output(1024,4096);
		 Member member = new Member();
		 member.setId(new Random().nextInt());
		 kryo.writeObject(output, member);
		 output.close();
		 System.out.println("kryo len: " + output.getBuffer().length);
		 
		 //java serialization
		 ByteArrayOutputStream bo = new ByteArrayOutputStream();
		 ObjectOutputStream oo = new ObjectOutputStream(bo);
		 oo.writeObject(member);
		 System.out.println("js b-len: " + bo.size());
		 
		 
	     ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>("b-topic",Integer.toString(member.getId()), output.getBuffer() );
	     ProducerRecord<String, byte[]> record2 = new ProducerRecord<String, byte[]>("topic_single_p", output.getBuffer() );
	     producer.send(record);
	     producer.send(record2);
	    
		 producer.close();
		 
	}
	
	//@Test
	public void consumeWithOffset() {
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		props.put("group.id", "customOffset");
		props.put("enable.auto.commit", "false");
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		Set<TopicPartition> assignmentSet = consumer.assignment();
	    consumer.subscribe(Arrays.asList("b-topic"));
		TopicPartition []tps = {new TopicPartition("b-topic", 0),new TopicPartition("b-topic", 1)};
		//consumer.assign(Arrays.asList(tps));
		//consumer.assign(new ArrayList<TopicPartition>(assignmentSet));
	    // consumer.seekToBeginning(new TopicPartition("b-topic", 0));
	    // consumer.seekToBeginning(new TopicPartition("b-topic", 1));
	     while (true) {
		     ConsumerRecords<String, byte[]> records = consumer.poll(5000);
		     for (ConsumerRecord<String, byte[]> record : records)
		             System.out.printf("offset = %d, key = %s, value = %s, partition=%s \n", record.offset(), record.key(), record.value().length,record.partition());
//		     System.out.println("Committing offset: " + consumer.committed(new TopicPartition("b-topic", 0)) );
//		     System.out.println("Committing offset: " + consumer.committed(new TopicPartition("b-topic", 1)) );
//		     
		     //for each of the records partitions commit the marker
		     for (TopicPartition partition : records.partitions()) {
		         List<ConsumerRecord<String, byte[]>> partitionRecords = records.records(partition);
		         for (ConsumerRecord<String, byte[]> record : partitionRecords)
		           System.out.println(record.offset() + ": " + record.value());

		         long lastoffset = partitionRecords.get(partitionRecords.size() - 1).offset();
		         consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(lastoffset + 1)));
		       }
	     }
	     
	     //consumer.close();
	}
	
	//@Test
	public void produceConsume() {
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		props.put("group.id", "b-topic-test");
		props.put("enable.auto.commit", "false");
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		Set<TopicPartition> assignmentSet = consumer.assignment();
	    consumer.subscribe(Arrays.asList("b-topic"));
		TopicPartition []tps = {new TopicPartition("b-topic", 0),new TopicPartition("b-topic", 1)};
		//consumer.assign(Arrays.asList(tps));
		//consumer.assign(new ArrayList<TopicPartition>(assignmentSet));
	    // consumer.seekToBeginning(new TopicPartition("b-topic", 0));
	    // consumer.seekToBeginning(new TopicPartition("b-topic", 1));
	     while (true) {
	     ConsumerRecords<String, byte[]> records = consumer.poll(100);
	     for (ConsumerRecord<String, byte[]> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s, partition=%s \n", record.offset(), record.key(), record.value().length,record.partition());
	     }
	     //consumer.close();
	}
	
	
	
	
	//@Test
	public void consumeSinglePartion() {
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		props.put("group.id", "group2");
		props.put("enable.auto.commit", "false");
		
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		
	    consumer.subscribe(Arrays.asList("topic_single_p"));
		//TopicPartition []tps = {new TopicPartition("topic_single_p", 0),new TopicPartition("topic_single_p", 1)};
		//consumer.assign(Arrays.asList(tps));
		//consumer.assign(new ArrayList<TopicPartition>(assignmentSet));
	     consumer.seek(new TopicPartition("topic_single_p", 0),0);
	    // consumer.seekToBeginning(new TopicPartition("b-topic", 1));
	     while (true) {
	     ConsumerRecords<String, byte[]> records = consumer.poll(100);
	     for (ConsumerRecord<String, byte[]> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s, partition=%s \n", record.offset(), record.key(), record.value().length,record.partition());
	     }
	     //consumer.close();
	}

}
