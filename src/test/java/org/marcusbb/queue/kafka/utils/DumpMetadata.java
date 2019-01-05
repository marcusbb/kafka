package org.marcusbb.queue.kafka.utils;

import java.util.ArrayList;
import java.util.List;

import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;

public class DumpMetadata {

	public static void main(String[] args) {
        kafka.javaapi.consumer.SimpleConsumer consumer  = new SimpleConsumer("localhost",
                9092,
                100000,
                64 * 1024, "test");
        List<String> topics2 = new ArrayList<String>();
        //topics2.add("tr2");
        TopicMetadataRequest req = new TopicMetadataRequest(topics2);
        kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
        List<kafka.javaapi.TopicMetadata> data3 =  resp.topicsMetadata();
        for (kafka.javaapi.TopicMetadata item : data3) {
            System.out.println("Topic: " + item.topic());
            for (kafka.javaapi.PartitionMetadata part: item.partitionsMetadata() ) {
                String replicas = "";
                String isr = "";
                for (kafka.cluster.BrokerEndPoint replica: part.replicas() ) {
                	
                    replicas += " " + replica.host();
                }
                for (kafka.cluster.BrokerEndPoint replica: part.isr() ) {
                    isr += " " + replica.host();
                }
                System.out.println( "    Partition: " +   part.partitionId()  + ": Leader: " + part.leader().host() + " Replicas:[" + replicas + "] ISR:[" + isr + "]");
            }
        }
    }
}
