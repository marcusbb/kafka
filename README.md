# Kafka client library

There is a bag full of fun Kafka features in here:

## Building
Requires maven.

There is a dependency to the crypto library, where secrets are stored and managed by Hashicorp Vault.
Please see [crypto](https://github.com/marcusbb/crypto) - they aren't in maven central - but simple to build.

```
mvn clean install
```

### JMS like producer and consumer interfacing
Remove the boiler plate of implementing a Consumer, and simply handle events by providing your own serialization

### Serialization
There are quite a few options to chose from.  Most notable is a field level encryption strategy to ensure that 
secret or sensitive bits of your messages are safe both in transit and on disk.
FLE serializer in particular brand is also Confluent Serde compatible and can integrate with schema registry.


### Retry 
When dealing with consumers that will fail, you don't simply want to ignore the problem. This is based on a pattern to 
re-deliver messages to your message handlers on a configurable/programmable back off policy.
Most handling of consumer failure (usually in conjunction with some remote interaction) is left to the user.  
This will help you define a single implementation for encapsulating garanteed (at least once) message processing.


### Producers

Producer: The generators of events.
There are 2 flavours of producer encapsulated by the [Producer](src/main/java/org/marcusbb/queue/Producer.java) 

- [Simple Producer](src/main/java/org/marcusbb/queue/kafka/SimpleProducer.java):  Which provides elementary implementation for Producer interface.
- [JTA Producer](src/main/java/org/marcusbb/queue/kafka/KafkaJTAProducer.java) - this will be integrated into a listening to the JTA transaction manager where EE type containment can interact appropriately with transaction semantics.


### Consumer Dispatcher
Consumer - The downstream listener and [consumer](src/main/java/org/marcusbb/queue/Consumer.java) for those events.  
consumerRegistry - consumers should be registered to an implementation of the ConsumerRegistry.

Currently the ConsumerRegistry's are implemented as the dispatching implementations of Consumer - meaning that if you choose a flavour of ConsumerDispatcher (src/main/java/org/marcusbb/queue/kafka/AbstractConsumer) it will necessarily implement ConsumerRegistry.
Each registry only allows a single consumer registry instance.  

Your consumer should be thread safe.  To ensure that it is thread safe, please have an instance of each ConsumerRegistry instance.

### Consumer Dispatcher types
- Default consumer - this consumer saves offsets *after* dispatching, meaning that failure to process from onMessage will result in failure of the consumer or by default will generate the exact same message in a "retry" topic
- Retrying consumer - by it's name saves offsets only after successful dispatching and consumption of message.  On failure of message processing by the client application, the consumer attempts to pause the appropriate topic + partition so that at most only one message is kept in memory to be tried at successive intervals - configurable via poll interval, with a configured number of retry attempts.  3 by default
- At most once consumer - this consumer will consumer and dispatch at most once.  It has no garantees of deliver, and has a very small window where messages may in fact may not be delivered

Available configuration:
Included with all possible configurations of the Kafka consumer there are additional properties that can be configure
- consumer.poll: The period at which Kafka consumer will poll brokers and return with a ConsumerRecord set.
- consumer.fromBeginning: -  true/false - Whether to start the consumer from the low water mark.  Kafka clients by default start from a high water mark of the current topic.  False by default
- consumer.throwOnSer: true/false - Whether to throw serialization exceptions to the client. True by default.
- consumer.retryAttempts:  Only available on KafkaRetryConsumerDispatcher.  This is the max retry atttempts configured per retry.

## Serialization

This library exposes a serializer abstraction [ByteSerializer](src/main/java/org/marcusbb/queue/ByteSerializer.java) that hides some of the concerns of message formatting.  Java and Kyro are the 2 current mechanisms supported.  You can implement a custom one easily and pass into the Consumer/Producer implementations.  
There are several flavours offered
- java serialization:  standard java serialization
- kryo serialization: uses kryo instead of java 
- Avro serialization:  This is an Avro serializer that depends on an inputted schema to read/write data
- Avro inferred serialization:  This uses Avro reflection to make a best effort estimate at runtime of the schema of the POJO and serialize bia avro
- EncryptedMessageSerializer:  This uses a double wrapped serialization technique, and relies on a standard format RoutableEncryptedMessage for serialization.  It contains both clear header information which is on the envolope of the message and an payload object which is encrypted via the crypto library.
- AvroEMS - This is a variation of EncryptedMessageSerializer.  Instead of using Kryo ast the outer message serialization format, it relies on Avro to serialize the envelop, thereby allowing non java clients to participate in routing type logic and concerns 

- Field level - Field level encryption defines an opinionated data structure base class called [AbstractREM](src/main/java/org/marcusbb/queue/AbstractREM.java).  There is a [Shadow](src/main/java/org/marcusbb/queue/Shadow.java) that are mandated within the AbstractREM, which you extend.  One of the main reasons why this is a concrete abstraction instead of interfaces is that developers are unlikely to want to maintain the structure appropriately.  There are also some issues limitations within Avro reflection.  AbstractREM is meant to just hold your data and you demark your fields (they don't have to be just primitive types) 
In the eample below the annotation definitions are found in the [crypto project](https://github.com/marcusbb/crypto).  Out of the box strings and longs are supported.  If you want another type supported you will either need to use the generic type (java serialization) or you can write your own.

```java
public class FooEvent extends AbstractREM {
		
	@EncryptedField(alias = "Name", iv = "iv1")
	String name;

	Bar bar; // you have other stuff 

	public Foo() {} // Deserialization constructor

	public String getName() {
		return name;
	}

	public void setName(String value) {
		name = value;
	}

}
```
Effectively what you're doing is defining your schema in java objects - a metadata driven approach to defining schema.  This does not bind you to java per se, as schema is intended to be generated and provisioned in schema registry prior to the first usage of the serializer.  Confluent Serde, optimistically publishes schema on message production, which can be added into this FLE SerDe.
