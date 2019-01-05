package org.marcusbb.queue.kafka.serialization;

import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.KafkaTestBase;
import org.marcusbb.queue.kafka.utils.Event;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;
import org.marcusbb.queue.serialization.impl.ems.EncryptedMessageSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.*;

public class EncryptedMessageSerializerTest extends KafkaTestBase {
	
	@Test
	public void encryptedDefaultTest() {
		RoutableEncryptedMessage<String> msg = new RoutableEncryptedMessage<>(new HashMap<String,String>(),
				new String("going my way"));
		
		EncryptedMessageSerializer<String> emSerializer = new EncryptedMessageSerializer(new MockVersionCipher(), new MockKeyBuilder(), "msg_routing", "msg_routing"){};
		//put on wire
		byte []encrypted = emSerializer.serialize(msg);
		
		//take off wire
		RoutableEncryptedMessage output = emSerializer.deserialize(encrypted);
		assertTrue(output instanceof RoutableEncryptedMessage);
		
		assertEquals(msg.getPayload(),output.getPayload());
		
		//can go back (routing)
		byte [] b = emSerializer.serialize(output);
		RoutableEncryptedMessage tway = (RoutableEncryptedMessage)emSerializer.deserialize( b  );
		
		assertEquals( msg.getPayload(), tway.getPayload() );
		
	}

	@Test
	public void encryptedAvroAvroTest() {
		Event orig  = new Event(10,new Event.TimeStamp(12,12),new BigDecimal(0));
		RoutableEncryptedMessage<Event> msg = new RoutableEncryptedMessage<>(new HashMap<String,String>(),orig);

		EncryptedMessageSerializer<Event> emSerializer = new EncryptedMessageSerializer<Event>(
				new MockVersionCipher(), new MockKeyBuilder(), iv,keyName,
				new AvroInferredSerializer<Event>(Event.class),
				new AvroInferredSerializer<RoutableEncryptedMessage<Event>>(RoutableEncryptedMessage.class)
		){};

		//put on wire
		byte []encrypted = emSerializer.serialize(msg);

		//take off wire
		RoutableEncryptedMessage<Event> output = emSerializer.deserialize(encrypted);
		assertThat(output, instanceOf(RoutableEncryptedMessage.class));
		assertThat(output.getPayload(),instanceOf(Event.class));
		assertEquals(orig,output.getPayload());
	}

	@Test
	public void encryptedAvroEMSTest() {
		RoutableEncryptedMessage msg = new RoutableEncryptedMessage(new HashMap<String,String>(),new String("going my way"));

		AvroEMS<String> emSerializer = new AvroEMS(new MockVersionCipher(), new MockKeyBuilder(), "msg_routing", "msg_routing");

		//put on wire
		byte []encrypted = emSerializer.serialize(msg);

		//take off wire
		RoutableEncryptedMessage output = emSerializer.deserialize(encrypted);
		assertTrue(output instanceof RoutableEncryptedMessage);
		//Assert.assertTrue(output instanceof EncryptedMessageSerializer.DeferredDecryptMessage);

		((RoutableEncryptedMessage)output).getPayload().equals(new String("going my way"));

		//can go back (routing)
		byte [] b = emSerializer.serialize(output);
		//Assert.assertEquals(ByteBuffer.wrap(encrypted), ByteBuffer.wrap(b));
		RoutableEncryptedMessage tway = (RoutableEncryptedMessage)emSerializer.deserialize( b  );

		assertEquals( new String("going my way"), tway.getPayload() );

	}

	@Test
	public void encryptedMessageSerializerTest() throws IOException, NoSuchAlgorithmException {
		EncryptedMessageSerializer<String> emSerializer = new AvroEMS<>(new MockVersionCipher(),new MockKeyBuilder(),iv, keyName);

		byte []encrypted = emSerializer.serialize(new RoutableEncryptedMessage<>(new HashMap<String,String>(),new String("going my way")));

		RoutableEncryptedMessage msg = (RoutableEncryptedMessage)emSerializer.deserialize(encrypted);

		assertEquals("going my way", msg.getPayload());
	}

	@Test
	public void avroSerializerTest() throws IOException, NoSuchAlgorithmException {
		AvroEMS<String> emSerializer = new AvroEMS<>(new MockVersionCipher(),new MockKeyBuilder(),iv,keyName);

		byte []encrypted = emSerializer.serialize(new RoutableEncryptedMessage<>(new HashMap<String,String>(),new String("going my way")));

		RoutableEncryptedMessage msg = (RoutableEncryptedMessage)emSerializer.deserialize(encrypted);

		assertEquals("going my way", msg.getPayload());
	}

	@Test(expected=SerializationException.class)
	public void testSerializationExceptionThrownOnDeserialize() {
		EncryptedMessageSerializer serializer = new EncryptedMessageSerializer(new MockVersionCipher(), new MockKeyBuilder(), "msg_routing", "msg_routing"){};
		byte []b = new byte[0];
		Object obj = serializer.deserialize(b);
	}

	@Test(expected=SerializationException.class)
	public void testSerializationExceptionThrownOnSerialize() {
		EncryptedMessageSerializer serializer = new EncryptedMessageSerializer(new MockVersionCipher(), new MockKeyBuilder(), "msg_routing", "msg_routing"){};
		byte []b = serializer.serialize(null);
	}

	@Test
	public void testSerializeIdempotency(){
		EncryptedMessageSerializer serializer = new EncryptedMessageSerializer(new MockVersionCipher(), new MockKeyBuilder(), "msg_routing", "msg_routing"){};
		RoutableEncryptedMessage message = new RoutableEncryptedMessage("test value");
		byte[] msgBytes = serializer.serialize(message);
		RoutableEncryptedMessage deserializeMsg = serializer.deserialize(msgBytes);
		assertNotNull(deserializeMsg.getEncPayload());
		byte[] nMsgBytes = serializer.serialize(deserializeMsg);
		RoutableEncryptedMessage nDeserializeMsg = serializer.deserialize(nMsgBytes);
		assertEquals(deserializeMsg.getPayload(),nDeserializeMsg.getPayload());
	}
}
