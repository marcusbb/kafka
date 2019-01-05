package org.marcusbb.queue.kafka.serialization;

import java.util.HashMap;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.KafkaTestBase;
import org.marcusbb.queue.kafka.utils.TestUtils;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.impl.KryoMessageSerializer;
import org.junit.Ignore;
import org.junit.Before;

public class KryoSerializerTest extends KafkaTestBase {
	KryoMessageSerializer serializer;

	@Before
	public void initialize() {
		serializer = new KryoMessageSerializer();
	}

	@Test(expected=SerializationException.class)
	public void testSerializationExceptionThrownOnDeserialize() {
		byte []b = new byte[0];
		serializer.register(RoutableEncryptedMessage.class);
		Object obj = serializer.deserialize(b);
	}

	@Test(expected=SerializationException.class)
	@Ignore
	public void testSerializationExceptionThrownOnSerialize() {
		// TODO: figure out how to pass an invalid object for serialization
		byte []b = serializer.serialize(new Object());
	}

	@Test
	public void testRoundTrip() {
		RoutableEncryptedMessage message = new RoutableEncryptedMessage(new HashMap<String, String>(), TestUtils.randomBytes(1024 * 10));
		byte []serializedMessage = serializer.serialize(message);
		serializer.register(RoutableEncryptedMessage.class);

		RoutableEncryptedMessage deserializedMessage = (RoutableEncryptedMessage)serializer.deserialize(serializedMessage);

		Assert.assertEquals(message.getHeaders(), deserializedMessage.getHeaders());
		Assert.assertTrue(Arrays.equals(message.getEncPayload(), deserializedMessage.getEncPayload()));
	}
}
