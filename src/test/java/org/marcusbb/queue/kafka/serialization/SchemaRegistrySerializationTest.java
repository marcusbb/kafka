package org.marcusbb.queue.kafka.serialization;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.kafka.utils.TestMessage;
import org.marcusbb.queue.kafka.utils.TestMessageV2;
import org.marcusbb.queue.kafka.utils.TestMessageV3;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;
import org.marcusbb.queue.serialization.impl.SchemaRegistrySerializer;
import org.marcusbb.queue.serialization.impl.ems.SchemaRegistryEMS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

public class SchemaRegistrySerializationTest {

	private final String TEST_MESSAGE_SUBJECT = "org.marcusbb.queue.kafka.utils.TestMessage";
	private SchemaRegistryClient schemaRegistryClient;

	private Integer version1;
	private Integer version2;
	private Integer version3;

	@Before
	public void before() {
		schemaRegistryClient = new MockSchemaRegistryClient();
	}

	@Test
	public void schemaRegistrySerializer() {
		SchemaRegistrySerializer<TestMessage> serializer =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);
		TestMessage origMessage = new TestMessage("hello_world_"+System.currentTimeMillis());
		byte[] serializedBytes = serializer.serialize(origMessage);

		TestMessage deserializedMessage = serializer.deserialize(serializedBytes);
		assertEquals(origMessage,deserializedMessage);
	}

	@Test
	public void setSchemaRegistryEMS(){

		SchemaRegistryEMS<TestMessage> schemaRegistryEMS =
				new SchemaRegistryEMS<>(
						schemaRegistryClient,
						new MockVersionCipher(),
						new MockKeyBuilder(),
						"",
						"",
						TEST_MESSAGE_SUBJECT);

		RoutableEncryptedMessage<TestMessage> origEnvelope =
				new RoutableEncryptedMessage<>(new TestMessage("hello_world_"+System.currentTimeMillis()));

		byte[] serializedBytes = schemaRegistryEMS.serialize(origEnvelope);

		RoutableEncryptedMessage<TestMessage> newEnvelope = schemaRegistryEMS.deserialize(serializedBytes);
		assertEquals(origEnvelope.getCreateTs(),newEnvelope.getCreateTs());
		assertEquals(origEnvelope.getPayload(),newEnvelope.getPayload());
	}


	@Test
	public void backwardCompatibilityForAddedField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		SchemaRegistrySerializer<TestMessage> serializer =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);
		TestMessage origMessage =
				new TestMessage("hello_world");
		byte[] serializedBytes = serializer.serialize(origMessage);

		SchemaRegistrySerializer<TestMessageV2> serializer2 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT, version2);

		// Deserialize v1 message as v2 message
		TestMessageV2 deserializedMessage = serializer2.deserialize(serializedBytes);

		assertEquals(origMessage.getContent(), deserializedMessage.getContent());
		assertNull(deserializedMessage.getMoreContent());
	}


	@Test
	public void forwardCompatibilityForAddedField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		SchemaRegistrySerializer<TestMessageV2> serializer2 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);
		TestMessageV2 origV2Message =
				new TestMessageV2("hello_world", "hello_universe");
		byte[] serializedBytes = serializer2.serialize(origV2Message);

		SchemaRegistrySerializer<TestMessage> serializer =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT, version1);

		// Deserialize v2 message as v1 message
		TestMessage deserializedMessage = serializer.deserialize(serializedBytes);

		assertEquals(origV2Message.getContent(), deserializedMessage.getContent());
		// moreContent field doesn't exist in this earlier version
	}

	@Test
	public void backwardCompatibilityForRenamedField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		SchemaRegistrySerializer<TestMessageV2> serializer2 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);
		TestMessageV2 origV2Message =
				new TestMessageV2("hello_world", "hello_universe");
		byte[] serializedBytes = serializer2.serialize(origV2Message);

		SchemaRegistrySerializer<TestMessageV3> serializer3 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT, version3);

		// Deserialize v2 as v3 message
		TestMessageV3 deserializedMessage = serializer3.deserialize(serializedBytes);

		assertEquals(origV2Message.getContent(), deserializedMessage.getContent());
		assertEquals(origV2Message.getMoreContent(), deserializedMessage.getAltContent());
	}

	@Test
	public void forwardIncompatibilityForRenamedField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		SchemaRegistrySerializer<TestMessageV3> serializer3 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);
		TestMessageV3 origV3Message =
				new TestMessageV3("hello_world", "hello_universe");
		byte[] serializedBytes = serializer3.serialize(origV3Message);

		SchemaRegistrySerializer<TestMessageV2> serializer2 =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT, version2);

		// Deserialize v3 as v2 message
		TestMessageV2 deserializedMessage = serializer2.deserialize(serializedBytes);

		assertEquals(origV3Message.getContent(), deserializedMessage.getContent());
		// renamed fields are *not* forward-compatible; v2 schema would have had to have been updated with v3 aliases
		assertNotEquals(origV3Message.getAltContent(), deserializedMessage.getMoreContent());
		assertNull(deserializedMessage.getMoreContent());
	}

	@Test
	public void backwardDeserializeWithCompatibleClass() throws Exception {
		TestMessage origMessage =
				new TestMessage("hello_world");

		SchemaRegistrySerializer<TestMessage> serializer =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TEST_MESSAGE_SUBJECT);

		byte[] bytes = serializer.serialize(origMessage);

		SchemaRegistrySerializer<TestMessage> deserializer =
				new SchemaRegistrySerializer<>(schemaRegistryClient, TestMessage.class);

		TestMessage deserializedMessage = deserializer.deserialize(bytes);

		assertEquals(origMessage.getContent(), deserializedMessage.getContent());
	}

	private void primeSchemaRegistryWithTestMessageSchemas() throws Exception {

		Schema schemaV1 = new AvroInferredSerializer(TestMessage.class).getWriterSchema();

		Schema schemaV2 = new AvroInferredSerializer(TestMessageV2.class).getWriterSchema();
		// Establish record alias
		schemaV2.addAlias(TEST_MESSAGE_SUBJECT);

		Schema schemaV3 = new AvroInferredSerializer(TestMessageV3.class).getWriterSchema();
		// Establish record aliases...
		schemaV3.addAlias(TEST_MESSAGE_SUBJECT);
		schemaV3.addAlias(TestMessageV2.class.getCanonicalName());
		// ... and the old field name alias
		schemaV3.getField("altContent").addAlias("moreContent");

		schemaRegistryClient.register(TEST_MESSAGE_SUBJECT, schemaV1);
		schemaRegistryClient.register(TEST_MESSAGE_SUBJECT, schemaV2);
		schemaRegistryClient.register(TEST_MESSAGE_SUBJECT, schemaV3);

		version1 = schemaRegistryClient.getVersion(TEST_MESSAGE_SUBJECT, schemaV1);
		version2 = schemaRegistryClient.getVersion(TEST_MESSAGE_SUBJECT, schemaV2);
		version3 = schemaRegistryClient.getVersion(TEST_MESSAGE_SUBJECT, schemaV3);
	}

}
