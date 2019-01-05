package org.marcusbb.queue.kafka.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;

import org.apache.avro.Schema;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.marcusbb.crypto.reflect.ReflectVersionedCipher;
import org.marcusbb.crypto.spi.reflect.ReflectUtil;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockKeyStore;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.message.fle.PayloadExt;
import org.marcusbb.queue.message.fle.PayloadExtV2;
import org.marcusbb.queue.message.fle.PayloadExtV3;
import org.marcusbb.queue.message.fle.TypicalCloneable;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;
import org.marcusbb.queue.serialization.impl.ems.FLERegistrySerDe;
import org.marcusbb.queue.serialization.impl.ems.FLESerializer;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

public class FieldLevelSerializationTest {

	static int schemaPort = 8091;
	static String schemaUrl = "http://localhost:" + schemaPort;
	static ReflectVersionedCipher reflectCipher;
	static SchemaRegistryClient client;

	private String payloadExtSubject;
	private Integer payloadExtVersion1;
	private Integer payloadExtVersion2;
	private Integer payloadExtVersion3;

	private final static String FOO_DEFAULT_VALUE = "bar";
	private final static String V2_FOO_VALUE = "baz";
	private final static String V3_FOO_VALUE = "zab";

	private final static String SECRET_FOO_VALUE = "pssst";

	private final static String ALT_SECRET_DEFAULT_VALUE = "shhh";

	@Before
	public void before() {
	}
	@BeforeClass
	public static void beforeClass() throws Exception {
		client = new MockSchemaRegistryClient();
		reflectCipher = new ReflectUtil(new MockKeyBuilder(),new MockVersionCipher(),new MockKeyStore());
	}

	@AfterClass
	public static void afterClass() {
		//VaultTestBase.afterClass();
	}

	private PayloadExt generatePayloadExt() {
		HashMap<String,String> headers = new HashMap<String,String>();
		 headers.put("region", "EU");
		 headers.put("corrId", "some_value");
		 PayloadExt avroObj = new PayloadExt(headers);
		 avroObj.setFirstString("hello_world");
		 avroObj.setCreditCard("01234567890123456");
		 return avroObj;
	}

	private PayloadExtV2 generatePayloadExtV2() {
		HashMap<String,String> headers = new HashMap<>();
		headers.put("region", "EU");
		headers.put("corrId", "some_value");
		PayloadExtV2 avroObj = new PayloadExtV2(headers);
		avroObj.setFirstString("hello_world");
		avroObj.setCreditCard("01234567890123456");
		avroObj.setFoo(V2_FOO_VALUE);
		avroObj.setSecretFoo(SECRET_FOO_VALUE);
		return avroObj;
	}

	private PayloadExtV3 generatePayloadExtV3() {
		HashMap<String,String> headers = new HashMap<>();
		headers.put("region", "EU");
		headers.put("corrId", "some_value");
		PayloadExtV3 avroObj = new PayloadExtV3(headers);
		avroObj.setFirstString("hello_world");
		avroObj.setCreditCard("01234567890123456");
		avroObj.setOof(V3_FOO_VALUE);
		avroObj.setSecretOof(SECRET_FOO_VALUE);
		return avroObj;
	}

	@Test
	public void testAbstract() {
		
		 AvroInferredSerializer<PayloadExt> serializer = new AvroInferredSerializer<>(PayloadExt.class);
		 
		 byte []b = serializer.serialize(generatePayloadExt());

		 assertEquals("PayloadExt",serializer.getWriterSchema().getName());

		 //assert headers and part of payload is present
		 assertNotNull(serializer.getWriterSchema().getField("bigDecimal1"));
		 assertNotNull(serializer.getWriterSchema().getField("headers"));
		 
		 PayloadExt ext = serializer.deserialize(b);

		 assertEquals("hello_world", ext.getFirstString());
		 assertEquals("EU",ext.getHeaders().get("region"));
	}

	@Test 
	public void testFLESerializer() throws IOException {
		FLESerializer fleSerializer = new FLESerializer(reflectCipher,new AvroInferredSerializer<PayloadExt>(PayloadExt.class));
		PayloadExt avroObj = generatePayloadExt();
		byte [] b = fleSerializer.serialize(avroObj);
		assertNotNull(avroObj.getShadow());
		
		//assert that credit card got nullified
		PayloadExt nonDecrypted = new AvroInferredSerializer<PayloadExt>(PayloadExt.class).deserialize(b);
		assertNotNull(nonDecrypted);
		assertNull(nonDecrypted.getCreditCard());

		PayloadExt decrypted = (PayloadExt)fleSerializer.deserialize(b);
		assertNotNull(decrypted);

		assertNotNull(decrypted.getCreditCard());
	}

	@Test
	public void testFLESerDe() throws Exception {
		FLERegistrySerDe<TypicalCloneable> serDe = new FLERegistrySerDe<>(client, "topic2");
		byte[] b = serDe.serialize(new TypicalCloneable("0123456789"));
		assertEquals(FLERegistrySerDe.MAGIC_BYTE,b[0]);
		TypicalCloneable cloneable = serDe.deserialize(b);
		assertNotNull(cloneable);

		serDe = new FLERegistrySerDe<>(client,"topic_encrypt", reflectCipher);
		byte []encrypted = serDe.serialize(new TypicalCloneable("1234556789"));
		serDe.deserialize(encrypted);
	}

	@Test
	public void testFLESerializerWithEmbeddedSchema() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// For serialized test record...
		FLERegistrySerDe<PayloadExtV3> serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher);
		PayloadExtV3 orig = generatePayloadExtV3();
		byte[] bytes = serializer.serialize(orig);

		FLERegistrySerDe<PayloadExtV3> deserializer =
				new FLERegistrySerDe<>(client, PayloadExtV3.class, payloadExtSubject);
		PayloadExtV3 deserialized = deserializer.deserialize(bytes);

		assertEquals(orig.getFirstString(), deserialized.getFirstString());
	}
	
	@Test
	public void testNonDecryptCompatibility() throws Exception {
		// For serialized test record...
		FLERegistrySerDe<PayloadExt> serDe =
				new FLERegistrySerDe<>(client, "topic_encrypt", reflectCipher);
		byte []encrypted = serDe.serialize(generatePayloadExt());

		// ...de-serialize with original crypto-enabled serializer
		PayloadExt sensitive = serDe.deserialize(encrypted);
		assertNotNull(sensitive.getCreditCard());
		
		// ...and with a non-encrypted serializer
		serDe = new FLERegistrySerDe<>(client, "topic_encrypt");
		PayloadExt noSensitive = serDe.deserialize(encrypted);
		assertNull(noSensitive.getCreditCard());
	}

	@Test
	public void testNonDecryptCompatibilityWithNonNullDefaultedField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		String instanceAltSecret = "orig-alt-secret";

		// For test record...
		PayloadExtV3 origPayload = generatePayloadExtV3();
		origPayload.setAltSecret(instanceAltSecret);

		// ...serialize with encrypted version-3 serializer
		FLERegistrySerDe<PayloadExtV3> encryptedSerializer =
				new FLERegistrySerDe<>(client, "topic_encrypt", reflectCipher);
		byte[] bytes = encryptedSerializer.serialize(origPayload);

		// Deserialize using an unencrypted serializer
		FLERegistrySerDe<PayloadExtV3> unencryptedSerializer =
				new FLERegistrySerDe<>(client, "topic_encrypt");
		PayloadExtV3 deserializedPayload = unencryptedSerializer.deserialize(bytes);

		assertNull(deserializedPayload.getAltSecret()); // alt-secret remains null; Avro non-null default does not interfere

		// Deserialize using the encrypted serializer
		deserializedPayload = encryptedSerializer.deserialize(bytes);
		assertEquals(instanceAltSecret, deserializedPayload.getAltSecret());
	}

	@Test
	public void testBackwardCompatibilityWithAddedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an original version of PayloadExt...
		PayloadExt origPayload = generatePayloadExt();

		// ...serialize to bytes with encrypted version-1 serializer.
		FLERegistrySerDe<PayloadExt> v1Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion1);
		byte[] bytes = v1Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-2 serializer
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion2);
		PayloadExtV2 deserializedPayload = v2Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNull(deserializedPayload.getCreditCard()); // encrypted field stays null
		assertNull(deserializedPayload.getSecretFoo()); // new null-defaulted, encrypted field stays null
		assertEquals(FOO_DEFAULT_VALUE, deserializedPayload.getFoo()); // new unencrypted field contains non-null default

		// Deserialize using an encrypted version-2 serializer
		v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		deserializedPayload = v2Serializer.deserialize(bytes);

		assertNull(deserializedPayload.getSecretFoo()); // secretFoo is available with its default value
	}

	/**
	 * This test highlights how specifying a serializer with a message-type class diminishes the authority of
	 * schema registry in de-serialized outcomes.  Specifically, if the inferred schema of the specified class
	 * varies from what ought to be the corresponding one in the schema registry, then behaviours expected to
	 * occur because of the schema registry version will not occur.  Avro annotations in code may address some
	 * of the gaps between inferred and declared schemas.
	 *
	 * NB: This dichotomy applies to any schema-registry oriented serializer; e.g. SchemaRegistrySerializer
	 *
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibilityWithAddedFieldsUsingReturnTypeDerivedReaderSchema() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an original version of PayloadExt...
		PayloadExt origPayload = generatePayloadExt();

		// ...serialize to bytes with encrypted version-1 serializer.
		FLERegistrySerDe<PayloadExt> v1Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion1);
		byte[] bytes = v1Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-2 serializer that designates reader-schema with message class-type
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, PayloadExtV2.class, payloadExtSubject);
		PayloadExtV2 deserializedPayload = v2Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());

		assertNull(deserializedPayload.getFoo()); // non-null default is not understood in class-reflected schema
	}

	/**
	 * This test highlights how specifying a serializer with an avro-version-annotated message-type class retains
	 * the authority of schema registry in de-serialized outcomes.  The version allows for a readerSchema to be
	 * looked up by retrieving the version from the annotation on the class.
	 * @throws Exception
	 */
	@Test
	public void testBackwardCompatibilityWithAddedFieldsUsingVersionAnnotatedReturnTypeDerivedReaderSchema() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an original version of PayloadExt...
		PayloadExt origPayload = generatePayloadExt();

		// ...serialize to bytes with encrypted version-1 serializer.
		FLERegistrySerDe<PayloadExt> v1Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion1);
		byte[] bytes = v1Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-3 serializer that designates reader-schema
		// (and subject) with annotated message class-type
		FLERegistrySerDe<PayloadExtV3> v2Serializer =
				new FLERegistrySerDe<>(client, PayloadExtV3.class);
		PayloadExtV3 deserializedPayload = v2Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());

		assertEquals(FOO_DEFAULT_VALUE, deserializedPayload.getOof()); // new unencrypted field contains non-null default
	}

	@Test
	public void testNewNonNullDefaultedEncryptedFieldBackwardCompatibleWhenDeserializedWithoutCrypto() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an version-2 of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();

		// ...serialize to bytes with encrypted version-2 serializer.
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-3 serializer
		FLERegistrySerDe<PayloadExtV3> v3Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion3);
		PayloadExtV3 deserializedPayload = v3Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNull(deserializedPayload.getCreditCard()); // encrypted field stays null

		// The new encrypted field with non-null default does not get set to null;
		// instead, the Avro non-null default is applied during deserialization.
		// There was no encrypted value, so there is nothing to 'guard'
		assertEquals(ALT_SECRET_DEFAULT_VALUE, deserializedPayload.getAltSecret());
	}


	@Test
	public void testUnencryptedForwardCompatibilityWithAddedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With a new version of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();

		// ...serialize to bytes with encrypted version-2 serializer.
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-1 serializer
		FLERegistrySerDe<PayloadExt> v1Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion1);
		PayloadExt deserializedPayload = v1Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNull(deserializedPayload.getCreditCard()); // encrypted field stays null
	}

	@Test
	public void testEncryptedForwardCompatibilityWithAddedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With a new version of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();

		// ...serialize to bytes with encrypted version-2 serializer.
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize using an encrypted version-1 serializer
		FLERegistrySerDe<PayloadExt> v1Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion1);

		PayloadExt deserializedPayload = v1Serializer.deserialize(bytes);
		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNotNull(deserializedPayload.getCreditCard()); // encrypted field has decrypted value
	}

	@Test
	public void testUnencryptedBackwardCompatibilityWithRenamedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an version-2 of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();

		// ...serialize to bytes with encrypted version-2 serializer.
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-3 serializer
		FLERegistrySerDe<PayloadExtV3> v3Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion3);
		PayloadExtV3 deserializedPayload = v3Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNull(deserializedPayload.getCreditCard()); // null-defaulted, encrypted field stays null
		assertNull(deserializedPayload.getSecretOof()); // renamed null-defaulted, encrypted field stays null
		assertEquals(V2_FOO_VALUE, deserializedPayload.getOof()); // renamed unencrypted field contains mapped value
	}

	@Test
	public void testEncryptedBackwardIncompatibilityWithRenamedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an version-2 of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();

		// ...serialize to bytes with encrypted version-2 serializer.
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize using an encrypted version-3 serializer
		FLERegistrySerDe<PayloadExtV3> v3Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion3);
		PayloadExtV3 deserializedPayload = v3Serializer.deserialize(bytes);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNotNull(deserializedPayload.getCreditCard()); // encrypted field has decrypted value
		assertEquals(V2_FOO_VALUE, deserializedPayload.getOof()); // renamed unencrypted field contains mapped value

		assertNull(deserializedPayload.getSecretOof()); // renamed encrypted field does NOT get mapped value; byte-shadow does not support alias
	}

	@Test
	public void testForwardIncompatibilityWithRenamedFields() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With an version-3 of PayloadExt...
		PayloadExtV3 origPayload = generatePayloadExtV3();

		// ...serialize to bytes with encrypted version-3 serializer.
		FLERegistrySerDe<PayloadExtV3> v3Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion3);
		byte[] bytes = v3Serializer.serialize(origPayload);

		// Deserialize using an unencrypted version-2 serializer
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion2);
		PayloadExtV2 deserializedPayload = v2Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		// The renamed field in the v3 message CANNOT get its value mapped to the corresponding originally-named field
		assertNotEquals(V3_FOO_VALUE, deserializedPayload.getFoo());
		assertEquals(FOO_DEFAULT_VALUE, deserializedPayload.getFoo());

		// Deserialize using an encrypted version-2 serializer
		v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, reflectCipher, payloadExtVersion2);
		deserializedPayload = v2Serializer.deserialize(bytes);
		assertNotNull(deserializedPayload);
		// The renamed encrypted field in the v3 message CANNOT get its value mapped to the corresponding originally-named field
		assertNull(deserializedPayload.getSecretFoo());
	}

	@Test
	public void testNullabilityOfNonNullByDefaultField() throws Exception {
		primeSchemaRegistryWithTestMessageSchemas();

		// With a new version of PayloadExt...
		PayloadExtV2 origPayload = generatePayloadExtV2();
		// ...set 'foo' field value to null
		origPayload.setFoo(null);

		// ...and serialize to bytes with regular version-2 serializer
		FLERegistrySerDe<PayloadExtV2> v2Serializer =
				new FLERegistrySerDe<>(client, payloadExtSubject, payloadExtVersion2);
		byte[] bytes = v2Serializer.serialize(origPayload);

		// Deserialize with the same serializer
		PayloadExtV2 deserializedPayload = v2Serializer.deserialize(bytes);

		assertNotNull(deserializedPayload);
		assertEquals("hello_world", deserializedPayload.getFirstString());
		assertNull(deserializedPayload.getFoo()); // null set field with non-null default is interpreted as null
	}


	private void primeSchemaRegistryWithTestMessageSchemas() throws Exception {
		Schema schemaV1 = new AvroInferredSerializer(PayloadExt.class).getWriterSchema();
		payloadExtSubject = schemaV1.getName(); // extract inferred schema subject

		// Use an explicit custom schema in order to adjust the type of the new `foo`
		// field to have a non-null default, but capable of being set to null; this
		// does not appear to work with a code-first annotations and reflection approach.
		String altSchemaV2 = "{\"type\":\"record\",\"name\":\"PayloadExtV2\",\"namespace\":\"org.marcusbb.queue.message.fle\",\"aliases\":[\"" + payloadExtSubject + "\"],\"fields\":[{\"name\":\"bigDecimal1\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BigDecimal\",\"namespace\":\"org.marcusbb.queue.serialization.avro.types\",\"fields\":[{\"name\":\"scaled\",\"type\":\"int\"},{\"name\":\"unscaled\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}],\"default\":null},{\"name\":\"firstString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"notNullable\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"firstLong\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"date\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Timestamp\",\"namespace\":\"org.marcusbb.queue.serialization.avro.types\",\"fields\":[{\"name\":\"epochLong\",\"type\":\"long\"}]}],\"default\":null},{\"name\":\"appendedv2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"creditCard\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"foo\",\"type\":[\"string\",\"null\"],\"default\":\"" + FOO_DEFAULT_VALUE + "\"},{\"name\":\"secretFoo\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"headers\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"default\":null},{\"name\":\"shadow\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Shadow\",\"namespace\":\"org.marcusbb.queue\",\"fields\":[{\"name\":\"fields\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ShadowField\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"algorithm\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"base64Encoded\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"keyAlias\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ivAlias\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"isTokenized\",\"type\":\"boolean\"}]}}],\"default\":null}]}],\"default\":null},{\"name\":\"ts\",\"type\":\"long\"}]}";
		Schema schemaV2 = new Schema.Parser().parse(altSchemaV2);

		String altSchemaV3 = "{\"type\":\"record\",\"name\":\"PayloadExtV3\",\"namespace\":\"org.marcusbb.queue.message.fle\",\"aliases\":[\"PayloadExtV2\",\"" + payloadExtSubject + "\"],\"fields\":[{\"name\":\"bigDecimal1\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"BigDecimal\",\"namespace\":\"org.marcusbb.queue.serialization.avro.types\",\"fields\":[{\"name\":\"scaled\",\"type\":\"int\"},{\"name\":\"unscaled\",\"type\":[\"null\",\"bytes\"],\"default\":null}]}],\"default\":null},{\"name\":\"firstString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"notNullable\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"firstInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"firstLong\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"date\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Timestamp\",\"namespace\":\"org.marcusbb.queue.serialization.avro.types\",\"fields\":[{\"name\":\"epochLong\",\"type\":\"long\"}]}],\"default\":null},{\"name\":\"appendedv2\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"creditCard\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"oof\",\"type\":[\"string\",\"null\"],\"default\":\"" + FOO_DEFAULT_VALUE + "\",\"aliases\":[\"foo\"]},{\"name\":\"secretOof\",\"type\":[\"null\",\"string\"],\"default\":null,\"aliases\":[\"secretFoo\"]},{\"name\":\"altSecret\",\"type\":[\"string\",\"null\"],\"default\":\"" + ALT_SECRET_DEFAULT_VALUE + "\"},{\"name\":\"version\",\"type\":\"int\"},{\"name\":\"headers\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"string\"}],\"default\":null},{\"name\":\"shadow\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Shadow\",\"namespace\":\"org.marcusbb.queue\",\"fields\":[{\"name\":\"fields\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"ShadowField\",\"fields\":[{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"algorithm\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"base64Encoded\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"keyAlias\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ivAlias\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"isTokenized\",\"type\":\"boolean\"}]}}],\"default\":null}]}],\"default\":null},{\"name\":\"ts\",\"type\":\"long\"}]}";
		Schema schemaV3 = new Schema.Parser().parse(altSchemaV3);

		client.register(payloadExtSubject, schemaV1);
		client.register(payloadExtSubject, schemaV2);
		client.register(payloadExtSubject, schemaV3);

		payloadExtVersion1 = client.getVersion(payloadExtSubject, schemaV1);
		payloadExtVersion2 = client.getVersion(payloadExtSubject, schemaV2);
		payloadExtVersion3 = client.getVersion(payloadExtSubject, schemaV3);
	}

	@Test
	public void testIfEncryptedFieldExist() throws Exception {
		FLESerializer fleSerializer = new FLESerializer(reflectCipher,new AvroInferredSerializer<PayloadExt>(PayloadExt.class));

		// For a record with a credit card value...
		PayloadExt avroObj = generatePayloadExt();

		// ...serialize with encryption...
		byte [] b = fleSerializer.serialize(avroObj);
		assertNotNull(avroObj.getShadow());
		// ... and de-serialize without decryption.
		PayloadExt nonDecrypted = new AvroInferredSerializer<PayloadExt>(PayloadExt.class).deserialize(b);

		// The non-decrypted record has the credit card nullified...
		assertNull(nonDecrypted.getCreditCard());
		// ... but isFieldNull still indicates that there is a value because of the underlying field shadow
		assertFalse(nonDecrypted.isFieldNull("creditCard"));

		// For a record ...
		avroObj = generatePayloadExt();
		// ... without a credit card value...
		avroObj.setCreditCard(null);

		// ...serialize with encryption...
		b = fleSerializer.serialize(avroObj);
		assertNotNull(avroObj.getShadow());
		// ... and de-serialize without decryption.
		nonDecrypted = new AvroInferredSerializer<PayloadExt>(PayloadExt.class).deserialize(b);

		// The non-decrypted record has no value in either the regular field, or the shadow
		assertTrue(nonDecrypted.isFieldNull("creditCard"));

		// NB: isFieldNull checks both shadow and the field itself; we can redundantly assert on the field
		assertNull(nonDecrypted.getCreditCard());
	}


}
