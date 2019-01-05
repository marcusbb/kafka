package org.marcusbb.queue.kafka.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.marcusbb.crypto.reflect.EncryptedField;
import org.marcusbb.crypto.reflect.ReflectVersionedCipher;
import org.marcusbb.crypto.spi.reflect.ReflectUtil;
import org.marcusbb.queue.AbstractREM;
import org.marcusbb.queue.kafka.utils.MockKeyBuilder;
import org.marcusbb.queue.kafka.utils.MockKeyStore;
import org.marcusbb.queue.kafka.utils.MockVersionCipher;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;
import org.marcusbb.queue.serialization.impl.ems.FLESerializer;


public class AbstractREMTest {

	public static class Foo extends AbstractREM {
		@EncryptedField(alias = "Name", iv = "iv1")
		String name;

		public Foo() {} // Deserialization constructor

		public String getName() {
			return name;
		}

		public void setName(String value) {
			name = value;
		}
	}

	public static class Baz extends Foo {
		@EncryptedField(alias = "Moniker", iv = "iv1")
		private String moniker;

		public Baz() {} // Deserialization constructor

		public String getMoniker() {
			return moniker;
		}

		public void setMoniker(String value) {
			moniker = value;
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void fle_request_for_field_is_empty_using_bad_field_name_throws() throws Exception {
		Foo foo = new Foo();

		foo.isFieldNull("bar");
	}

	@Test
	public void fle_nondeserialized_manually_populated_resolves_call_to_is_field_null() throws Exception {
		Foo foo = new Foo();
		foo.setName("test");

		// instance has no field shadows...
		assertNull(foo.getShadow().getFields());

		boolean isEmpty = foo.isFieldNull("name");

		//... and isFieldNull resolves using the regular set field state
		assertFalse(isEmpty);
	}

	@Test
	public void fle_nondeserialized_unpopulated_resolves_call_to_is_field_null() throws Exception {
		Foo foo = new Foo();

		// instance has no field shadows...
		assertNull(foo.getShadow().getFields());

		boolean isEmpty = foo.isFieldNull("name");

		//... and isFieldNull resolves using the regular unset field state
		assertTrue(isEmpty);
	}

	@Test
	public void fle_nondeserialized_superclass_protected_field_resolves_call_to_is_field_null() throws Exception {
		// Assuming an instance unencumbered by SecurityManager reflection limits...
		Foo foo = new Foo();

		// NB: timestamp field is populated by default
		assertNotNull(foo.getTs());
		// ... and version is a primitive with an expected default value
		assertEquals(0, foo.getVersion());

		// instance has no field shadows...
		assertNull(foo.getShadow().getFields());

		boolean isTsEmpty = foo.isFieldNull("ts");
		boolean isVersionEmpty = foo.isFieldNull("version");

		// ... but the timestamp and version fields value are accounted-for
		assertFalse(isTsEmpty);
		assertFalse(isVersionEmpty);
	}

	@Test
	public void fle_nondeserialized_super_and_supersuper_class_fields_resolve_calls_to_is_field_null() throws Exception {
		// Assuming an instance unencumbered by SecurityManager reflection limits...
		Baz baz = new Baz();
		baz.setName("test");

		// NB: timestamp field is populated by default
		assertNotNull(baz.getTs());
		// ... and version is a primitive with an expected default value
		assertEquals(0, baz.getVersion());

		// instance has no field shadows...
		assertNull(baz.getShadow().getFields());

		boolean isTsEmpty = baz.isFieldNull("ts");
		boolean isVersionEmpty = baz.isFieldNull("version");
		boolean isNameEmpty = baz.isFieldNull("name");
		boolean isMonikerEmpty = baz.isFieldNull("moniker");

		// ... but the timestamp and version fields value are accounted-for
		assertFalse(isTsEmpty);
		assertFalse(isVersionEmpty);
		assertFalse(isNameEmpty);
		assertTrue(isMonikerEmpty);
	}


	@Test
	public void fle_crypto_deserialized_with_encrypted_and_populated_field_resolves_call_to_is_field_null() throws Exception {
		Foo foo = new Foo();
		foo.setName("test");

		AvroInferredSerializer<Foo> serde = new AvroInferredSerializer<>(Foo.class);
		FLESerializer fleSerde = new FLESerializer(getCipher(), serde);

		byte[] serialized = fleSerde.serialize(foo);

		// deserialize with crypto to populate the field corresponding field
		Foo deserialized = (Foo)fleSerde.deserialize(serialized);

		boolean isEmpty = deserialized.isFieldNull("name");

		assertFalse(isEmpty);
	}

	@Test
	public void fle_crypto_deserialized_without_populated_field_resolves_call_to_is_field_null() throws Exception {
		Foo foo = new Foo();
		foo.setName("test");

		AvroInferredSerializer<Foo> serde = new AvroInferredSerializer<>(Foo.class);
		FLESerializer fleSerializer = new FLESerializer(getCipher(), serde);

		byte[] serialized = fleSerializer.serialize(foo);

		FLESerializer fleDeserializer = new FLESerializer(getCipher(), serde);
		fleDeserializer.setFullDecrypt(false); // no decryption during deserialization

		Foo deserialized = (Foo)fleDeserializer.deserialize(serialized);

		boolean isEmpty = deserialized.isFieldNull("name");

		// normal field remains unpopulated
		assertNull(deserialized.getName());
		// ... but the underlying shadow has a value that is discerned
		assertFalse(isEmpty);
	}


	@Test
	public void fle_crypto_deserialized_with_mixed_populated_fields_resolves_calls_to_is_field_null() throws Exception {
		Baz baz = new Baz();
		baz.setMoniker("test");

		AvroInferredSerializer<Baz> serde = new AvroInferredSerializer<>(Baz.class);
		FLESerializer fleSerializer = new FLESerializer(getCipher(), serde);

		byte[] serialized = fleSerializer.serialize(baz);

		FLESerializer fleDeserializer = new FLESerializer(getCipher(), serde);
		fleDeserializer.setFullDecrypt(false); // no decryption during deserialization

		Baz deserialized = (Baz)fleDeserializer.deserialize(serialized);

		boolean isNameEmpty = deserialized.isFieldNull("name");
		boolean isMonikerEmpty = deserialized.isFieldNull("moniker");

		// regular fields remain unpopulated
		assertNull(deserialized.getName());
		assertNull(deserialized.getMoniker());

		// ... but the underlying shadow moniker value is discerned
		assertFalse(isMonikerEmpty);
		// ... and the unset value for name also resolves
		assertTrue(isNameEmpty);
	}


	@Test
	public void fle_crypto_deserialized_with_empty_string_resolves_calls_to_is_field_null() throws Exception {
		Foo foo = new Foo();
		foo.setName("");

		AvroInferredSerializer<Foo> serde = new AvroInferredSerializer<>(Foo.class);
		FLESerializer fleSerializer = new FLESerializer(getCipher(), serde);

		byte[] serialized = fleSerializer.serialize(foo);

		FLESerializer fleDeserializer = new FLESerializer(getCipher(), serde);
		fleDeserializer.setFullDecrypt(false); // no decryption during deserialization

		Foo deserialized = (Foo)fleDeserializer.deserialize(serialized);

		boolean isNameNull = deserialized.isFieldNull("name");

		assertFalse(isNameNull);
	}


	@Test
	public void fle_crypto_deserialized_with_empty_and_non_empty_strings_resolves_calls_to_is_field_null() throws Exception {
		Baz baz = new Baz();
		baz.setName("test");
		baz.setMoniker("");

		AvroInferredSerializer<Baz> serde = new AvroInferredSerializer<>(Baz.class);
		FLESerializer fleSerializer = new FLESerializer(getCipher(), serde);

		byte[] serialized = fleSerializer.serialize(baz);

		FLESerializer fleDeserializer = new FLESerializer(getCipher(), serde);
		fleDeserializer.setFullDecrypt(false); // no decryption during deserialization

		Baz deserialized = (Baz)fleDeserializer.deserialize(serialized);

		boolean isNameNull = deserialized.isFieldNull("name");
		boolean isMonikerNull = deserialized.isFieldNull("moniker");

		// fields with non-empty shadows register as non-null
		assertFalse(isNameNull);
		// ... even though deserialization has nullified the corresponding backing field
		assertTrue(null == deserialized.getName());

		// fields with empty-string shadows still register as non-null; it is a matter of coincidence that
		// the test encryption mechanism encrypts an empty-string to an empty-string
		assertFalse(isMonikerNull);
		// ... and the corresponding backing field being is also nullified
		assertTrue(null == deserialized.getMoniker());


		// Repopulate the fields
		deserialized.setName("testing again");
		deserialized.setMoniker("test");


		// fields with shadows that have their backing fields repopulated remain non-empty
		isNameNull = deserialized.isFieldNull("name");
		assertFalse(isNameNull);

		// fields with empty shadows will evaluate to non-empty if the backing field is populated
		isMonikerNull = deserialized.isFieldNull("moniker");
		assertFalse(isMonikerNull);

		// With a non-decrypted deserialized instance, reset the re-populated field to be empty
		deserialized.setName(null);

		// fields whose backing field is emptied still may resolve as non-empty if their corresponding shadow exists
		isNameNull = deserialized.isFieldNull("name");
		assertFalse(isNameNull);

	}


	@Test
	public void fle_decrypted_records_may_not_mutate_protected_fields_for_further_serde_operations() throws Exception {
		Baz baz = new Baz();
		baz.setName("test");

		AvroInferredSerializer<Baz> serde = new AvroInferredSerializer<>(Baz.class);
		FLESerializer fleSerializer = new FLESerializer(getCipher(), serde); // crypto-enabled serializer

		byte[] serialized = fleSerializer.serialize(baz);

		Baz decryptedDeserialized = (Baz)fleSerializer.deserialize(serialized);
		assertEquals("test", decryptedDeserialized.getName());

		decryptedDeserialized.setName(null); // this change will not remain in effect...

		byte[] reserialized = fleSerializer.serialize(decryptedDeserialized);
		Baz reDeserialized = (Baz)fleSerializer.deserialize((reserialized));

		boolean isNameNull = reDeserialized.isFieldNull("name");
		assertFalse(isNameNull);

		// Previous setting to null does not transfer through to shadow, and the backing field is set
		assertEquals("test", reDeserialized.getName());
	}

	@Test
	public void fle_crypto_deserialized_without_shadow_fields_resolves_calls_to_is_field_null() throws Exception {
		Baz baz = new Baz();
		baz.setName("test");
		baz.setMoniker("");

		AvroInferredSerializer<Baz> serde = new AvroInferredSerializer<>(Baz.class);

		// serialized without fle
		byte[] serialized = serde.serialize(baz);

		FLESerializer fleDeserializer = new FLESerializer(getCipher(), serde);
		fleDeserializer.setFullDecrypt(false); // no decryption during deserialization

		Baz deserialized = (Baz) fleDeserializer.deserialize(serialized);

		boolean isNameNull = deserialized.isFieldNull("name");
		boolean isMonikerNull = deserialized.isFieldNull("moniker");

		assertFalse(isNameNull);
		assertFalse(isMonikerNull);
	}

	private ReflectVersionedCipher getCipher() {
		return new ReflectUtil(new MockKeyBuilder(),new MockVersionCipher(),new MockKeyStore());
	}

}
