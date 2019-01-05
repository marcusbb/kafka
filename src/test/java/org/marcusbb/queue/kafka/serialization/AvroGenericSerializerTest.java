package org.marcusbb.queue.kafka.serialization;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.marcusbb.queue.kafka.utils.TestGenericMessage;
import org.marcusbb.queue.serialization.impl.AvroGenericSerializer;


public class AvroGenericSerializerTest {

	@Test
	public void canSerializeDeserializeGenerically() {
		AvroGenericSerializer serializer =
				new AvroGenericSerializer(TestGenericMessage.getSchema());

		TestGenericMessage orig = new TestGenericMessage("hello_world", "test");

		byte[] serialized = serializer.serialize(orig.toGeneric());

		TestGenericMessage output = TestGenericMessage.fromGeneric(serializer.deserialize(serialized));

		assertEquals(orig, output);
	}


	@Test
	public void cannotDeserializeGenericRecordUsingSchemaWithManuallyReorderedFields() {

		// For test schema...
		Schema origSchema = TestGenericMessage.getSchema();
		System.out.println(String.format("original schema: %s", origSchema.toString()));

		// ... provide a manually altered schema that exchanges the order of the two fields
		String shuffled = "{\"type\":\"record\",\"name\":\"TestGenericMessage\",\"namespace\":\"org.marcusbb.queue.kafka.utils\",\"fields\":[{\"name\":\"supplement\",\"type\":\"string\"},{\"name\":\"content\",\"type\":\"string\"}]}";
		Schema shuffledSchema = new Schema.Parser().parse(shuffled);
		System.out.println(String.format("shuffled schema: %s", shuffledSchema.toString()));

		assertNotEquals(origSchema.toString(), shuffledSchema.toString());

		// With test record based on the original schema...
		TestGenericMessage origMessage = new TestGenericMessage("hello_world", "test");
		GenericRecord origRecord = origMessage.toGeneric();
		System.out.println(String.format("original generic-record: %s", origRecord));

		// ...serialize with original schema...
		AvroGenericSerializer origSerializer = new AvroGenericSerializer(origSchema);
		byte[] serialized = origSerializer.serialize(origRecord);

		// ...deserialize with shuffled schema...
		AvroGenericSerializer shuffledSerializer = new AvroGenericSerializer(shuffledSchema);
		GenericRecord deserializedRecord = shuffledSerializer.deserialize(serialized);
		System.out.println(String.format("deserialized generic-record: %s", deserializedRecord));

		// Field values are mismatched!
		assertNotEquals(origRecord.get("content").toString(), deserializedRecord.get("content").toString());
		assertNotEquals(origRecord.get("supplement").toString(), deserializedRecord.get("supplement").toString());
	}


	@Test
	public void cannotDeserializeGenericRecordUsingSchemaWithApiReorderedFields() {

		// For test schema...
		Schema origSchema = TestGenericMessage.getSchema();
		System.out.println(String.format("original schema: %s", origSchema.toString()));

		// ...provide a 'shuffled' schema by constructing one with fields applied in different order from original
		Schema shuffledSchema = SchemaBuilder
				.record("TestGenericMessage")
				.namespace("org.marcusbb.queue.kafka.utils")
				.fields()
					.name("supplement").type().stringType().noDefault()
					.name("content").type().stringType().noDefault()
				.endRecord();
		System.out.println(String.format("shuffled schema: %s", shuffledSchema.toString()));

		assertNotEquals(origSchema.toString(), shuffledSchema.toString());

		// With test record based on the original schema...
		TestGenericMessage origMessage = new TestGenericMessage("hello_world", "test");
		GenericRecord origRecord = origMessage.toGeneric();
		System.out.println(String.format("original generic-record: %s", origRecord));

		// ...serialize with original schema...
		AvroGenericSerializer origSerializer = new AvroGenericSerializer(origSchema);
		byte[] serialized = origSerializer.serialize(origRecord);

		// ...deserialize with shuffled schema...
		AvroGenericSerializer shuffledSerializer = new AvroGenericSerializer(shuffledSchema);
		GenericRecord deserializedRecord = shuffledSerializer.deserialize(serialized);
		System.out.println(String.format("deserialized generic-record: %s", deserializedRecord));

		// Field values are mismatched!
		assertNotEquals(origRecord.get("content").toString(), deserializedRecord.get("content").toString());
		assertNotEquals(origRecord.get("supplement").toString(), deserializedRecord.get("supplement").toString());
	}


	@Test
	public void canDeserializeGenericRecordUsingSchemaWithReorderedFieldsByApplyingReaderSchema() {

		// For test schema...
		Schema origSchema = TestGenericMessage.getSchema();
		System.out.println(String.format("original schema: %s", origSchema.toString()));

		// ...provide a 'shuffled' schema by constructing one with fields applied in different order from original
		Schema shuffledSchema = SchemaBuilder
				.record("TestGenericMessage")
				.namespace("org.marcusbb.queue.kafka.utils")
				.fields()
				.name("supplement").type().stringType().noDefault()
				.name("content").type().stringType().noDefault()
				.endRecord();
		System.out.println(String.format("shuffled schema: %s", shuffledSchema.toString()));

		assertNotEquals(origSchema.toString(), shuffledSchema.toString());

		// With test record based on the original schema...
		TestGenericMessage origMessage = new TestGenericMessage("hello_world", "test");
		GenericRecord origRecord = origMessage.toGeneric();
		System.out.println(String.format("original generic-record: %s", origRecord));

		// ...serialize with original schema...
		AvroGenericSerializer origSerializer = new AvroGenericSerializer(origSchema);
		byte[] serialized = origSerializer.serialize(origRecord);

		// ...deserialize specifying original writer schema and shuffled reader schema
		AvroGenericSerializer shuffledSerializer = new AvroGenericSerializer(origSchema, shuffledSchema);
		GenericRecord deserializedRecord = shuffledSerializer.deserialize(serialized);
		System.out.println(String.format("deserialized generic-record: %s", deserializedRecord));

		// Field values line up correctly.
		assertEquals(origRecord.get("content").toString(), deserializedRecord.get("content").toString());
		assertEquals(origRecord.get("supplement").toString(), deserializedRecord.get("supplement").toString());
	}

}
