package org.marcusbb.queue.serialization.impl;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 *
 * Serializer that requires Avro schema passed in.
 *
 * @param <T>
 */
public class AvroSerializer<T extends SpecificRecord> implements ByteSerializer<T> {

	private Schema schema;

	public AvroSerializer(Schema schema) {
		this.schema = schema;
	}

	@Override
	public byte[] serialize(T obj) {
		try {
			SpecificDatumWriter<T> writer = new SpecificDatumWriter<T>(schema);

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bout, null);
			writer.write(obj, encoder);
			encoder.flush();

			return bout.toByteArray();

		} catch (Exception e) {
			throw new SerializationException(e);
		}

	}

	@Override
	public T deserialize(byte[] b) {
		DatumReader<T> reader = new SpecificDatumReader<T>(schema);

		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(b, null);

		try {
			return reader.read(null, decoder);
		} catch (Exception e) {
			throw new SerializationException(e);
		}

	}
}
