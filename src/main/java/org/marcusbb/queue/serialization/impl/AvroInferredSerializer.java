package org.marcusbb.queue.serialization.impl;

import org.apache.avro.Schema;
import org.apache.avro.io.*;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.avro.AvroReflectData;

import java.io.ByteArrayOutputStream;

/**
 *
 *
 * A serializer that infers the Avro writerSchema by Avro reflection.
 * This is not as generic as Java bean serialization and may have breaking impact on serialization.
 *
 *
 * @param <T>
 */
public class AvroInferredSerializer<T> implements ByteSerializer<T> {

	private Class<T> cl;
	private Schema writerSchema;
	private Schema readerSchema;
	private AvroReflectData reflectData;

	private AvroInferredSerializer() { }

	public AvroInferredSerializer(Class cl) {
		this.cl = cl;
		reflectData = new AvroReflectData();
		writerSchema = reflectData.getSchema(cl);
	}

	public AvroInferredSerializer(Schema writerSchema) {
		this(writerSchema, null);
	}

	public AvroInferredSerializer(Schema writerSchema, Schema readerSchema) {
		reflectData = new AvroReflectData();


		if (writerSchema == null) {
			writerSchema = reflectData.getSchema(cl);
		}

		this.writerSchema = writerSchema;
		this.readerSchema = readerSchema;
	}

	@Override
	public byte[] serialize(T obj) {

		try {
			if (writerSchema == null) {
				writerSchema = reflectData.getSchema(obj.getClass());
			}

			ByteArrayOutputStream bout = new ByteArrayOutputStream();
			DatumWriter<T> writer = reflectData.createDatumWriter(writerSchema);
			BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bout, null);
			writer.write(obj, encoder);
			encoder.flush();
			return bout.toByteArray();
		}
		catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(byte[] b) {

		if (cl == null && writerSchema == null)
			throw new IllegalStateException("Deserializer must know class and initialized with it");

		if (writerSchema == null) {
			writerSchema = reflectData.getSchema(cl);
		}

		return deserialize(b, writerSchema);
	}

	public T deserialize(byte[] b, Schema writerSchema) {
		DatumReader<T> reader;
		if (readerSchema == null) {
			reader = reflectData.createDatumReader(writerSchema);
		}
		else {
			reader = reflectData.createDatumReader(writerSchema, readerSchema);
		}

		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(b, null);

		try {
			return reader.read(null, decoder);
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	public Schema getReaderSchema() { return readerSchema; }
	public Schema getWriterSchema() {
		return writerSchema;
	}

}
