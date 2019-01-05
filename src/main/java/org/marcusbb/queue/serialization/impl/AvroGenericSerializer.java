package org.marcusbb.queue.serialization.impl;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;

import java.io.ByteArrayOutputStream;

public class AvroGenericSerializer implements ByteSerializer<GenericRecord> {
	private Schema readerSchema;
	private Schema writerSchema;

	public AvroGenericSerializer(Schema writerSchema) {
		this.writerSchema = writerSchema;
	}

	public AvroGenericSerializer(Schema writerSchema, Schema readerSchema) {
		this.writerSchema = writerSchema;
		this.readerSchema = readerSchema;
	}

	@Override
	public byte[] serialize(GenericRecord record) throws SerializationException {
		try {

			if (writerSchema == null) {
				writerSchema = record.getSchema();
			}

			ByteArrayOutputStream bout = new ByteArrayOutputStream();

			DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(writerSchema);
			BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(bout, null);
			writer.write(record, encoder);
			encoder.flush();
			return bout.toByteArray();
		}
		catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public GenericRecord deserialize(byte[] b) throws SerializationException {
		if (writerSchema == null) {
			throw new IllegalStateException("no writer schema provided");
		}

		GenericDatumReader<GenericRecord> reader;
		if (readerSchema == null) {
			reader = new GenericDatumReader<>(writerSchema);
		}
		else {
			reader = new GenericDatumReader(writerSchema, readerSchema);
		}

		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(b, null);

		try {
			return reader.read(null, decoder);
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}
}
