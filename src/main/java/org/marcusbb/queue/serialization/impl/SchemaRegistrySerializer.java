package org.marcusbb.queue.serialization.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.apache.avro.Schema;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 * uses schema registry to provide backward and forward compatibility support
 * for evolving avro schemas.
 */
public class SchemaRegistrySerializer<T> extends AbstractSchemaRegistrySerializer<T> implements ByteSerializer<T> {

	public SchemaRegistrySerializer(SchemaRegistryClient schemaRegistry) {
		super(schemaRegistry, null, null);
	}

	public SchemaRegistrySerializer(SchemaRegistryClient schemaRegistry, Class<T> messageType) {
		super(schemaRegistry, messageType, null);
	}

	public SchemaRegistrySerializer(SchemaRegistryClient schemaRegistry, String subject) {
		super(schemaRegistry,null, subject);
	}

	public SchemaRegistrySerializer(SchemaRegistryClient schemaRegistry, Class<T> messageType, String subject) {
		super(schemaRegistry, messageType, subject);
	}

	public SchemaRegistrySerializer(SchemaRegistryClient schemaRegistry, String subject, int readerSchemaVersion) {
		super(schemaRegistry, null, subject);
		this.readerSchemaVersion = readerSchemaVersion;
	}

	@Override
	public byte[] serialize(T data) {
		try {
			SchemaMetadata metadata = resolveWriterSchemaInfo(data.getClass());

			Schema writerSchema = new Schema.Parser().parse(metadata.getSchema());
			int writerSchemaId = metadata.getId();

			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ByteBuffer idB = ByteBuffer.allocate(SCHEMA_ID_BYTES_LENGTH).putInt(writerSchemaId);
			out.write(MAGIC_BYTE);
			out.write(idB.array());

			AvroInferredSerializer<T> serializer = new AvroInferredSerializer<>(writerSchema);

			out.write(serializer.serialize(data));

			return out.toByteArray();

		} catch (IOException | RestClientException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(byte[] bytes) {
		try {
			ByteBuffer buffer = ByteBuffer.wrap(bytes);
			if(buffer.get() != MAGIC_BYTE) {
				throw new SerializationException(new IllegalArgumentException("No magic byte provided"));
			}

			int writerSchemaId = buffer.getInt();

			Schema writerSchema = schemaRegistry.getByID(writerSchemaId);
			Schema readerSchema = resolveReaderSchema(writerSchema);

			int length = buffer.limit() - BYTE_PREFIX_LENGTH;
			byte[] payloadBytes = new byte[length];
			buffer.get(payloadBytes, 0, length);

			AvroInferredSerializer<T> serializer = readerSchema != null ?
					new AvroInferredSerializer<T>(writerSchema, readerSchema) :
					new AvroInferredSerializer<T>(writerSchema);

			return serializer.deserialize(payloadBytes);

		} catch (IOException | RestClientException | ClassNotFoundException e) {
			throw new SerializationException(e);
		}
	}

}
