package org.marcusbb.queue.serialization.impl.ems;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.marcusbb.crypto.reflect.ReflectVersionedCipher;
import org.marcusbb.queue.AbstractREM;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.impl.AbstractSchemaRegistrySerializer;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

/**
 *
 * Field level encryption serialization + deserialization with contract to {@link AbstractREM}.
 *
 * It implements Confluent's SerDe interfaces and is compatible with it's schema registry serialization.
 *
 * At serialization time, it registers/gets to Confluent's SchemaRegistry internally by interpreting the {@link Schema} from
 * the object's class, determined at runtime.
 * Header information is then written by way of {{@link #MAGIC_BYTE} + verion(int).
 *
 * At deserialization time the schema is interpreted by the header information and retrieved from schema.
 *
 * The only significant difference between this serializer and  {@link FLESerializer} is that
 * this implements the Confluents scheme for byte output, wrapping the FLESerializer for doing
 * the work of encrypt and decrypt.
 *
 *
 */
public class FLERegistrySerDe<T extends AbstractREM> extends AbstractSchemaRegistrySerializer<T> implements ByteSerializer<T>, Serializer<T>,Deserializer<T>{

	protected ReflectVersionedCipher cipher;

	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry) throws IOException, RestClientException {
		super(schemaRegistry,null,null);
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, ReflectVersionedCipher cipher) throws IOException, RestClientException {
		this(schemaRegistry, null, null,cipher);
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, String subject) throws IOException, RestClientException {
		super(schemaRegistry, null, subject);
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, Class<T> messageType) throws IOException, RestClientException {
		super(schemaRegistry, messageType, null);
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, Class<T> messageType, String subject) throws IOException, RestClientException {
		super(schemaRegistry, messageType, subject);
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, String subject, int readerSchemaVersion) throws IOException, RestClientException {
		this(schemaRegistry, null, subject);
		this.readerSchemaVersion = readerSchemaVersion;
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, String subject, ReflectVersionedCipher cipher) throws IOException, RestClientException {
		this(schemaRegistry, null, subject);
		this.cipher = cipher;
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, Class<T> messageType, String subject, ReflectVersionedCipher cipher) throws IOException, RestClientException {
		this(schemaRegistry, messageType, subject);
		this.cipher = cipher;
	}
	public FLERegistrySerDe(SchemaRegistryClient schemaRegistry, String subject, ReflectVersionedCipher cipher, int readerSchemaVersion) throws IOException, RestClientException {
		this(schemaRegistry, null, subject, cipher);
		this.readerSchemaVersion = readerSchemaVersion;
	}

	@Override
	public void configure(Map<String, ?> configs, boolean isKey) {
		// Is there anything to configure?
	}

	@Override
	public void close() {
		// close schemaRegistry
	}

	@Override
	public byte[] serialize(String subject, T data) {

		try {
			SchemaMetadata metadata = resolveWriterSchemaInfo(data.getClass());

			int writerSchemaId = metadata.getId();
			Schema writerSchema = schemaRegistry.getByID(writerSchemaId);

			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ByteBuffer verB = ByteBuffer.allocate(SCHEMA_ID_BYTES_LENGTH).putInt(writerSchemaId);
			bos.write(MAGIC_BYTE);
			bos.write(verB.array());

			AvroInferredSerializer<T> avroSerializer = new AvroInferredSerializer<>(writerSchema);

			ByteSerializer<T> serializer = cipher != null ?
					(ByteSerializer<T>)(new FLESerializer(cipher, avroSerializer)) :
					avroSerializer;

			bos.write(serializer.serialize(data));

			return bos.toByteArray();
		} catch (IOException | RestClientException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(String subject, byte[] data) {
		try {
			ByteBuffer bb = ByteBuffer.wrap(data);
			if (bb.get() != MAGIC_BYTE) {
				throw new SerializationException(new IllegalArgumentException("No magic byte provided"));
			}

			int writerSchemaId = bb.getInt();

			Schema writerSchema = schemaRegistry.getByID(writerSchemaId);
			Schema readerSchema = resolveReaderSchema(writerSchema);

			int length = bb.limit() - BYTE_PREFIX_LENGTH;
			byte[] payloadBytes = new byte[length];
			bb.get(payloadBytes, 0, length);

			AvroInferredSerializer<T> avroSerializer = readerSchema != null ?
					new AvroInferredSerializer<T>(writerSchema, readerSchema) :
					new AvroInferredSerializer<T>(writerSchema);

			ByteSerializer<?> serializer = cipher != null ?
					new FLESerializer(cipher, avroSerializer) :
					avroSerializer;

			return (T)serializer.deserialize(payloadBytes);
		}
		catch (IOException | RestClientException | ClassNotFoundException e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public byte[] serialize(T obj) {
		return serialize(subject,obj);
	}

	@Override
	public T deserialize(byte[] b) {
		return deserialize(subject,b);
	}

}
