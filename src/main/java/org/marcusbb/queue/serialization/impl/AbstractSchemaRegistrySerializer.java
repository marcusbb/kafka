package org.marcusbb.queue.serialization.impl;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import joptsimple.internal.Strings;
import org.apache.avro.Schema;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.serialization.AvroSchemaVersion;
import org.marcusbb.queue.serialization.avro.AvroReflectData;

import java.io.IOException;


public abstract class AbstractSchemaRegistrySerializer<T> {

	public static final byte MAGIC_BYTE = 0x0;

	protected static final int SCHEMA_ID_BYTES_LENGTH = 4;
	protected static final int BYTE_PREFIX_LENGTH = 1 + SCHEMA_ID_BYTES_LENGTH; // MAGIC_BYTE + SCHEMA-ID-BYTES

	protected SchemaRegistryClient schemaRegistry;
	protected Class<T> messageType;
	protected String subject;
	protected Integer readerSchemaVersion;
	protected AvroReflectData reflectData;

	public AbstractSchemaRegistrySerializer(SchemaRegistryClient schemaRegistry, Class<T> messageType, String subject) {
		this.schemaRegistry = schemaRegistry;
		this.messageType = messageType;
		this.subject = subject;
		reflectData = new AvroReflectData();
	}

	/**
	 * Serializer determines readerSchema based on
	 * 1. whether a reader-schema-version is specified,
	 * 2. ...whether a message type is provided,
	 * 3. determine messagetype from the writerschema
	 * 4. otherwise, returns null ; invoker can deserialize without a readerSchema
	 *
	 * @return reader Schema or null
	 * @throws IOException
	 * @throws RestClientException
	 * @param writerSchema
	 */
	protected Schema resolveReaderSchema(Schema writerSchema) throws IOException, RestClientException, ClassNotFoundException {
		Schema readerSchema = null;
		if (readerSchemaVersion != null) {
			SchemaMetadata metadata = schemaRegistry.getSchemaMetadata(subject, readerSchemaVersion);
			readerSchema = schemaRegistry.getBySubjectAndID(subject, metadata.getId());
		}
		else if (messageType != null) {
			readerSchema = getSchemaFromType(messageType);
		}
		else if (writerSchema != null){
			String filler = "";
			if (!writerSchema.getNamespace().endsWith("$"))
				filler = ".";
			Class<T> messageType = (Class<T>) Class.forName(writerSchema.getNamespace()+filler +writerSchema.getName());
			readerSchema = getSchemaFromType(messageType);
		}
		return readerSchema;
	}

	private Schema getSchemaFromType(Class<T> messageType) throws IOException, RestClientException {
		if (messageType.isAnnotationPresent(AvroSchemaVersion.class)) {
			int schemaVersion = messageType.getAnnotation(AvroSchemaVersion.class).value();
			String annotatedSubject = messageType.getAnnotation(AvroSchemaVersion.class).subject();
			SchemaMetadata metadata = schemaRegistry.getSchemaMetadata(annotatedSubject, schemaVersion);
			return schemaRegistry.getBySubjectAndID(annotatedSubject, metadata.getId());
		}
		else if (messageType != null) {
			return reflectData.getSchema(messageType);
		}
		else {
			throw new IllegalStateException("Either the writerschema-inferred message type has no AvroSchemaVersion annotation or no message type was explicitly provided to the serializer.");
		}
	}


	/**
	 * Schema subject is preferably retrieved from the AvroSchemaAnnotation, or passed to the serializer
	 * of a specified message type.  Such explicit determination helps with unambiguous schema retrieval from registry.
	 * NB: passing-in the subject to the serializer ties it to the subject and is unsuitable for use cases
	 *     where multiple message types may have to be handled.
	 *
	 * If subject is not provided, either the messageType class provided to the serializer or the class of the
	 * data provided for serialization are used to derive the writer-schema.  A special case is provided for in the
	 * case of RoutableEncryptedMessage types, which act only as message envelopes and may be registered with their
	 * canonical name.
	 *
	 * In all other cases, an illegal state exception is thrown to indicate that the serializer does not have
	 * sufficient information to unambiguously determine writerSchema in the context of the supplied schema registry.
	 *
	 * @param serializableDataType
	 * @return SchemaMetadata corresponding to the writerSchema
	 * @throws IOException
	 * @throws RestClientException
	 */
	protected SchemaMetadata resolveWriterSchemaInfo(Class<?> serializableDataType) throws IOException, RestClientException {
		if (serializableDataType.isAnnotationPresent(AvroSchemaVersion.class)) {
			int annotatedVersion = serializableDataType.getAnnotation(AvroSchemaVersion.class).value();
			String annotatedSubject = serializableDataType.getAnnotation(AvroSchemaVersion.class).subject();
			return schemaRegistry.getSchemaMetadata(annotatedSubject, annotatedVersion);
		}
		else if (subject != null) {
			Schema writerSchema = reflectData.getSchema(serializableDataType);
			schemaRegistry.register(subject, writerSchema);
			int writerSchemaVersion = schemaRegistry.getVersion(subject, writerSchema);
			return schemaRegistry.getSchemaMetadata(subject, writerSchemaVersion);
		}
		else if (serializableDataType.isAssignableFrom(RoutableEncryptedMessage.class) ) {
			Schema writerSchema = reflectData.getSchema(serializableDataType);
			int envelopeSchemaId = schemaRegistry.register(serializableDataType.getCanonicalName(), writerSchema);
			return new SchemaMetadata(envelopeSchemaId,0, writerSchema.toString());
		}
		else {
			throw new IllegalStateException("Cannot serialize without subject");
		}
	}

}
