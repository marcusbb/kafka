package org.marcusbb.queue.serialization.impl.ems;

import org.marcusbb.crypto.VersionedCipher;
import org.marcusbb.crypto.VersionedKeyBuilder;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.serialization.impl.SchemaRegistrySerializer;
import org.marcusbb.queue.serialization.impl.ems.security.MessageAuthenticator;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

/**
 *
 * A special case of {@link EncryptedMessageSerializer}
 * with {@link SchemaRegistrySerializer} serializer for payload and envelope
 *
 */

public class SchemaRegistryEMS<T> extends EncryptedMessageSerializer<T> {

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<T>(schemaRegistryClient),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient));
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient, Class<T> messageType,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<>(schemaRegistryClient, messageType),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient));
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient, Class<T> messageType,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<>(schemaRegistryClient, messageType, subject),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient));
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<T>(schemaRegistryClient, subject),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient));
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject, int readerSchemaVersion) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<T>(schemaRegistryClient, subject, readerSchemaVersion),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient));
    }

    public SchemaRegistryEMS(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             SchemaRegistrySerializer<T> payloadSerializer,
                             SchemaRegistrySerializer<RoutableEncryptedMessage<T>> envelopeSerializer) {
        super(cipher, keyBuilder, iv, alias, payloadSerializer,envelopeSerializer);
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient, Class<T> messageType,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             MessageAuthenticator messageAuthenticator) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<>(schemaRegistryClient, messageType),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient), messageAuthenticator);
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient, Class<T> messageType,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject,
                             MessageAuthenticator messageAuthenticator) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<>(schemaRegistryClient, messageType, subject),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient), messageAuthenticator);
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject,
                             MessageAuthenticator messageAuthenticator) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<T>(schemaRegistryClient, subject),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient), messageAuthenticator);
    }

    public SchemaRegistryEMS(SchemaRegistryClient schemaRegistryClient,
                             VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             String subject,
                             int readerSchemaVersion,
                             MessageAuthenticator messageAuthenticator) {
        super(cipher, keyBuilder, iv, alias,
                new SchemaRegistrySerializer<T>(schemaRegistryClient, subject, readerSchemaVersion),
                new SchemaRegistrySerializer<RoutableEncryptedMessage<T>>(schemaRegistryClient), messageAuthenticator);
    }

    public SchemaRegistryEMS(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
                             SchemaRegistrySerializer<T> payloadSerializer,
                             SchemaRegistrySerializer<RoutableEncryptedMessage<T>> envelopeSerializer,
                             MessageAuthenticator messageAuthenticator) {
        super(cipher, keyBuilder, iv, alias, payloadSerializer,envelopeSerializer, messageAuthenticator);
    }

}


