package org.marcusbb.queue.serialization.impl.ems;

import org.marcusbb.crypto.VersionedCipher;
import org.marcusbb.crypto.VersionedKeyBuilder;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.serialization.impl.AvroInferredSerializer;
import org.marcusbb.queue.serialization.impl.KryoMessageSerializer;
import org.marcusbb.queue.serialization.impl.ems.security.MessageAuthenticator;

/**
 *
 * A special case of {@link EncryptedMessageSerializer}
 * with {@link AvroInferredSerializer} serializer for envelope and {@link KryoMessageSerializer} for payload.
 *
 */
public class AvroEMS<T> extends EncryptedMessageSerializer<T> {

	public AvroEMS(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias) {

		super(cipher, keyBuilder, iv, alias,
				new KryoMessageSerializer<T>(),
				new AvroInferredSerializer<RoutableEncryptedMessage<T>>(RoutableEncryptedMessage.class));
	}

	public AvroEMS(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias, MessageAuthenticator messageAuthenticator) {

		super(cipher, keyBuilder, iv, alias,
				new KryoMessageSerializer<T>(),
				new AvroInferredSerializer<RoutableEncryptedMessage<T>>(RoutableEncryptedMessage.class), messageAuthenticator);
	}
}