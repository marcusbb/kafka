package org.marcusbb.queue.serialization.impl.ems;

import org.marcusbb.crypto.VersionedCipher;
import org.marcusbb.crypto.VersionedKey;
import org.marcusbb.crypto.VersionedKeyBuilder;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;
import org.marcusbb.queue.serialization.impl.KryoMessageSerializer;
import org.marcusbb.queue.serialization.impl.ems.security.MessageAuthenticator;

/**
 * This is a 2-layered encrypted envelope serializer designed for serializing/deserializing
 * payloads wrapped within an envelope {@link org.marcusbb.queue.RoutableEncryptedMessage}.
 * Provides flexibility to chose different implementations of ByteSerializer
 * for payload and envelope.
 *
 * In the first phase, payload is serialized and then encrypted before enclosing them
 * into {@link RoutableEncryptedMessage}. And in the second phase of serialization, the
 * envelope is serialized by specified envelope serializer.
 *
 * By default is thread safe: as per {@link KryoMessageSerializer}.
 * NOT thread safe if underlying serializers: envelope and payload are not thread safe.
 * 
 */
public abstract class EncryptedMessageSerializer<T> implements ByteSerializer<RoutableEncryptedMessage<T>> {

	protected VersionedCipher cipher;
	protected VersionedKeyBuilder keyBuilder;
	protected VersionedKey versionedKey;
	protected String iv;
	protected String alias;
	protected ByteSerializer<T> payloadSerializer;
	protected ByteSerializer<RoutableEncryptedMessage<T>> envelopeSerializer;
	protected MessageAuthenticator messageAuthenticator;

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv,String alias) {
		this.cipher = cipher;
		this.keyBuilder = keyBuilder;
		this.iv = iv;
		this.alias = alias;
		this.payloadSerializer = new KryoMessageSerializer<>();
		this.envelopeSerializer = new KryoMessageSerializer<>();
		this.messageAuthenticator = new MessageAuthenticator();
		versionedKey = keyBuilder.buildKey(alias, iv);
	}

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder, String iv, String alias,
									  MessageAuthenticator messageAuthenticator) {
		this(cipher,keyBuilder,iv,alias);
		this.messageAuthenticator = messageAuthenticator;
	}

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder,
									  String iv, String alias,
									  ByteSerializer<T> payloadSerializer) {
		this(cipher,keyBuilder,iv,alias);
		this.payloadSerializer = payloadSerializer;
	}

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder,
									  String iv, String alias,
									  ByteSerializer<T> payloadSerializer,
									  MessageAuthenticator messageAuthenticator) {
		this(cipher,keyBuilder,iv,alias,payloadSerializer);
		this.messageAuthenticator = messageAuthenticator;
	}

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder,
									  String iv, String alias,
									  ByteSerializer<T> payloadSerializer,
									  ByteSerializer<RoutableEncryptedMessage<T>> envelopeSerializer) {
		this(cipher,keyBuilder,iv,alias, payloadSerializer);
		this.envelopeSerializer = envelopeSerializer;
	}

	public EncryptedMessageSerializer(VersionedCipher cipher, VersionedKeyBuilder keyBuilder,
									  String iv, String alias,
									  ByteSerializer<T> payloadSerializer,
									  ByteSerializer<RoutableEncryptedMessage<T>> envelopeSerializer,
									  MessageAuthenticator messageAuthenticator) {
		this(cipher,keyBuilder,iv,alias,payloadSerializer,envelopeSerializer);
		this.messageAuthenticator = messageAuthenticator;
	}

	@Override
	public byte[] serialize(RoutableEncryptedMessage<T> msg) {
		try {

			RoutableEncryptedMessage<T> nmsg = encryptMessageIfNeeded(msg);

			return envelopeSerializer.serialize(nmsg);
		}catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	private RoutableEncryptedMessage<T> encryptMessageIfNeeded(RoutableEncryptedMessage<T> msg) {
		byte[] encPayload = msg.getEncPayload();

		boolean isEncryptionNeeded = (encPayload == null && msg.getPayload() != null);

		if (!isEncryptionNeeded) {
			return msg;
		}

		byte [] pbytes = payloadSerializer.serialize(msg.getPayload());
		encPayload = cipher.encrypt(versionedKey, pbytes);

		byte[] signature = messageAuthenticator.sign(pbytes);
		return new RoutableEncryptedMessage<>(msg, encPayload, signature);
	}

	@Override
	public RoutableEncryptedMessage<T> deserialize(byte[] b) {
		try {
			RoutableEncryptedMessage<T> msg = envelopeSerializer.deserialize(b);

			byte[] decryptedPayload = cipher.decrypt(versionedKey, msg.getEncPayload());

			messageAuthenticator.verify(decryptedPayload, msg.getSignature());

			T payload = payloadSerializer.deserialize(decryptedPayload);

			RoutableEncryptedMessage<T> nmsg = new RoutableEncryptedMessage<>(msg, payload);

			return nmsg;

		}catch (Exception e){
			throw new SerializationException(e);
		}
	}

}
