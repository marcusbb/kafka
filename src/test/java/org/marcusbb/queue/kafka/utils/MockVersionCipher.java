package org.marcusbb.queue.kafka.utils;

import org.apache.commons.codec.binary.Base64;
import org.marcusbb.crypto.VersionedCipher;
import org.marcusbb.crypto.VersionedKey;

public class MockVersionCipher implements VersionedCipher {

	@Override
	public byte[] encrypt(VersionedKey version, byte[] payload) {
		return Base64.encodeBase64(payload);
	}

	@Override
	public byte[] decrypt(VersionedKey version, byte[] payload) {
		return Base64.decodeBase64(payload);
	}
	
}
