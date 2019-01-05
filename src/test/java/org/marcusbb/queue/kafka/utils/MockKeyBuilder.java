package org.marcusbb.queue.kafka.utils;

import org.marcusbb.crypto.VersionedKey;
import org.marcusbb.crypto.VersionedKeyBuilder;
import org.marcusbb.crypto.exception.UnknownKeyVersion;

public class MockKeyBuilder implements VersionedKeyBuilder {

	@Override
	public VersionedKey buildKey(String name, String ivSpecName) throws UnknownKeyVersion {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VersionedKey buildKey(String name, byte[] iv) throws UnknownKeyVersion {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VersionedKey buildKey(String keyAlias, String ivSpecName, byte[] versionedCipherText)
			throws UnknownKeyVersion {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VersionedKey buildKey(String keyAlias, byte[] iv, byte[] versionedCipherText) throws UnknownKeyVersion {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getVersion() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public VersionedKey buildPrivateKey(String keyAlias, byte[] versionedCipherText) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public VersionedKey buildPublicKey(String keyAlias) {
		// TODO Auto-generated method stub
		return null;
	}
	

}
