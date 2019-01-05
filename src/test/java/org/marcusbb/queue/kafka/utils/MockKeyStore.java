package org.marcusbb.queue.kafka.utils;

import java.io.IOException;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.Mac;
import javax.crypto.SecretKey;

import org.marcusbb.crypto.KeyStoreManager;

public class MockKeyStore implements KeyStoreManager{

	@Override
	public byte[] getIvParameter(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Mac getMac(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PrivateKey getPrivateKey(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PublicKey getPublicKey(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public SecretKey getSecretKey(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void load() throws IOException, KeyStoreException {
		// TODO Auto-generated method stub
		
	}

}
