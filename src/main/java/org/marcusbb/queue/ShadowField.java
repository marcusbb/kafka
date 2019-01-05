package org.marcusbb.queue;

import java.io.Serializable;

import org.marcusbb.crypto.reflect.CipherCloneable;

public class ShadowField extends CipherCloneable.DefaultCloneable implements Serializable {

	private static final long serialVersionUID = 1L;
	
	private String name;
	
	private String algorithm;
	
	private String base64Encoded;
	
	private String keyAlias;
	
	private String ivAlias;
	
	private boolean isTokenized = false;
	
	public ShadowField() {}
	public ShadowField(String name, String algorithm,String base64Encoded, String keyAlias, String ivAlias,boolean isTokenized) {
		this.name = name;
		this.algorithm = algorithm;
		this.base64Encoded = base64Encoded;
		this.keyAlias = keyAlias;
		this.ivAlias = ivAlias;
		this.isTokenized = isTokenized;
	}

	
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(String algorithm) {
		this.algorithm = algorithm;
	}

	public String getBase64Encoded() {
		return base64Encoded;
	}

	public void setBase64Encoded(String base64Encoded) {
		this.base64Encoded = base64Encoded;
	}

	public String getKeyAlias() {
		return keyAlias;
	}

	public void setKeyAlias(String keyAlias) {
		this.keyAlias = keyAlias;
	}

	public String getIvAlias() {
		return ivAlias;
	}

	public void setIvAlias(String ivAlias) {
		this.ivAlias = ivAlias;
	}
	public boolean isTokenized() {
		return isTokenized;
	}
	public void setTokenized(boolean isTokenized) {
		this.isTokenized = isTokenized;
	}

	
	
	
}