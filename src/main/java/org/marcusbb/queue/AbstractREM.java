package org.marcusbb.queue;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang.reflect.FieldUtils;
import org.marcusbb.crypto.reflect.CipherCloneable;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;


public abstract class AbstractREM extends CipherCloneable.DefaultCloneable implements Serializable {


	private static final long serialVersionUID = 1L;

	//corresponds to the schema registry version
	@AvroName(value="version")
	int version = 0;

	@AvroName(value="headers") @Nullable
	Map<String,String> headers;

	//internal use only
	@AvroName(value="shadow") @Nullable
	Shadow shadow = new Shadow();

	@AvroName(value="ts")
	long ts = System.nanoTime();

	//this is simply for serializers
	public AbstractREM() {
	}
	
	public AbstractREM(Map<String,String> headers) {
		if (headers==null)
			throw  new IllegalArgumentException("Invalid null headers");
		this.headers = headers;
		

	}

	// Ensure that de-serialized entities have an initialized headers field
	private void conditionallyInitHeaders() {
		if (headers == null) {
			headers = new HashMap<>();
		}
	}

	public Map<String, String> getHeaders() {
		conditionallyInitHeaders();
		return headers;
	}


	public String getHeader(String key) {
		conditionallyInitHeaders();
		return headers.get(key);
	}

	public void addHeader(String key, String value) {
		conditionallyInitHeaders();
		headers.put(key, value);
	}

	public void addHeaders(Map<String,String> addHeaders){
		conditionallyInitHeaders();
		headers.putAll(addHeaders);
	}

	public long getTs() {
		return ts;
	}

	public Shadow getShadow() {
		return shadow;
	}

	public void setShadow(Shadow shadow) {
		this.shadow = shadow;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public int getVersion() {
		return version;
	}

	public String getExceptionMessage(){
		return getHeader(AbstractConsumer.EXCEPTION_MSG_HEADER);
	}

	public String getExceptionStackTrace(){
		return getHeader(AbstractConsumer.EXCEPTION_STACK_HEADER);
	}

	public int getRetryDeliveryCount(){
		String count = getHeader(AbstractConsumer.DELIVERY_COUNT_HEADER);
		return Integer.parseInt( count != null ? count : "0");
	}

	/**
	 * Provide a means to check for a field's population including those that are protected and
	 * have encrypted corresponding shadow field values.  Non-null shadow fields imply that the
	 * instance has a value for this field -- the instance would have originally been serialized
	 * with crypto, so the value in the backing field is encrypted into the corresponding shadow
	 * field while the backing field gets set to null.
	 *
	 * If there is no shadow for the field in question, reflection is used to determine whether
	 * the regular backing field has a value.
	 *
	 * NB: this method does not address 'empty' semantics.
	 * For fields with types supporting empty semantics, the encrypted empty representation may
	 * result in non-null, and non-empty ciphers, so there is no guaranteed means of establishing
	 * 'empty' state in both the backing field and the encrypted/shadow field.
	 *
	 * The final result takes into account the shadow (if provided) and the backing field so that
	 * edits made to the instance after deserialization also get taken into account.
	 *
	 * @param fieldName
	 * @return
	 * @throws IllegalArgumentException
	 */
	public boolean isFieldNull(String fieldName) throws IllegalArgumentException {

		boolean isBackingFieldNull;
		boolean hasShadow = false;
		boolean isShadowFieldNull = false;

		if (hasShadowFields()) {
			for (ShadowField f : shadow.getFields())	{
				if (f.getName().equalsIgnoreCase(fieldName)) {
					hasShadow = true;
					// NB: a test for empty would not guarantee empty backing field
					isShadowFieldNull = null == f.getBase64Encoded();
					break;
				}
			}
		}

		Object value;
		try {
			value = FieldUtils.readField(this,fieldName,true);
		} catch (IllegalAccessException iae) {
			value = null; // we cannot determine value, so interpret as null
		}

		isBackingFieldNull = value == null;

		return hasShadow ? isBackingFieldNull && isShadowFieldNull : isBackingFieldNull;
	}


	@Override
	public String toString() {
		return "ts=" + ts + ",headers=[" + headers + "]";
	}


	@Override
	public Object clone() {

		AbstractREM clone = (AbstractREM) super.clone();

		if (hasShadowFields()) {
			Shadow shadowClone = new Shadow();
			ArrayList<ShadowField> fclones = new ArrayList<>();
			for (ShadowField f : shadow.getFields()) 
				fclones.add((ShadowField) f.clone());

			shadowClone.setFields(fclones);
			clone.setShadow(shadowClone);
		}

		return clone;

	}

	private boolean hasShadowFields() {
		return shadow != null && shadow.getFields() != null;
	}

}
