package org.marcusbb.queue;

import org.apache.avro.reflect.AvroSchema;
import org.marcusbb.queue.kafka.consumer.AbstractConsumer;
import org.marcusbb.queue.serialization.impl.ems.AvroEMS;
import org.marcusbb.queue.serialization.impl.ems.EncryptedMessageSerializer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * To be used in tandem with #{@link EncryptedMessageSerializer}
 *
 * And primary intent is an envelope.
 *
 */
@AvroSchema(value = "{\"type\":\"record\",\"name\":\"RoutableEncryptedMessage\",\"namespace\":\"org.marcusbb.queue\",\"fields\":[{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"encPayload\",\"type\":{\"type\":\"bytes\",\"java-class\":\"[B\"}},{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"signature\", \"type\": { \"type\":\"bytes\", \"java-class\":\"[B\" }}]}")
public final class RoutableEncryptedMessage<T> implements Envelope<T> {

	private static final long serialVersionUID = 1L;
	
	// encrypted bytes of payload obtained after serializing object reference of payload (T)
	byte []encPayload;

	Map<String,String> headers;

	long ts = System.currentTimeMillis();

	byte[] signature = new byte[0];

	transient T payload;
	
	//this is simply for serializers
	private RoutableEncryptedMessage() {}

	public RoutableEncryptedMessage(T payload) {
		this(new HashMap<String,String>(),payload);
	}

	public RoutableEncryptedMessage(Map<String,String> headers, T payload) {
		this.headers = headers;
		this.payload = payload;
	}
	
	public RoutableEncryptedMessage(Map<String,String> headers, byte[] encPayload) {
		this.encPayload = encPayload;
		this.headers = headers;
	}

	public RoutableEncryptedMessage(Map<String,String> headers, byte[] encPayload, byte[] signature) {
		this(headers, encPayload);
		this.signature = signature;
	}

	public RoutableEncryptedMessage(RoutableEncryptedMessage<T> wrapper, T payload) {
		this(wrapper.headers,payload);
		this.encPayload = wrapper.encPayload;
		this.signature = wrapper.signature;
		this.ts = wrapper.ts;
	}

	public RoutableEncryptedMessage(RoutableEncryptedMessage<T> wrapper, byte[] encPayload) {
		this(wrapper.headers,encPayload, wrapper.signature);
		this.ts = wrapper.ts;
	}

	public RoutableEncryptedMessage(RoutableEncryptedMessage<T> wrapper, byte[] encPayload, byte[] signature) {
		this(wrapper.headers,encPayload);
		this.ts = wrapper.ts;
		this.signature = signature;
	}

	@Override
	public Map<String, String> getHeaders() {
		return Collections.unmodifiableMap(headers);
	}

	@Override
	public void addHeader(String key, String value) {
		if (headers == null){
			headers = new HashMap<>();
		}
		if (value != null) {
			headers.put(key, value);
		}
	}

	@Override
	public String getHeader(String key){
		String val=null;
		if (headers != null){
			val = headers.get(key);
		}
		return val;
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

	@Override
	public T getPayload() {
		return payload;
	}

	public byte[] getEncPayload() {
		return encPayload;
	}

	@Override
	public long getCreateTs() {
		return ts;
	}

	public byte[] getSignature() {
		return signature;
	}

	@Override
	public String toString() {
		return "RoutableEncryptedMessage [headers=" + headers + ",type="+(getPayload() != null ? getPayload().getClass():null)+"]";
	}

}
