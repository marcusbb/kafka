package org.marcusbb.queue.serialization.impl;

import java.io.*;

import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;

public class JavaMessageSerializer<T extends Serializable> implements ByteSerializer<T> {
	

	public byte[] serialize(T obj) {
		try {
			 ByteArrayOutputStream bo = new ByteArrayOutputStream();
			 ObjectOutputStream oo = new ObjectOutputStream(bo);
			 oo.writeObject(obj);
			 return bo.toByteArray();
		}catch (Exception e) {
			throw new SerializationException(e);
		}
		
	}

	public T deserialize(byte []b) {
		return serializeToObject(b);
	}
	
	public T serializeToObject(byte []b) {
		try {
			ObjectInputStream ins = new ObjectInputStream(new ByteArrayInputStream(b));
			return (T) ins.readObject();
		}catch (Exception e) {
			throw new SerializationException(e);
		}
	}
	
	
}