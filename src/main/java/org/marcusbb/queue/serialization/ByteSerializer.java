package org.marcusbb.queue.serialization;



public interface ByteSerializer<T> {

	public byte [] serialize(T obj) throws SerializationException ;
	
	public T deserialize(byte []b) throws SerializationException;

}
