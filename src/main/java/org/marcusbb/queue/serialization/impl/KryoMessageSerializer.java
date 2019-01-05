package org.marcusbb.queue.serialization.impl;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.marcusbb.queue.serialization.ByteSerializer;
import org.marcusbb.queue.serialization.SerializationException;

/**
 *
 * A thread safe generic (java) serialization mechanism.
 * For forward and backward compatibility {@link CompatibleFieldSerializer} is chosen
 * for safety sake
 *
 * @param <T>
 */
public class KryoMessageSerializer<T> implements ByteSerializer<T> {

	private int initCap = 1024;
	private int maxCap = 1024*1024;

	private static List<Class<?>> registeredClasses = new ArrayList<>();

	private static ThreadLocal<Kryo> kryo =  new ThreadLocal<Kryo>() {

		@Override
		protected Kryo initialValue() {
			Kryo kryo = new Kryo();
			kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
			for (Class<?> cl:registeredClasses)
				kryo.register(cl);
			return kryo;
		}};

	public KryoMessageSerializer() {

	}

	public KryoMessageSerializer(int initCap,int maxCap) {
		this.initCap = initCap;
		this.maxCap = maxCap;
	}

	public void register(Class c) {
		kryo.get().register(c);
	}

	public static void registerClasses(Class<?>... clazz) {
		registeredClasses.addAll(Arrays.asList(clazz));
	}

	@Override
	public byte[] serialize(Object obj) {
		try {
			Output output = new Output(initCap,maxCap);
			kryo.get().writeClassAndObject(output, obj);
			return output.getBuffer();
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}

	@Override
	public T deserialize(byte[] b) {
		Input input = new Input(b);
		try {
			return (T)kryo.get().readClassAndObject(input);
		} catch (Exception e) {
			throw new SerializationException(e);
		}
	}

}
