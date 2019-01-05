package org.marcusbb.streams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.marcusbb.queue.serialization.ByteSerializer;

import java.util.Map;

/**
 * Created by kirill on 18/01/18.
 */
public class SerdeAdapter<T> implements Serde<T> {
    private final DeserializerAdapter<T> deserializer;
    private final SerializerAdapter<T> serializer;

    public SerdeAdapter(ByteSerializer<T> innerSerializer) {
        this.serializer = new SerializerAdapter<>(innerSerializer);
        this.deserializer = new DeserializerAdapter<>(innerSerializer);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
