package org.marcusbb.streams.serialization;

import org.apache.kafka.common.serialization.Serializer;
import org.marcusbb.queue.serialization.ByteSerializer;

import java.util.Map;

/**
 * Created by kirill on 18/01/18.
 */
public class SerializerAdapter<T> implements Serializer<T> {
    private ByteSerializer<T> innerSerializer;

    public SerializerAdapter(ByteSerializer<T> innerSerializer) {
        this.innerSerializer = innerSerializer;
    }

    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, T data) {
        return innerSerializer.serialize(data);
    }

    @Override
    public void close() {

    }
}
