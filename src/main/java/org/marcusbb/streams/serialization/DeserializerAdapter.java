package org.marcusbb.streams.serialization;

import org.apache.kafka.common.serialization.Deserializer;
import org.marcusbb.queue.serialization.ByteSerializer;

import java.util.Map;

/**
 * Created by kirill on 18/01/18.
 */
public class DeserializerAdapter<T> implements Deserializer<T> {
    private ByteSerializer<T> innerSerializer;


    public DeserializerAdapter(ByteSerializer<T> innerSerializer) {
        this.innerSerializer = innerSerializer;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        return innerSerializer.deserialize(data);
    }

    @Override
    public void close() {

    }
}
