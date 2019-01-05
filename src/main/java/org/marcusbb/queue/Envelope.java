package org.marcusbb.queue;

import java.io.Serializable;
import java.util.Map;

public interface Envelope<T> extends Serializable {

    long getCreateTs();

    Map<String, String> getHeaders();

    void addHeader(String key, String value);

    String getHeader(String key);

    T getPayload();

}