package org.marcusbb.queue.kafka.utils;

import org.marcusbb.queue.serialization.impl.ems.security.hash.HashFunction;

/**
 * Created by kirill on 30/05/17.
 */
public class MockHashFunction implements HashFunction{
    @Override
    public byte[] computeHash(byte[] plaintext) {
        return String.format("%d", plaintext.length).getBytes();
    }
}
