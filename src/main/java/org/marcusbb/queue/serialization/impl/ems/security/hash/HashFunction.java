package org.marcusbb.queue.serialization.impl.ems.security.hash;

/**
 * Created by kirill on 29/05/17.
 */
public interface HashFunction {

    byte[] computeHash(byte[] input);
}
