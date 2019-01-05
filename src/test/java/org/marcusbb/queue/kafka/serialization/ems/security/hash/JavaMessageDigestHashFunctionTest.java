package org.marcusbb.queue.kafka.serialization.ems.security.hash;

import org.junit.Test;
import org.marcusbb.queue.serialization.impl.ems.security.hash.HashFunction;
import org.marcusbb.queue.serialization.impl.ems.security.hash.JavaMessageDigestHashFunction;

import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by kirill on 30/05/17.
 */
public class JavaMessageDigestHashFunctionTest {
    private static final byte[] messageSHA1 = new byte[] { (byte)0xe0, (byte)0x2a, (byte)0xa1, (byte)0xb1, (byte)0x06, (byte)0xd5, (byte)0xc7, (byte)0xc6, (byte)0xa9, (byte)0x8d, (byte)0xef, (byte)0x2b, (byte)0x13, (byte)0x00, (byte)0x5d, (byte)0x5b, (byte)0x84, (byte)0xfd, (byte)0x8d, (byte)0xc8 };
    private static final byte[] message = "Hello, world".getBytes();

    private static final byte[] emptySHA1 = new byte[] { (byte)0xda,(byte)0x39,(byte)0xa3,(byte)0xee,(byte)0x5e,(byte)0x6b,(byte)0x4b,(byte)0x0d,(byte)0x32,(byte)0x55,(byte)0xbf,(byte)0xef,(byte)0x95,(byte)0x60,(byte)0x18,(byte)0x90,(byte)0xaf,(byte)0xd8,(byte)0x07,(byte)0x09 };

    @Test
    public void computeSHA1() throws NoSuchAlgorithmException {
        HashFunction hashFunction = new JavaMessageDigestHashFunction("SHA-1");
        assertArrayEquals("SHA1 hash should be correct", messageSHA1, hashFunction.computeHash(message));
        assertArrayEquals("SHA1 of empty string should be correct", emptySHA1, hashFunction.computeHash(new byte[0]));
    }
}
