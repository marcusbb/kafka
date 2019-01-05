package org.marcusbb.queue.serialization.impl.ems.security.hash;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class JavaMessageDigestHashFunction implements HashFunction {
    private final String algorithm;

    public JavaMessageDigestHashFunction(String algorithm) throws NoSuchAlgorithmException {
        this.algorithm = algorithm;
    }

    @Override
    public byte[] computeHash(byte[] plaintext) {
        try {
            /**
             * Note that the MessageDigest instance returned by {@link MessageDigest#getInstance(String)}
             * is not thread-safe, so we cannot simply call it once in the constructor.
             *
             * Another approach could be to protect this method with a lock (e.g. {@code synchronized(this) ... }),
             * and only then we could construct the MessageDigest once in the constructor.
             *
             * Relative benefits of locking-based approaches (over getting a new MessageDigest every time) are not immediately evident
             * (<a href="https://stackoverflow.com/questions/1082050/linking-to-an-external-url-in-javadoc">StackOverflow</a>).
             *
             * Further performance testing and monitoring performance in production may reveal a need for further optimizations.
             */
            return MessageDigest.getInstance(algorithm).digest(plaintext);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
