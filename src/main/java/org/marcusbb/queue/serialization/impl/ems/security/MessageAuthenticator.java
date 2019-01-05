package org.marcusbb.queue.serialization.impl.ems.security;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.marcusbb.queue.serialization.impl.ems.security.hash.HashFunction;
import org.marcusbb.queue.serialization.impl.ems.security.hash.JavaMessageDigestHashFunction;

public class MessageAuthenticator {

    private final HashFunction hashFunction;


    private static HashFunction getDefaultHashFunction() {
        try {
            return new JavaMessageDigestHashFunction("SHA-256");
        }
        catch(NoSuchAlgorithmException ex) {
            throw new MessageAuthenticatorException("unable to instantiate hash function", ex);
        }
    }

    public MessageAuthenticator()  {
        this(getDefaultHashFunction());
    }

    public MessageAuthenticator(HashFunction hashFunction){
        this.hashFunction = hashFunction;
    }

    public final byte[] sign(byte[] messageBytes) throws MessageAuthenticatorException {
        try {
            return hashFunction.computeHash(messageBytes);
        }
        catch (Exception ex) {
            throw new MessageAuthenticatorException("unable to sign message", ex);
        }
    }

    public final void verify(byte[] messageBytes, byte[] messageSignature) {
        try {
            byte[] actualHash = hashFunction.computeHash(messageBytes);

            if (!Arrays.equals(messageSignature, actualHash)) {
                throw new MessageAuthenticatorException("message signature does not match");
            }
        }
        catch (Exception ex) {
            throw new MessageAuthenticatorException("unable to verify message signature", ex);
        }
    }

}
