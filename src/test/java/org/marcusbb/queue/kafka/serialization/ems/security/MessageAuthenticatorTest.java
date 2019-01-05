package org.marcusbb.queue.kafka.serialization.ems.security;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.marcusbb.queue.Envelope;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.utils.MockHashFunction;
import org.marcusbb.queue.serialization.impl.ems.security.MessageAuthenticator;
import org.marcusbb.queue.serialization.impl.ems.security.MessageAuthenticatorException;

import java.security.NoSuchAlgorithmException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Created by kirill on 01/06/17.
 */
public class MessageAuthenticatorTest {
    private static final byte[] message = "Hello, world".getBytes();

    private MessageAuthenticator messageAuthenticator = new MessageAuthenticator(new MockHashFunction());


    @Test
    public void basicSigning() throws NoSuchAlgorithmException {
        byte[] signature = messageAuthenticator.sign(message);
        assertArrayEquals("Encoded hash should be correct", new MockHashFunction().computeHash(message), signature);
    }

    @Test
    public void signAndVerify() throws NoSuchAlgorithmException {
        byte[] signature = messageAuthenticator.sign(message);
        messageAuthenticator.verify(message, signature);
    }

    @Test(expected = MessageAuthenticatorException.class)
    public void failIfSignatureIsDifferent() {
        messageAuthenticator.verify(message, new byte[] {123});

        Assert.fail("Message authentication should fail when signature does not match the message");
    }

    @Test(expected = MessageAuthenticatorException.class)
    public void failIfSignatureIsEmpty() {
        messageAuthenticator.verify(message, new byte[0]);
        Assert.fail("Message authentication should fail when signature is empty");
    }

    @Test(expected = MessageAuthenticatorException.class)
    public void failIfSignatureIsNull() {
        messageAuthenticator.verify(message, null);
        Assert.fail("Message authentication should fail when signature is null");
    }

    @Test
    public void acceptEmptyMessage() {
        byte[] signature = messageAuthenticator.sign(new byte[0]);
        messageAuthenticator.verify(new byte[0], signature);
    }

    @Test(expected = MessageAuthenticatorException.class)
    public void failSignIfMessageIsNull(){
        messageAuthenticator.sign(null);
        Assert.fail();
    }

    @Test(expected = MessageAuthenticatorException.class)
    public void failVerifyIfMessageIsNull() {
        messageAuthenticator.verify(null, new byte[0]);
        Assert.fail();
    }
}
