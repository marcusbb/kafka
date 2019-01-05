package org.marcusbb.queue.kafka.utils;

import org.marcusbb.queue.Envelope;
import org.marcusbb.queue.RoutableEncryptedMessage;
import org.marcusbb.queue.kafka.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class TestConsumer<T> implements Consumer<T> {
    protected Logger logger = LoggerFactory.getLogger(TestConsumer.class);
    public boolean throwEx=false;
    CountDownLatch latch;
    public T lastMessage = null;
    public List<T> messagesDelivered = new ArrayList<>();

    public TestConsumer() {}
    public TestConsumer(CountDownLatch latch) {
        this.latch = latch;
    }
    public TestConsumer(boolean throwEx) {
        this.throwEx = throwEx;
    }

    @Override
    public void onMessage(T msg, byte[] key) {
        if (msg instanceof Envelope){
            Envelope envelope = (Envelope) msg;
            logger.info("message received - envelope={},payload={}",envelope,envelope.getPayload());
        }else{
            logger.info("message received - {}",msg);
        }
        lastMessage = msg;
        messagesDelivered.add(msg);
        if (latch != null) {
            latch.countDown();
        }
        if (throwEx){
            throw new RuntimeException("consumer failure exception");
        }
    }
}
