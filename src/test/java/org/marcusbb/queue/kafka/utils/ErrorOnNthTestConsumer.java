package org.marcusbb.queue.kafka.utils;

/**
 * Created by jim.lin on 7/5/2017.
 */
public class ErrorOnNthTestConsumer<T> extends TestConsumer<T> {
    private int nth;
    private int counter;
    public ErrorOnNthTestConsumer(boolean throwEx, int nth) {
        super(throwEx);
        this.nth = nth;
        counter = 1;
    }

    @Override
    public void onMessage(T msg, byte[] key) {
        try {
            super.onMessage(msg, key);
        }
        finally {
            //change to false to ensure the Nth call will simulate a successful receipt
            if (++counter >= nth)
                super.throwEx = false;
        }
    }
}
