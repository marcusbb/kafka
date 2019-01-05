package org.marcusbb.queue.kafka.utils;


import org.apache.kafka.common.utils.Time;

class SystemTime implements Time {
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

	@Override
	public long hiResClockMs() {
		// TODO Auto-generated method stub
		return 0;
	}
}
