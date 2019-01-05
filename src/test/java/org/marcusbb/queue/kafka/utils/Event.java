package org.marcusbb.queue.kafka.utils;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.AvroName;
import org.apache.avro.reflect.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Event {

    @AvroName(value="msgVersion")
    @AvroDefault(value="1.0.0")
    int version;

    int cost;

    @Nullable
    TimeStamp stamp;

    @Nullable
    BigDecimal amount;

    @AvroName(value="public")
    Map<String,String> headers = new HashMap<String,String>();

    @AvroName(value="currentDate")
    Date date ;

    ByteBuffer b = ByteBuffer.allocate(0);

    public Event() {}                        // required to read

    public Event(int cost, TimeStamp stamp){
        this.cost = cost;
        this.stamp = stamp;
    }

    public Event(int cost, TimeStamp stamp,BigDecimal amount){
        this.cost = cost;
        this.stamp = stamp;
        this.amount = amount;
    }
    @Override
    public String toString() {
        return "Event [version=" + version + ", cost=" + cost + ", stamp=" + stamp + ", headers=" + headers + ", date="
                + date + ", b=" + b + "]";
    }
    @Override
    public boolean equals(Object obj) {
        Event src = (Event)obj;
        return src.cost == cost && src.stamp.hour==stamp.hour && src.stamp.second == stamp.second && src.amount.equals(amount);
    }

    public static class TimeStamp {
        int hour = 0;
        int second = 0;
        public TimeStamp() {}                     // required to read
        public TimeStamp(int hour, int second){
            this.hour = hour;
            this.second = second;
        }
        @Override
        public String toString() {
            return "TimeStamp [hour=" + hour + ", second=" + second + "]";
        }

    }
}