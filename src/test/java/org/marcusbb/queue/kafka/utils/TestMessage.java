package org.marcusbb.queue.kafka.utils;

import java.io.Serializable;

public class TestMessage implements Serializable {

    String content;

    public TestMessage() {}

    public TestMessage(String content){
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    @Override
    public String toString() {
        return "TestMessage [content=" + content + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TestMessage that = (TestMessage) o;

        return content.equals(that.content);
    }

}
