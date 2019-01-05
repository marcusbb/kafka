package org.marcusbb.queue.kafka.utils;

import java.io.Serializable;

/**
 * TestMessage with an renamed field: moreContent -> altContent
 * Used for testing schema evolution.
 */
public class TestMessageV3 implements Serializable {

	String content;
	String altContent;

	public TestMessageV3() {}

	public TestMessageV3(String content){
		this.content = content;
		this.altContent = null;
	}

	public TestMessageV3(String content, String altContent) {
		this.content = content;
		this.altContent = altContent;
	}

	public String getContent() {
		return content;
	}

	public String getAltContent() {
		return altContent;
	}

	@Override
	public String toString() {
		return "TestMessageV3 [content=" + content + ", altContent=" + altContent + "]";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		TestMessageV3 that = (TestMessageV3) o;

		return content.equals(that.content) && altContent.equals(that.altContent);
	}
}
